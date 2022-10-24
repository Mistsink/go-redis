package parser

import (
	"bufio"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data resp.Reply

	Err error
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && s.expectedArgsCount == len(s.args)
}

// ParseStream 异步解析字节流的设计方法：使用 channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)

	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()

	bufReader := bufio.NewReader(reader)
	state := readState{}
	var msg []byte
	var err error

	for {
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)

		if err != nil {
			if ioErr {
				ch <- &Payload{Err: err}
				close(ch)
				return
			} else {
				ch <- &Payload{Err: err}
				state = readState{}
				continue
			}

		}

		//	是否多行解析
		if !state.readingMultiLine {
			//	单行
			switch msg[0] {
			case '*':
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: makeProtocolErr(msg)}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{Data: reply.EmptyMultiBulkReply{}}
					state = readState{}
					continue
				}
			case '$':
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: makeProtocolErr(msg)}
					state = readState{}
					continue
				}
				if state.bulkLen == -1 {
					ch <- &Payload{Data: reply.NullBulkReply{}}
					state = readState{}
					continue
				}
			case '+' | '-' | ':':
				lineReply, err := parseSingleLineReply(msg)
				ch <- &Payload{Data: lineReply, Err: err}
				state = readState{}
				continue
			}
		} else {
			//	多行
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{Err: makeProtocolErr(msg)}
				state = readState{}
				continue
			}

			if state.finished() {
				var resultReply resp.Reply
				switch state.msgType {
				case '*':
					resultReply = reply.NewMultiBulkReply(state.args)
				case '$':
					resultReply = reply.NewBulkReply(state.args[0])
				}
				ch <- &Payload{Data: resultReply, Err: err}

				state = readState{}
			}
		}
	}
}

// readLine
//
//	return:	result, if io-err, error
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error

	if state.bulkLen == 0 { //	说明不是读字符数组
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' { //	非 \r\n 结尾
			return nil, false, makeProtocolErr(msg)
		}
	} else { //	应该读字符数组，可能 payload 中也会出现 \r\n
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, makeProtocolErr(msg)
		}

		state.bulkLen = 0 //	重置状态
	}
	return msg, false, nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return makeProtocolErr(msg)
	}

	if expectedLine == 0 {
		state.expectedArgsCount = 0
	} else if expectedLine > 0 {
		state.expectedArgsCount = int(expectedLine)
		state.readingMultiLine = true
		state.msgType = msg[0]
		state.args = make([][]byte, 0, expectedLine)
	} else { //	< 0
		return makeProtocolErr(msg)
	}
	return nil
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return makeProtocolErr(msg)
	}

	if state.bulkLen == -1 {
		state.expectedArgsCount = 0
		return nil
	} else if state.bulkLen > 0 {
		state.expectedArgsCount = 1
		state.readingMultiLine = true
		state.msgType = msg[0]
		state.args = make([][]byte, 0, 1)
	} else {
		return makeProtocolErr(msg)
	}
	return nil
}

// parseSingleLineReply 解析如：+ - : 这种单行消息
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")[1:]
	switch msg[0] {
	case '+': //	status
		return reply.NewStatusReply(str), nil
	case '-': //	err
		return reply.NewStandardErrReply(str), nil
	case ':': //		int
		intVal, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, makeProtocolErr(msg)
		}
		return reply.NewIntReply(intVal), nil
	default:
		return nil, makeProtocolErr(msg)
	}
}

// readBody 解析主体内容
func readBody(msg []byte, state *readState) error {
	line := string(msg[:len(msg)-2])
	var err error

	if line[0] == '$' { //	eg: $3\r\n...
		state.bulkLen, err = strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return makeProtocolErr(msg)
		}
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else { //	eg: PONG\r\n
		state.args = append(state.args, []byte(line))
	}
	return nil
}

func makeProtocolErr(msg []byte) error {
	return errors.New("protocol error: " + string(msg))
}
