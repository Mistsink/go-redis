package reply

import (
	"bytes"
	"go-redis/interface/resp"
	"strconv"
)

var (
	nullBulkReplyBytes = []byte("$-1")
	CRLF               = "\r\n"
)

type BulkReply struct {
	Arg []byte
}

func (r *BulkReply) ToBytes() []byte {
	_len := len(r.Arg)
	if _len == 0 {
		return []byte(string(nullBulkReplyBytes) + CRLF)
	}

	return []byte("$" + strconv.Itoa(_len) + CRLF +
		string(r.Arg) + CRLF)
}

func NewBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

type MultiBulkReply struct {
	Args [][]byte
}

func (r *MultiBulkReply) ToBytes() []byte {
	argsLen := len(r.Args)
	buf := bytes.Buffer{}

	buf.WriteString("*" + strconv.Itoa(argsLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString(string(nullBulkReplyBytes) + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF +
				string(arg) + CRLF)
		}
	}

	return buf.Bytes()
}

func NewMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args: args}
}

type StatusReply struct {
	Status string
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

func NewStatusReply(status string) *StatusReply {
	return &StatusReply{Status: status}
}

type IntReply struct {
	Code int64
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

func NewIntReply(code int64) *IntReply {
	return &IntReply{Code: code}
}

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

type StandardErrReply struct {
	Status string
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func NewStandardErrReply(status string) *StandardErrReply {
	return &StandardErrReply{Status: status}
}

func IsErrReply(reply resp.Reply) bool {
	return reply.ToBytes()[0] == '-'
}
