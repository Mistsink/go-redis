package aof

import (
	"go-redis/config"
	dbface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/conn"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"os"
	"strconv"
	"sync"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// AOFHandler receive msgs from channel and write to AOF file
type AOFHandler struct {
	db          dbface.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	// aof goroutine will send msg to main goroutine through this
	// channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.RWMutex
	currentDB  int
}

func NewAOFHandler(database dbface.Database) (*AOFHandler, error) {
	aofHandler := &AOFHandler{}

	aofHandler.aofFilename = config.Properties.AppendFilename
	aofHandler.db = database
	aofFile, err := os.OpenFile(aofHandler.aofFilename,
		os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	aofHandler.aofFile = aofFile
	//	load msgs from aofFile
	aofHandler.loadAOF()

	//	prepare the channel
	aofHandler.aofChan = make(chan *payload, aofQueueSize)
	go aofHandler.handleAOF()

	return aofHandler, nil
}

func (handler *AOFHandler) AddAOF(dbIdx int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIdx,
		}
	}
}

// handleAOF 落盘
func (handler *AOFHandler) handleAOF() {
	handler.currentDB = 0 //	保险的初始化

	for payload := range handler.aofChan {
		if payload.dbIndex != handler.currentDB {
			_, err := handler.aofFile.Write(reply.NewMultiBulkReply(
				utils.ToCmdLine("select", strconv.Itoa(payload.dbIndex)),
			).ToBytes())
			if err != nil {
				logger.Error(err)
				continue
			}
		}
		_, err := handler.aofFile.Write(
			reply.NewMultiBulkReply(payload.cmdLine).ToBytes())
		if err != nil {
			logger.Error(err)
			continue
		}
	}
}

func (handler *AOFHandler) loadAOF() {
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()

	ch := parser.ParseStream(file)
	fakeConn := conn.NewConn(nil)
	for payload := range ch {
		//	error
		if payload.Err != nil {
			// error must close conn
			if payload.Err == io.EOF {
				break
			}
			// protocol error
			logger.Error(err)
			continue
		}

		//	exec
		if payload.Data == nil {
			continue
		}

		multiBulkReply, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Info("require multi bulk reply")
			continue
		}

		execReply := handler.db.Exec(fakeConn, multiBulkReply.Args)
		if reply.IsErrReply(execReply) {
			logger.Error(execReply.ToBytes())
		}
	}
}
