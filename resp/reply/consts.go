package reply

type PongReply struct{}

var pongBytes = []byte("+PONG\r\n")

func (r PongReply) ToBytes() []byte {
	return pongBytes
}

func NewPongReply() *PongReply {
	return &PongReply{}
}

type OkReply struct{}

var okBytes = []byte("+OK\r\n")

func (r OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply)

func NewOkReply() *OkReply {
	return theOkReply
}

// NullBulkReply null bulk reply
type NullBulkReply struct{}

var nullBulkBytes = []byte("$-1\r\n")

func (r NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func NewNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// EmptyMultiBulkReply empty array(multi bulk) reply
type EmptyMultiBulkReply struct{}

var emptyMultiBulkBytes = []byte("*0\r\n")

func (r EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func NewEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

type NoReply struct{}

var noBytes = []byte("")

func (r NoReply) ToBytes() []byte {
	return noBytes
}

func NewNoReply() *NoReply {
	return &NoReply{}
}
