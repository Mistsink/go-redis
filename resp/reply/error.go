package reply

type UnknownErrReply struct{}

var unknownBytes = []byte("-Err unknown\r\n")

func (r *UnknownErrReply) Error() string {
	return "Err unknown"
}

func (r *UnknownErrReply) ToBytes() []byte {
	return unknownBytes
}

func NewUnknownErrReply() *UnknownErrReply {
	return &UnknownErrReply{}
}

type ArgNumErrReply struct {
	Cmd string
}

func (r *ArgNumErrReply) Error() string {
	return "-Err wrong number of arguments for '" + r.Cmd + "' command"
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-Err wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func NewArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-Err syntax error\r\n")
var theSyntaxErrReply = &SyntaxErrReply{}

func (r *SyntaxErrReply) Error() string {
	return "Err syntax error"
}

func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func NewSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}

type WrongTypeErrReply struct {
}

var wrongTypeErrBytes = []byte("-WrongType Operation against a key holding the wrong kind of value\r\n")

func (r *WrongTypeErrReply) Error() string {
	return "WrongType Operation against a key holding the wrong kind of value"
}

func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func NewWrongTypeErrReply() *WrongTypeErrReply {
	return &WrongTypeErrReply{}
}

type ProtocolErrReply struct {
	Msg string
}

func (r *ProtocolErrReply) Error() string {
	return "Err Protocol error: " + r.Msg
}

func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-Err Protocol error: '" + r.Msg + "'\r\n")
}
