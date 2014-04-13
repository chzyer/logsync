package remotedir

import (
	"log"
	"time"
	"net/rpc"
)

type LocalInfo struct {
	Host string
}

type RemoteDir struct {
	info *LocalInfo
	conn *rpc.Client
	addr string
}

func NewRemoteDir(addr string) (r *RemoteDir, err error) {
	r = new(RemoteDir)
	r.addr = addr
	err = r.init()
	return
}

func (r *RemoteDir) init() (err error) {
	conn, err := rpc.Dial("tcp", r.addr)
	if err != nil {
		return
	}
	r.conn = conn
	return

}

func (r *RemoteDir) call(api string, arg, ret interface{}) (err error) {
	err = r.conn.Call(api, arg, ret)
	for err != nil {
		log.Println(err)
		err = r.init()
		if err != nil {
			time.Sleep(time.Second)
		}
	}
	return
}

type WriteFileArg struct {
	Buf []byte
	Path string
	Offset int64
}
type WriteFileRet struct {
	N int
	Err error
}
func (r *RemoteDir) WriteFile(buf []byte, path string, offset int64) (n int, err error) {
	ret := new(WriteFileRet)
	err = r.call("Dir.WriteFile", &WriteFileArg{buf, path, offset}, ret)
	if err != nil {
		return
	}
	return ret.N, ret.Err
}

type FileOffsetArg struct {
	Path []string
}
type FileOffsetRet struct {
	Offset []int64
	Err error
}
func (r *RemoteDir) FileOffset(path []string) (offset []int64, err error) {
	ret := new(FileOffsetRet)
	err = r.call("Dir.FileOffset", &FileOffsetArg{path}, ret)
	if err != nil {
		return
	}
	return ret.Offset, ret.Err
}
