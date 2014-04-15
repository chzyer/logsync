package remotedir

import (
	"net/rpc"
	"logsync/log"
)

var _ = log.Println

type FileInfo struct {
	Offset int64
	Deleted bool
}

type Info struct {
	Host string
	Path string
}

type RemoteDir struct {
	info *Info
	addr string
	client *rpc.Client
	err error
}

func NewDir(addr string, info *Info) (r *RemoteDir, err error) {
	r = &RemoteDir{info:info}
	r.addr = addr
	r.connect()
	if r.err != nil {
		err = r.err
		return
	}
	return
}

func (r *RemoteDir) connect() {
	client, err := rpc.Dial("tcp", r.addr)
	if err != nil {
		r.err = err
		return
	}
	r.client = client
	r.conf()
}

func (r *RemoteDir) conf() (err error) {
	r.client.Call("Dir.Conf", r.info, err)
	return
}

type FileInfoArg struct { Fname []string }
type FileInfoReply struct {
	Infos map[string] *FileInfo
	Err error
}
func (r *RemoteDir) FileInfo(fname []string) (ret map[string] *FileInfo, err error) {
	reply := new(FileInfoReply)
	r.client.Call("Dir.FileInfo", &FileInfoArg{fname}, reply)
	ret, err = reply.Infos, reply.Err
	return
}

type WriteAtArg struct {
	Fname string
	Data []byte
	Offset int64
}
type WriteAtReply struct {
	N int
	Err error
}
func (r *RemoteDir) WriteAt(fname string, buf []byte, offset int64) (n int, err error) {
	reply := new(WriteAtReply)
	r.client.Call("Dir.WriteAt", &WriteAtArg{fname, buf, offset}, reply)
	n, err = reply.N, reply.Err
	return
}

func (r *RemoteDir) Close() (err error) {
	err = r.client.Close()
	if err != nil {
		return
	}
	return
}
