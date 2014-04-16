package remotedir

import (
	"time"
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
	LogType string
	ServiceName string
	ServiceType string
}

type RemoteDir struct {
	info *Info
	addr string
	client *rpc.Client
}

func NewDir(addr string, info *Info) (r *RemoteDir, err error) {
	r = &RemoteDir{info:info}
	r.addr = addr
	if err=r.connect(); err!=nil {
		return
	}
	return
}

func (r *RemoteDir) connect() (err error) {
	client, err := rpc.Dial("tcp", r.addr)
	if err != nil {
		return
	}
	r.client = client
	err = r.conf()
	return
}

func (r *RemoteDir) call(api string, arg, reply interface{}) {
resend:
	netErr := r.client.Call(api, arg, reply)
	if netErr == nil {
		return
	}

reconnect:
	netErr = r.connect()
	if netErr != nil {
		time.Sleep(2*time.Second)
		log.Info("reconnect", netErr)
		goto reconnect
	}
	goto resend
}

// -----------------------------------------------------------------------------

type DeleteFileArg struct {
	Fname []string
}
type DeleteFileReply struct {
	Err error
}
func (r *RemoteDir) DeleteFile(fname []string) (err error) {
	reply := new(DeleteFileReply)
	r.call("Dir.DeleteFile", &DeleteFileArg{fname}, reply)
	err = reply.Err
	return
}

// Conf ------------------------------------------------------------------------

type ConfReply struct {
	Err error
}
func (r *RemoteDir) conf() (err error) {
	reply := new(ConfReply)
	r.call("Dir.Conf", r.info, reply)
	err = reply.Err
	return
}

// FileInfo --------------------------------------------------------------------

type FileInfoArg struct { Fname []string }
type FileInfoReply struct {
	Infos map[string] *FileInfo
	Err error
}
func (r *RemoteDir) FileInfo(fname []string) (ret map[string] *FileInfo, err error) {
	reply := new(FileInfoReply)
	r.call("Dir.FileInfo", &FileInfoArg{fname}, reply)
	ret, err = reply.Infos, reply.Err
	return
}

// WriteAt ---------------------------------------------------------------------

type WriteAtArg struct {
	Fname string
	Data []byte
	Offset int64
}
type WriteAtReply struct {
	N int
	Err error
}
// not network error
func (r *RemoteDir) WriteAt(fname string, buf []byte, offset int64) (n int, remoteErr error) {
	reply := new(WriteAtReply)
	r.call("Dir.WriteAt", &WriteAtArg{fname, buf, offset}, reply)
	n, remoteErr = reply.N, reply.Err
	return
}

// Close -----------------------------------------------------------------------

func (r *RemoteDir) Close() (err error) {
	err = r.client.Close()
	return
}
