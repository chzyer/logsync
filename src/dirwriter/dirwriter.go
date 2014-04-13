package dirwriter

import (
	"log"
	"sync"
	"os"
	"net"
	"path/filepath"
	"net/rpc"
)

type Service struct {
	conn net.Listener
	svr *rpc.Server
}

func New(listen, root string) (d *Service, err error) {
	d = new(Service)
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return
	}
	d.svr = rpc.NewServer()
	dir := new(Dir)
	dir.root = root
	d.svr.Register(dir)
	d.conn = ln
	return
}

func (d *Service) Run() {
	d.svr.Accept(d.conn)
}

type Dir struct {
	root string
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
var data = map[string]*os.File{}
var l sync.Mutex
func (d *Dir) WriteFile(arg *WriteFileArg, reply *WriteFileRet) (err error) {
	path := d.root + arg.Path
	f, ok := data[path]
	if ! ok {
		if _, err := os.Stat(filepath.Dir(path)); err != nil && os.IsNotExist(err) {
			err := os.MkdirAll(filepath.Dir(path), 0777)
			if err != nil {
				log.Println(err)
				return err
			}
		}
		
		f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			log.Println(err)
			reply.Err = err
			err = nil
			return
		}
	}
	reply.N, reply.Err = f.WriteAt(arg.Buf, arg.Offset)
	return
}

type FileOffsetArg struct {
	Path []string
}
type FileOffsetRet struct {
	Offset []int64
	Err error
}
func getFileOffset(path string) (offset int64, err error) {
	stat, err := os.Stat(path)
	if err != nil {
		return
	}
	offset = stat.Size()
	return
}
func (d *Dir) FileOffset(arg *FileOffsetArg, reply *FileOffsetRet) (err error) {
	// 如果不存在，可能是临时文件被删除或者根本不存在
	log.Println(arg)
	reply.Offset = make([]int64, len(arg.Path))
	for idx, p := range arg.Path {
		arg.Path[idx] = d.root + p
		reply.Offset[idx], _ = getFileOffset(arg.Path[idx])
	}
	return
}
