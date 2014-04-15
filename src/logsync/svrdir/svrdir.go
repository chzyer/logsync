package svrdir

import (
	"net"
	"net/rpc"
	"strings"
	"logsync/log"
	"logsync/svrfile"
)

var (
	_ = log.Println
)

type SvrDir struct {
	Root string
	Host string
	Path string
	file *svrfile.SvrFile
	svr *rpc.Server
}

func ServeConn(root string, file *svrfile.SvrFile, conn net.Conn) {
	s := new(SvrDir)
	s.file = file
	svr := rpc.NewServer()
	svr.RegisterName("Dir", s)
	s.Root = root
	s.svr = svr
	s.svr.ServeConn(conn)
}

func (s *SvrDir) makePath(fname string) string {
	return s.Root + "/" + s.Host + "/" + s.Path + "/" + fname
}

// Conf ------------------------------------------------------------------------

type ConfArg struct {
	Host, Path string
}
type ConfReply struct {
	Err error
}
func (s *SvrDir) Conf(arg *ConfArg, reply *ConfReply) (err error) {
	s.Host = arg.Host
	s.Path = strings.Trim(arg.Path, "/")
	return
}

// WriteAt ---------------------------------------------------------------------

type WriteAtArg struct {
	Fname string
	Data []byte
	Offset int64
}
type WriteAtReply struct{
	N int
	Err error
}
func (s *SvrDir) WriteAt(arg *WriteAtArg, reply *WriteAtReply) (err error) {
	reply.N, reply.Err = s.file.WriteFileAt(s.makePath(arg.Fname), arg.Data, arg.Offset)
	return
}

// FileInfo --------------------------------------------------------------------

type FileOffsetArg struct {
	Fname []string
}
type FileOffsetReply struct {
	Infos map[string] *svrfile.FileInfo
}
func (s *SvrDir) FileInfo(arg *FileOffsetArg, reply *FileOffsetReply) (err error) {
	infos := make(map[string] *svrfile.FileInfo, len(arg.Fname))
	for _, f := range arg.Fname {
		fpath := s.makePath(f)
		info, err := s.file.GetFileInfo(fpath)
		if err != nil {
			log.Error(fpath, err)
			continue
		}
		infos[f] = info
	}
	reply.Infos = infos
	return
}
