package svrdir

import (
	"net"
	"net/rpc"
	"hash/crc32"
	"logsync/log"
	"logsync/svrfile"
)

var (
	_ = log.Println
)

type SvrDir struct {
	root []string
	HashRoot string
	file *svrfile.SvrFile
	svr *rpc.Server
}

func ServeConn(root []string, file *svrfile.SvrFile, conn net.Conn) {
	s := new(SvrDir)
	s.root = root
	s.file = file
	svr := rpc.NewServer()
	svr.RegisterName("Dir", s)
	s.svr = svr
	s.svr.ServeConn(conn)
}

func (s *SvrDir) getHashRoot(host string) string {
	return s.root[int(crc32.ChecksumIEEE([]byte(host))) % len(s.root)]
}

func (s *SvrDir) makePath(fname string) string {
	return s.HashRoot + "/" + fname
}

// Conf ------------------------------------------------------------------------

type ConfArg struct {
	Host string
	LogType string
	ServiceName string
	ServiceType string
}
type ConfReply struct {
	Err error
}
func (s *SvrDir) Conf(arg *ConfArg, reply *ConfReply) (err error) {
	root := s.getHashRoot(arg.Host + "/" + arg.ServiceName + "/" + arg.LogType)
	path := arg.Host + "/" + arg.ServiceType + "/" + arg.ServiceName + "/" + arg.LogType
	s.HashRoot = root + "/" + path
	return
}

// DeleteFile ------------------------------------------------------------------

type DeleteFileArg struct {
	Fname []string
}
type DeleteFileReply struct {
	Err error
}
func (s *SvrDir) DeleteFile(arg *DeleteFileArg, reply *DeleteFileReply) (err error) {
	log.Todo("delete file", arg)
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
