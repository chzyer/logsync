package svrfile

import (
	"os"
	"os/user"
	"sync"
	"path/filepath"
)

type SvrFile struct {
	rwl sync.RWMutex
	data map[string] *os.File
	OwnerUid,OwnerGid string
}

func NewSvrFile(owner string) (s *SvrFile, err error) {
	u, err := user.Lookup(owner)
	if err != nil {
		return
	}
	s = new(SvrFile)
	s.OwnerUid = u.Uid
	s.OwnerGid = u.Gid
	return
}

type FileInfo struct {
	Offset int64
	Deleted bool
}

func (s *SvrFile) getFile(path string) (f *os.File, err error) {
	s.rwl.RLock()
	f = s.data[path]
	s.rwl.RUnlock()
	if f != nil {
		return
	}

	// make new
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0777)
	}
	nf, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		// disk full
		return
	}
	s.rwl.Lock()
	f = s.data[path]
	if f == nil {
		s.data[path] = nf
		nf = nil
	}
	s.rwl.Unlock()

	// close file if not used
	if nf != nil {
		nf.Close()
	}

	return
}

func (s *SvrFile) WriteFileAt(path string, buf []byte, at int64) (n int, err error) {
	f, err := s.getFile(path)
	if err != nil {
		return
	}

	// 假设单个文件只有一个写入，没加锁
	n, err = f.WriteAt(buf, at)
	return
}

func (s *SvrFile) GetFileInfo(path string) (info *FileInfo, err error) {
	stat, err := os.Stat(path)
	if err != nil {
		if ! os.IsNotExist(err) {
			return
		}

		info = new(FileInfo)
		err = nil

		_, errDel := os.Stat(path + ".delete")
		if errDel != nil {
			return
		}

		// 存在.delete文件
		info.Deleted = true
		return
	}

	info = new(FileInfo)
	info.Offset = stat.Size()
	return
}
