package svrfile

import (
	"os"
	"time"
	"sync"
	"os/user"
	"logsync/log"
	"path/filepath"
)

type SvrFile struct {
	rwl sync.RWMutex
	data map[string] *os.File
	updateTime map[string] time.Time
	UnuseTime time.Duration
	user *user.User
}

func NewSvrFile(owner string) (s *SvrFile, err error) {
	s = new(SvrFile)
	if owner != "" {
		var u *user.User
		u, err = user.Lookup(owner)
		if err != nil {
			return
		}
		s.user = u
	}
	s.data = make(map[string]*os.File, 10<<10)
	s.updateTime = make(map[string]time.Time, 10<<10)
	s.UnuseTime = time.Minute
	go s.CleanUnusedFile()
	return
}

type FileInfo struct {
	Offset int64
	Deleted bool
}

func (s *SvrFile) CleanUnusedFile() {
	for _ = range time.Tick(time.Minute) {
		now := time.Now()
		s.rwl.Lock()
		for k, v := range s.updateTime {
			if now.Sub(v) > s.UnuseTime {
				delete(s.updateTime, k)
				s.closeFile(k)
			}
		}
		s.rwl.Unlock()
	}
}

func (s *SvrFile) closeFile(path string) (err error) {
	f, ok := s.data[path]
	if ok {
		err = f.Close()
		delete(s.data, path)
	}
	return
}

func (s *SvrFile) getFile(path string) (f *os.File, err error) {
	s.rwl.RLock()
	s.updateTime[path] = time.Now()
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
	// TODO: change file owner
	nf, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		// (disk full, invalid argument)
		log.Info("create/open file error", err, "extra:", []byte(path))
		return
	}
	s.rwl.Lock()
	f = s.data[path]
	if f == nil {
		s.data[path] = nf
		f = nf
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
