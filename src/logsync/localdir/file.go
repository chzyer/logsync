package localdir

import (
	"io"
	"os"
	"time"
	"sync"
	"sort"

	"logsync/log"
)

var (
	DefaultWriteSize = 2 << 20
	TimeIdle = time.Minute
)

type FileWriter interface {
	WriteAt(string, []byte, int64) (int, error)
}

type FileSlice []*File
func (f FileSlice) Less(i, j int) bool {
	return f[i].MTime.Before(f[j].MTime)
}
func (f FileSlice) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}
func (f FileSlice) Len() int {
	return len(f)
}
func SortFileSlice(l []*File) {
	sort.Sort(FileSlice(l))
}

type File struct {
	file *os.File // may nil
	fname, fpath string
	l sync.Mutex
	updated bool
	offset int64
	MTime time.Time
	updateTime time.Time
	buf []byte
}

func NewFile(dir, fname string, offset int64) (f *File, err error) {
	f = new(File)
	f.initPath(dir, fname)

	stat, err := os.Stat(f.fpath)
	if err != nil {
		return
	}
	if stat.IsDir() {
		err = ErrNotDirectory
		return
	}
	f.updateTime = time.Now()
	f.MTime = stat.ModTime()
	if offset < stat.Size() {
		f.Updated()
	}
	return
}

func (f *File) initPath(dir, fname string) {
	length := len(dir)
	if dir[length-1] == '/' {
		dir = dir[:length-1]
	}
	f.fpath = dir + "/" + fname
	f.fname = fname

}

func (f *File) IsUpdated() bool {
	return f.updated
}

func (f *File) NeedClose(now time.Time) bool {
	return now.Sub(f.updateTime) > TimeIdle
}

func (f *File) Updated() (err error) {
	log.Info("set file", f.fname, "updated")
	err = f.initFile()
	if err != nil {
		return
	}
	f.updated = true
	f.updateTime = time.Now()
	return
}

func (f *File) SetOffset(offset int64) {
	log.Info(f.fpath, offset)
	f.offset = offset
	f.updateTime = time.Now()
}

func (f *File) initFile() (err error) {
	if f.file != nil {
		return
	}
	f.file, err = os.OpenFile(f.fpath, os.O_RDONLY, 0666)
	if err != nil {
		return
	}
	f.buf = make([]byte, DefaultWriteSize)
	return
}

func (f *File) WriteTo(fw FileWriter) (n int, localErr, remoteErr error) {
	f.updateTime = time.Now()
	f.updated = false

	var rn, wn int
	for localErr==nil && remoteErr==nil {
		rn, localErr = f.file.ReadAt(f.buf, f.offset)
		if rn == 0 {
			break
		}

		wn, remoteErr = fw.WriteAt(f.fname, f.buf[:rn], f.offset)
		if wn == 0 {
			break
		}

		if wn != rn {
			remoteErr = io.ErrShortWrite
			break
		}
		n += wn
		f.offset += int64(wn)
	}

	if localErr == io.EOF && n > 0 {
		localErr = nil
	}
	return
}

func (f *File) Close() (err error) {
	f.updated = false
	if f.file != nil {
		err = f.file.Close()
		f.file = nil
		f.buf = nil
	}
	return
}
