package localdir

import (
	"io"
	"os"
	"time"
	"sync"
	"sort"
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
	f[i], f[j] = f[i], f[j]
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
	willDelete bool
	Size int64
	err error
	buf []byte
}

func NewFile(dir, fname string, updated bool) (f *File, err error) {
	f = new(File)
	length := len(dir)
	if dir[length-1] == '/' {
		dir = dir[:length-1]
	}
	f.fpath = dir + "/" + fname
	f.fname = fname
	f.updated = updated
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
	f.Size = stat.Size()
	return
}

func (f *File) NeedFree(now time.Time) bool {
	if f.willDelete {
		return true
	}
	return now.Sub(f.updateTime) > TimeIdle
}

func (f *File) MarkDelete() {
	f.willDelete = true
}

func (f *File) Updated() {
	f.updated = true
	f.updateTime = time.Now()
}

func (f *File) SetOffset(offset int64) {
	f.offset = offset
	f.updateTime = time.Now()
}

func (f *File) initFile() {
	if f.file != nil {
		return
	}
	f.file, f.err = os.OpenFile(f.fpath, os.O_RDONLY, 0666)
	f.buf = make([]byte, DefaultWriteSize)
}

func (f *File) WriteTo(fw FileWriter) (n int, localErr, remoteErr error) {
	f.updateTime = time.Now()
	f.initFile()
	if f.err != nil {
		localErr = f.err
		return
	}
	stat, err := f.file.Stat()
	if err != nil {
		return
	}
	size := stat.Size()-f.offset

	var rn, wn int
	for localErr==nil && remoteErr==nil && size>0 {
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
		wn64 := int64(wn)
		f.offset += wn64
		size -= wn64
	}

	if localErr == io.EOF && n > 0 {
		localErr = nil
	}
	return
}

func (f *File) Close() (err error) {
	if f.file != nil {
		err = f.file.Close()
	}
	return
}
