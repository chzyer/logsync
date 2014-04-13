package localdir

import (
	"log"
	"inotify"
	"io"
	"os"
	rd "remotedir"
	"sync"
	"time"
)

var (
	DefaultWriteSize = 2 << 20
)

type File struct {
	file       *os.File // may nil
	willDelete bool
	increased  bool
	updateTime time.Time
	offset     int64
}

func NewEmptyFile() (f *File) {
	f = new(File)
	f.updateTime = time.Now()
	f.increased = true
	return
}

func NewFile(path string, offset int64) (f *File, err error) {
	f = NewEmptyFile()
	f.file, err = os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return
	}
	if offset > 0 {
		f.offset = offset
	}
	return
}

func (f *File) Close() (err error) {
	return f.file.Close()
}

func (f *File) Size() (n int64, err error) {
	stat, err := f.file.Stat()
	if err != nil {
		return
	}

	n = stat.Size()
	return
}

func (f *File) WriteRemote(w *rd.RemoteDir) (n int, lerr, rerr error) {
	buf := make([]byte, DefaultWriteSize)
	f.updateTime = time.Now()

	// n < DefaultWriteSize
	wn := 0
	rn := DefaultWriteSize
	for lerr == nil && rerr == nil {
		rn, lerr = f.file.ReadAt(buf, f.offset)
		if rn == 0 {
			return
		}

		wn, rerr = w.WriteFile(buf[:rn], f.file.Name(), f.offset)
		if rerr != nil {
			log.Println("rerr", rerr)
		}
		if wn == 0 {
			return
		}

		if wn != rn {
			rerr = io.ErrShortWrite
			return
		}
		n += wn
		f.offset += int64(wn)
	}

	if lerr == io.EOF && n > 0 {
		lerr = nil
	}
	return
}

type Dir struct {
	remote  *rd.RemoteDir
	watcher *inotify.Watcher
	rwl     sync.RWMutex
	data    map[string]*File
	err     error
}

// ps: watch path
func NewDir(remoteDirAddr, p string) (d *Dir, err error) {
	if p[len(p)-1] == '/' {
		p = p[:len(p)-1]
	}
	d = new(Dir)
	d.watcher, err = inotify.NewWatcher()
	if err != nil {
		return
	}

	err = d.watcher.Watch(p)
	if err != nil {
		return
	}

	d.remote, err = rd.NewRemoteDir(remoteDirAddr)
	if err != nil {
		return
	}
	
	// get remote file list
	d.data = make(map[string]*File, 4096)
	dir, err := os.Open(p)
	if err != nil {
		return
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return
	}
	for idx, n := range names {
		names[idx] = p+"/"+n
	}
	offsets, err := d.remote.FileOffset(names)
	if err != nil {
		return
	}
	if len(offsets) != len(names) {
	}
	for idx, path := range names {
		d.data[path], err = NewFile(path, offsets[idx])
		if err != nil {
			continue
		}
	}

	// 需要知道offset
	go d.eventReceive()
	return
}

func (d *Dir) Sync() {
	for _ = range time.Tick(time.Second) {
		d.rwl.RLock()
		for p, f := range d.data {
			// init file
			if f.file == nil {
				log.Println("new file open", p)
				nf, err := os.OpenFile(p, os.O_RDONLY, 0666)
				if err != nil {
					f.willDelete = true
					continue
				}
				f.file = nf
			}

			// push data
			if f.increased {
				// log.Println("file increase", p)
				f.increased = false
				_, le, re := f.WriteRemote(d.remote)
				if le != nil {
					f.willDelete = true
					continue
				}
				if re != nil {
					log.Println(re)
					continue
				}
			}
		}
		d.rwl.RUnlock()

		// add remove
		now := time.Now()
		d.rwl.Lock()
		for p, f := range d.data {
			if f.willDelete || now.Sub(f.updateTime) > time.Minute {
				f.Close()
				delete(d.data, p)
			}
		}
		d.rwl.Unlock()
	}
}

func (d *Dir) FileCreated(path string) {
	d.rwl.Lock()
	_, ok := d.data[path]
	if !ok {
		log.Println("make new file")
		d.data[path] = NewEmptyFile()
	}
	d.rwl.Unlock()
}

func (d *Dir) SetIncreased(path string) {
	d.rwl.RLock()
	f, ok := d.data[path]
	if ok {
		f.increased = true
	}
	d.rwl.RUnlock()

	if ! ok {
		d.FileCreated(path)
	}
}

func (d *Dir) eventReceive() {
	for {
		select {
		case ev := <-d.watcher.Event:
			// log.Println(ev)
			switch {
			case ev.Match(inotify.IN_CREATE):
				log.Println("flag create", ev.Name)
				d.FileCreated(ev.Name)
			case ev.Match(inotify.IN_MODIFY):
				// log.Println("flag increase")
				d.SetIncreased(ev.Name)
			}
		case err := <-d.watcher.Error:
			d.err = err
		}
	}
}

func (d *Dir) Close() (err error) {
	d.rwl.Lock()
	for _, f := range d.data {
		f.Close()
	}
	d.data = make(map[string] *File, 1024)
	d.rwl.Unlock()
	err = d.watcher.Close()
	return
}
