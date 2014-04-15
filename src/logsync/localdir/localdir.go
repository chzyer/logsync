package localdir

import (
	"os"
	"time"
	"sync"
	"errors"
	"inotify"
	"path/filepath"

	"logsync/log"
	"logsync/remotedir"
)

var (
	ErrNotDirectory = errors.New("path except directory!")
)

type LocalDir struct {
	Path string
	watcher *inotify.Watcher
	err error
	rwl sync.RWMutex

	// 文件添加后，除非被删除，否则一直存留在data
	// 当文件空闲时，会被关闭，File.file会置于nil
	// 当文件被置于updated时，File.file会被打开
	data map[string] *File
}

func NewDir(p string) (d *LocalDir, err error) {
	stat, err := os.Stat(p)
	if err != nil {
		return
	}

	if ! stat.IsDir() {
		err = ErrNotDirectory
		return
	}

	watcher, err := inotify.NewWatcher()
	if err != nil {
		return
	}

	err = watcher.AddWatch(p, inotify.IN_MODIFY|inotify.IN_DELETE)
	if err != nil {
		return
	}

	d = new(LocalDir)
	d.Path = p
	d.watcher = watcher
	d.data = make(map[string] *File, 1<<10)
	return
}

func (d *LocalDir) GetSortedFiles() (ret []*File) {
	ret = make([]*File, 0, len(d.data))
	for _, f := range d.data {
		if ! f.IsUpdated() {
			continue
		}
		ret = append(ret, f)
	}
	SortFileSlice(ret)
	return
}

// 仅仅在初始化的时候运行
func (d *LocalDir) updateFileList(rd *remotedir.RemoteDir) {
	flist, err := d.fileList()
	if err != nil {
		return
	}
	data, err := rd.FileInfo(flist)
	if err != nil {
		return
	}
	for _, fp := range flist {
		item, ok := data[fp]
		if ! ok {
			// 服务器没返回的不添加
			continue
		}
		if item.Deleted {
			// 服务器标记删除的不添加
			continue
		}
		f, err := NewFile(d.Path, fp, item.Offset)
		if err != nil {
			continue
		}
		d.data[fp] = f
	}
}

func (d *LocalDir) OnFileUpdate(fname string) (err error) {
	d.rwl.RLock()
	f := d.data[fname]
	d.rwl.RUnlock()
	if f != nil {
		err = f.Updated()
		return
	}

	// 文件新增
	nf, err := NewFile(d.Path, fname, 0)
	if err != nil {
		log.Error(err)
		return
	}

	d.rwl.Lock()
	defer d.rwl.Unlock()
	f, ok := d.data[fname]
	if ok {
		err = f.Updated()
		return
	}

	d.data[fname] = nf
	return
}

func (d *LocalDir) OnFileDelete(fname string) {
	d.rwl.Lock()
	defer d.rwl.Unlock()

	f := d.data[fname]
	if f != nil {
		f.Close()
	}
	log.Info("delete", fname)
	delete(d.data, fname)
}

func (d *LocalDir) receiveEvent(errch chan error) {
	for {
		select {
		case ev := <-d.watcher.Event:
			// log.Info("get event", ev)
			// must be modify
			fname := filepath.Base(ev.Name)
			switch {
			case ev.Match(inotify.IN_MODIFY):
				err := d.OnFileUpdate(fname)
				if err != nil {
					log.Error(err)
				}
			case ev.Match(inotify.IN_DELETE):
				log.Info("get event", ev)
				d.OnFileDelete(fname)
			}
		case err := <-d.watcher.Error:
			log.Error(err)
			errch <- err
			break
		}
	}
}

func (d *LocalDir) syncingFile(errch chan error, fw FileWriter) {
	for _ = range time.Tick(time.Second) {
		d.rwl.RLock()
		flist := d.GetSortedFiles()
		for _, f := range flist {
			waitTime := time.Second
		reWrite:
			n, localErr, remoteErr := f.WriteTo(fw)
			if localErr != nil {
				log.Error(f.fpath, "localErr:", localErr, "n:", n)
				f.Close()
			} else if remoteErr != nil {
				log.Println(d.Path, "remoteErr:", remoteErr, "sleep", waitTime)
				// remote error, like, disk full, retry forever
				time.Sleep(waitTime)
				if waitTime < time.Minute {
					waitTime *= 2
				}
				goto reWrite
			}
		}
		d.rwl.RUnlock()

		// remove
		now := time.Now()
		d.rwl.Lock()
		for _, f := range d.data {
			if f.NeedClose(now) {
				f.Close()
			}
		}
		d.rwl.Unlock()
	}
}

func (d *LocalDir) Sync(rd *remotedir.RemoteDir) (err error) {
	// 只监控增加的文件。
	d.updateFileList(rd)
	errch := make(chan error)
	go d.receiveEvent(errch)
	go d.syncingFile(errch, rd)
	err = <- errch
	return
}

func (d *LocalDir) fileList() (list []string, err error) {
	dir, err := os.Open(d.Path)
	if err != nil {
		return
	}
	list, err = dir.Readdirnames(-1)
	return
}
