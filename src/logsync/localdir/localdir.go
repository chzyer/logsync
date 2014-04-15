package localdir

import (
	"os"
	"time"
	"sync"
	"errors"
	"inotify"

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

	err = watcher.AddWatch(p, inotify.IN_MODIFY)
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
		ret = append(ret, f)
	}
	SortFileSlice(ret)
	return
}

// updated 表示是否更新File里面的updated值
// ok=false if type(fname)==directory or not exist(fname)
func (d *LocalDir) setFile(fname string, updated bool) (f *File, ok bool) {
	ok = true
	d.rwl.RLock()
	f = d.data[fname]
	d.rwl.RUnlock()
	if f != nil {
		if updated {
			f.Updated()
		}
		return
	}

	f, err := NewFile(d.Path, fname, updated)
	if err != nil {
		ok = false
		return
	}
	d.rwl.Lock()
	defer d.rwl.Unlock()
	nf, ok := d.data[fname]
	if ! ok {
		d.data[fname] = f
		return
	}

	f = nf
	if updated {
		f.Updated()
	}
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
		f, ok := d.setFile(fp, false)
		if ok {
			f.SetOffset(item.Offset)
			// 如果服务器offset小于文件大小，标记为更新
			if f.Size > item.Offset {
				f.Updated()
			}
		}
	}
}

func (d *LocalDir) receiveEvent(errch chan error) {
	for {
		select {
		case ev := <-d.watcher.Event:
			// must be modify
			d.setFile(ev.Name, true)
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
		log.Info("loop", len(flist))
		for _, f := range flist {
			waitTime := time.Second
		reWrite:
			_, localErr, remoteErr := f.WriteTo(fw)
			if localErr != nil {
				log.Error("localErr:", localErr)
				f.MarkDelete()
			}
			if remoteErr != nil {
				log.Println("remoteErr:", remoteErr, "sleep", waitTime)
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
		removed := 0
		d.rwl.Lock()
		for p, f := range d.data {
			if f.NeedFree(now) {
				removed ++
				f.Close()
				delete(d.data, p)
			}
		}
		d.rwl.Unlock()
		log.Info("remove", removed, "file")
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
