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

/*
单文件
0x40  IN_MOVE
0x100 IN_CREATE
*/

/*
当localdir为单个文件时表示处理单个日志文件滚动，采用一下策略
1. 文件名使用inode命名，防止单文件滚动导致覆盖
2. data内可以存在多个文件(旧已滚动后的文件，还有当前已清空的文件)
3. 文件被删除事件无法被捕捉。(文件被移动了)
4. 文件更新后需要更变offset
*/
type LocalDir struct {
	Path string
	UseInodeName bool
	watcher *inotify.Watcher
	err error
	isdir bool
	rwl sync.RWMutex

	// 文件添加后，除非被删除，否则一直存留在data
	// 当文件空闲时，会被关闭，File.file会置于nil
	// 当file.updated=true时，File.file会被打开
	data map[string] *File

	deleted []string
}

func NewDir(p string, useInodeName bool) (d *LocalDir, err error) {
	stat, err := os.Stat(p)
	if err != nil {
		return
	}

	watcher, err := inotify.NewWatcher()
	if err != nil {
		return
	}

	err = watcher.AddWatch(p, inotify.IN_MODIFY|inotify.IN_DELETE|inotify.IN_MOVED_FROM|inotify.IN_CREATE)
	if err != nil {
		return
	}

	d = new(LocalDir)
	d.isdir = stat.IsDir()
	d.Path = p
	d.watcher = watcher
	d.UseInodeName = useInodeName
	size := 1<<10
	if ! d.isdir {
		size = 1
	}
	d.data = make(map[string] *File, size)
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
	for _, fname := range flist {
		item, ok := data[fname]
		if ! ok {
			// 服务器没返回的不添加
			continue
		}
		if item.Deleted {
			// 服务器标记删除的不添加
			continue
		}
		f, err := d.MakeFile(fname, item.Offset)
		if err != nil {
			continue
		}
		d.data[fname] = f
	}
}

func (d *LocalDir) MakeFile(fname string, offset int64) (f *File, err error) {
	f, err = NewFile(d.Path, fname, offset)
	if err != nil {
		return
	}
	if d.UseInodeName {
		f.AliasName(f.InodeString())
	}
	return
}

func (d *LocalDir) OnFileUpdate(fname string, truncate bool) (err error) {
	d.rwl.RLock()
	f := d.data[fname]
	d.rwl.RUnlock()
	if f != nil {
		if truncate {
			f.SetOffset(0)
		}
		err = f.Updated()
		return
	}

	// 文件新增
	nf, err := d.MakeFile(fname, 0)
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

func (d *LocalDir) onFileDelete(fname string) {
	d.rwl.Lock()
	d.deleted = append(d.deleted, fname)
	f := d.data[fname]
	if f != nil {
		f.Close()
	}
	log.Info("delete", fname)
	delete(d.data, fname)
	d.rwl.Unlock()
}

func (d *LocalDir) receiveEvent(errch chan error) {
	for {
		select {
		case ev := <-d.watcher.Event:
			// log.Info("get event", ev)
			// must be modify
			fname := filepath.Base(ev.Name)
			switch {
			case ev.Match(inotify.IN_CREATE):
				// offset 清0
				log.Info("truncate", ev.Name)
				err := d.OnFileUpdate(fname, true)
				if err != nil {
					log.Error(err)
				}
			case ev.Match(inotify.IN_MODIFY):
				err := d.OnFileUpdate(fname, false)
				if err != nil {
					log.Error(err)
				}
			case ev.Match(inotify.IN_DELETE):
				// 如果当前文件被移除，应该删除
				log.Info("get event", ev)
				d.onFileDelete(fname)
			case ev.Match(inotify.IN_MOVED_FROM):
				log.Todo("小心nginx 机制会覆盖文件", ev)
			}
		case err := <-d.watcher.Error:
			log.Error(err)
			errch <- err
			break
		}
	}
}

func (d *LocalDir) syncingFile(errch chan error, fw *remotedir.RemoteDir) {
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

		now := time.Now()
		d.rwl.Lock()
		{
			// close idle
			for _, f := range d.data {
				if f.NeedClose(now) {
					f.Close()
				}
			}

			// delete
			if len(d.deleted) > 0 {
				err := fw.DeleteFile(d.deleted)
				if err != nil {
					log.Error(err)
				} else {
					d.deleted = nil
				}
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
