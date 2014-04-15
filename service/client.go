package main
// run: make -C ../ client && ../bin/client -c ../client.conf

import (
	"os"
	"flag"
	"io/ioutil"
	"encoding/json"
	"path/filepath"

	"logsync/log"
	"logsync/remotedir"
	"logsync/localdir"
)

var (
	confpath = flag.String("c", "client.conf", "conf file")
)

type Conf struct {
	Host string `json:"host"`
	Path []string `json:"path"` // support glob
	ServerAddr string `json:"server_addr"`
}

type Client struct {
	*Conf
	DirPath []string
}

func NewClient(c *Conf) (client *Client) {
	client = &Client{Conf:c}
	client.UpdatePath()
	log.Info("sync directory...", client.DirPath)
	return
}

func (c *Client) UpdatePath() {
	for _, pb := range c.Path {
		ps, err := filepath.Glob(pb)
		if err != nil {
			log.Error(err)
			continue
		}
		for _, p := range ps {
			stat, _ := os.Stat(p)
			if stat != nil && stat.IsDir() {
				c.DirPath = append(c.DirPath, p)
			}
		}
	}
}

func (c *Client) syncDir(p string, errChan chan error) {
	var err error
	defer func() {
		if err != nil {
			errChan <- err
		}
	}()

	remote, err := remotedir.NewDir(c.ServerAddr, &remotedir.Info {
		Path: p,
		Host: c.Host,
	})
	if err != nil {
		return
	}

	dir, err := localdir.NewDir(p)
	if err != nil {
		return
	}
	err = dir.Sync(remote)
}

func (c *Client) Run() (err error) {
	errChan := make(chan error)
	for _, p := range c.DirPath {
		go c.syncDir(p, errChan)
	}
	err = <- errChan
	return
}

func main() {
	flag.Parse()
	data, err := ioutil.ReadFile(*confpath)
	if err != nil {
		log.Exit(err)
	}

	conf := new(Conf)
	if err=json.Unmarshal(data, conf); err != nil {
		log.Exit(err)
	}

	c := NewClient(conf)
	if err=c.Run(); err != nil {
		log.Exit(err)
	}
}
