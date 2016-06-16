package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/codemodus/sigmon"
	"github.com/codemodus/vitals"
)

const (
	width = 16
)

type filesInfo struct {
	dir string
	fsi []os.FileInfo
}

type result struct {
	path string
	data string
	err  error
}

func gzFilesInfoIn(dir string) (*filesInfo, error) {
	fsi, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for k := len(fsi) - 1; k >= 0; k-- {
		if fsi[k].IsDir() || !strings.HasSuffix(fsi[k].Name(), ".gz") {
			fsi = append(fsi[:k], fsi[k+1:]...)
		}
	}

	return &filesInfo{dir: dir, fsi: fsi}, nil
}

func digest(done <-chan struct{}, paths <-chan string, c chan<- result) {
	for p := range paths {
		r := result{path: p}

		func() {
			f, err := os.Open(p)
			if err != nil {
				r.err = err
				return
			}
			defer func() {
				_ = f.Close()
			}()

			gzr, err := gzip.NewReader(f)
			if err != nil {
				r.err = err
				return
			}
			defer func() {
				_ = gzr.Close()
			}()

			data, err := ioutil.ReadAll(gzr)
			if err != nil {
				r.err = err
				return
			}

			r.data = string(data)
		}()

		select {
		case c <- r:
		case <-done:
			return
		}
	}
}

func funnel(done <-chan struct{}, fsi *filesInfo) (<-chan result, <-chan error) {
	c := make(chan result)
	errc := make(chan error, 1)

	var wg sync.WaitGroup
	paths := make(chan string)

	go func() {
		go func() {
			for _, v := range fsi.fsi {
				select {
				case paths <- path.Join(fsi.dir, v.Name()):
				case <-done:
					errc <- errors.New("canceled")
				}
			}

			close(paths)
		}()

		wg.Add(width)
		for i := 0; i < width; i++ {
			go func() {
				digest(done, paths, c)
				wg.Done()
			}()
		}

		go func() {
			wg.Wait()
			close(c)
		}()
	}()

	return c, errc
}

func main() {
	sm := sigmon.New(nil)
	sm.Run()

	profM := flag.String("profmem", "",
		`Run memory profile and write to named file.`)

	flag.Parse()

	fsi, err := gzFilesInfoIn("./testdata/")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	done := make(chan struct{})
	defer close(done)

	sm.Set(func(sm *sigmon.SignalMonitor) {
		close(done)
	})

	rs, errc := funnel(done, fsi)

	for r := range rs {
		select {
		case err := <-errc:
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		default:
			fmt.Println(r.path, r.data, r.err)
		}
	}

	if err := vitals.WriteHeapProfile(*profM); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)

	}
}
