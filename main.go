/*

bigd collects file info from the "testdata" directory and concurrently
processes the files by decompressing the contents and then printing the
data or related error.

The "width" of concurrency is set by the constant "width". Parallelism is
scheduled properly regardless of CPUs available, and the processing will
be serial if only one CPU is available. Width, in this case, helps control
the maximum available goroutines to limit the usage of RAM (see heap
profile results).

Usage:
	* This is not properly setup to be built. Use "go run main.go".

Available flags:
	-slow
		Slow processing to clarify behavior.
	-profmem={filename}
		Run memory profile and write to named file.

*/
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
	"time"

	"github.com/codemodus/sigmon"
	"github.com/codemodus/vitals"
)

const (
	// width controls the amount of goroutines running the digest function.
	width = 16
)

var (
	// slow enables a slowing of the digest function to aid the
	// understanding of designed concurrency by changing the output timing.
	slow = false
)

// filesInfo holds a slice of os.FileInfo along with the directory the
// contents came from.
type filesInfo struct {
	dir string
	fsi []os.FileInfo
}

// result holds a full file path, processed data, and error (if any).
type result struct {
	path string
	data string
	err  error
}

// gzFilesInfoIn is a convenience function which grabs all gzipped files
// within the provided directory with a depth of 1.
func gzFilesInfoIn(dir string) (*filesInfo, error) {
	fsi, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for k := len(fsi) - 1; k >= 0; k-- {
		// Remove directories, and files without the correct extension.
		if fsi[k].IsDir() || !strings.HasSuffix(fsi[k].Name(), ".gz") {
			fsi = append(fsi[:k], fsi[k+1:]...)
		}
	}

	return &filesInfo{dir: dir, fsi: fsi}, nil
}

// digest processes the file located at the currently provided path, and
// sends out a new result. It could be used, instead, to communicate with
// relevant micorservices.
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

		if slow {
			time.Sleep(time.Second)
		}

		select {
		case c <- r:
		case <-done:
			return
		}
	}
}

// funnel receives a filesInfo type, spawns goroutines (determined by the
// const "width"), and closes
func funnel(done <-chan struct{}, fsi *filesInfo) (<-chan result, <-chan error) {
	c := make(chan result)
	errc := make(chan error, 1)

	var wg sync.WaitGroup
	paths := make(chan string)

	go func() {
		// anon go func sends paths down the correct channel.
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

		// setup digesters by width.
		wg.Add(width)
		for i := 0; i < width; i++ {
			go func() {
				digest(done, paths, c)
				wg.Done()
			}()
		}

		// wait and close result channel after paths have been processed.
		go func() {
			wg.Wait()
			close(c)
		}()
	}()

	return c, errc
}

func main() {
	// Ignore system signals.
	sm := sigmon.New(nil)
	sm.Run()

	// Define and parse app flags.
	flag.BoolVar(&slow, "slow", false,
		`Slow processing to clarify behavior.`)
	profM := flag.String("profmem", "",
		`Run memory profile and write to named file.`)

	flag.Parse()

	// Get populated filesInfo type.
	fsi, err := gzFilesInfoIn("./testdata/")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Setup done channel for cancellation.
	done := make(chan struct{})
	defer close(done)

	// Close done channel on any system signal.
	sm.Set(func(sm *sigmon.SignalMonitor) {
		close(done)
	})

	// Get results and error channels (is non-blocking).
	rs, errc := funnel(done, fsi)

	// Print result contents or error.
	for r := range rs {
		select {
		case err := <-errc:
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		default:
			fmt.Println(r.path, r.data, r.err)
		}
	}

	// Write heap profile if flag is set.
	if err := vitals.WriteHeapProfile(*profM); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)

	}

	// Return system signal handling to default.
	sm.Stop()
}
