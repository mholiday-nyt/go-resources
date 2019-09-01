package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// TODO - sync these between programs

type ignores map[string]bool

var ignoreDirExts = ignores{
	".app":         true,
	".pkg":         true,
	".git":         true,
	".lproj":       true,
	".pbproj":      true,
	".xcassets":    true,
	".framework":   true,
	".xcodeproj":   true,
	".xcworkspace": true,
	".xcdatamodel": true,
}

var ignoreDirs = ignores{}

var ignoreFiles = ignores{
	".DS_Store":  true,
	".gitignore": true,
}

type pair struct {
	hash string
	path string
}

type fileList []string
type results map[string]fileList

var wg sync.WaitGroup

func hashFile(path string) pair {
	f, err := os.Open(path)

	if err != nil && err != os.ErrNotExist {
		log.Fatal(err)
	}

	defer f.Close()

	// MD5 may not be cryptographically secure but it works for
	// finding matching files well enough

	h := md5.New()

	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	// we need to format the hash since we're using string keys

	return pair{fmt.Sprintf("%x", h.Sum(nil)), path}
}

func process(path string, out chan<- pair, limits2 chan bool) {
	limits2 <- true
	out <- hashFile(path)
	<-limits2
	wg.Done()
}

func collect(out <-chan pair, result chan results) {
	hashes := make(results)

	// we keep a multi-map of hash -> file paths that match, so we
	// need to append any new data

	for pair := range out {
		hashes[pair.hash] = append(hashes[pair.hash], pair.path)
	}

	result <- hashes
}

func walkDir(dir string, out chan<- pair, limits1 chan bool, limits2 chan bool) error {
	defer func() {
		//fmt.Fprintf(os.Stderr, "finished %s\n", dir)
		<-limits1
		wg.Done()
	}()

	visit := func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			if err == os.ErrNotExist {
				return nil
			} else {
				return err
			}
		}

		if fi.Mode().IsDir() && p != dir {
			if _, ok := ignoreDirExts[filepath.Ext(p)]; ok {
				return filepath.SkipDir
			}

			if _, ok := ignoreDirs[p]; ok {
				return filepath.SkipDir
			}

			wg.Add(1)
			//fmt.Fprintf(os.Stderr, "forked %s\n", p)
			go walkDir(p, out, limits1, limits2)
			return filepath.SkipDir
		}

		if fi.Mode().IsRegular() && fi.Size() > 0 {
			if _, ok := ignoreFiles[filepath.Base(p)]; !ok {
				wg.Add(1)
				go process(p, out, limits2)
			}
		}

		return nil
	}

	limits1 <- true
	return filepath.Walk(dir, visit)
}

func searchTree(dir string, limits1 chan bool, limits2 chan bool) (results, error) {
	workers := 4 * runtime.GOMAXPROCS(0) // a reasonable worker pool size

	out := make(chan pair, workers) // feed hashes to collector
	result := make(chan results)    // storage we'll give to the collector

	fmt.Fprintf(os.Stderr, "--- start collecting ---\n")

	// we want a pool of workers, not a goroutine per file;
	// each worker gets a path to a file that needs to be hashed
	// and when there are no more paths, it reports done

	// for i := 0; i < workers; i++ {
	//     go process(in, out, done)
	// }

	// we need another goroutine to act as a collector for the
	// workers' output so we don't block here; hashes is safe
	// since only this goroutine will mutate it

	go collect(out, result)

	fmt.Fprintf(os.Stderr, "--- walk started ---\n")

	// multi-threaded walk of the directory tree looking for files; we
	// create goroutines to handle directories in parallel which makes
	// only a very slight difference in the final result
	//
	// perhaps we could use another workgroup to limit the parallelism

	wg.Add(1)
	err := walkDir(dir, out, limits1, limits2)

	if err != nil {
		log.Fatal(err)
	}

	// signal no more paths to be hashed, which will eventually stop all workers

	wg.Wait()

	fmt.Fprintf(os.Stderr, "--- walk ended ---\n")

	// wait for all the workers to be done

	// for i := 0; i < workers; i++ {
	//     <-done
	// }

	// by closing "out" we signal that all the hashes have been collected;
	// we can't really do this in workers unless each would have a separate
	// channel to the collector instead of a shared channel

	close(out)

	hashes := <-result

	fmt.Fprintf(os.Stderr, "--- done collecting ---\n")

	// so now hashes will be up-to-date with all the data

	return hashes, err
}

func status(done <-chan bool) {
	for {
		select {
		case <-done:
			return

		case <-time.After(60 * time.Second):
			buf := make([]byte, 1<<16)

			runtime.Stack(buf, true)

			fmt.Fprintf(os.Stderr, "%s\n\n", buf)
		}
	}
}

func main() {
	fmt.Fprintf(os.Stderr, "GOMAXPROCS=%v\n", runtime.GOMAXPROCS(0))

	dupFlag := flag.Bool("d", false, "only print dups")
	// verbFlag := flag.Bool("v", false, "verbose output")

	searchDir := "."

	flag.Parse()

	if len(flag.Args()) > 0 {
		// see if a directory has been specified on the command line

		searchDir = flag.Args()[0]
	}

	limits1 := make(chan bool, 12) // limit walk children
	limits2 := make(chan bool, 20) // limit file children

	hashes, err := searchTree(searchDir, limits1, limits2)

	if err != nil {
		log.Fatal(err)
	}

	for hash, files := range hashes {
		// print only duplicates unless all files are desired

		if (len(files) > 1) || !*dupFlag {
			// use the last 6 digits like git does as a short ID and
			// then print all file paths indented under the summary

			fmt.Println(hash[len(hash)-6:], len(files))

			for _, file := range files {
				fmt.Println("   ", strings.Replace(file, " ", "\\ ", -1))
			}
		}
	}
}

// orig walk, one goroutine walks the tree: 56.11s
//            add buffer[workers] to out:   52.76s
//            add buffer[1024] to in:       no improvement
//            32 (vs 16) worker goroutines: 51.36
//
// from this we see that the tree walker dominates, but was blocked
// whenever the workers were blocked by the collector, so adding
// a buffer to out helped keep things moving
//
// new walk,  one goroutine per directory:  51.14s
//            add buffer[workers] to out:   50.53
//            add buffer[8*workers] to in:  50.03
//            add buffer[1024] to in:       no improvement
//            32 (vs 16) worker goroutines: 48.75
//
// from this we see that if we can walk the tree in parallel, we
// get more bang for the buck increasing the number of workers
// (provided we have buffers so the walkers aren't blocked)
//
// goroutine per directory and per file:    panic = "runtime: failed to create new OS thread"
//   so limit goroutines to 20 in-progress: 48.19s
//   bump that up to 200:                   bad, bad - thrashing
//   so let's try 64:                       51.41, a little slower
//   split 20 (file) + 20 (walk) routines:  50.26, not better
//   split  8 (file) +  8 (walk) routines:  50.44, not better
//   split 12 (file) + 20 (walk) routines:  52.05, not better
//   split 20 (file) + 12 (walk) routines:  49.95
//
// so from all this, just under 50s is about the limit of what we can achieve
// for the given directory (about 128G of files in my Dropbox); we need some
// level of parallelism in both the tree walk and the file processing to get
// the best result, and some buffering on the collector's input channel
//
// NOTE - we only get about 600% of the CPU, i.e., about 6 HTs worth out of 8
//
//
// the output looks like:
//
// fdd242 2
//    /Users/mholiday/Dropbox/Consolidated\ Downloads/Nokian_English_2013-2014.pdf
//    /Users/mholiday/Dropbox/Old\ Downloads/Nokian_English_2013-2014.pdf
// ec42a4 2
//    /Users/mholiday/Dropbox/Consolidated\ Desktop/Chautauqua Meadow.pdf
//    /Users/mholiday/Dropbox/Old\ Desktop/Chautauqua Meadow.pdf
// ...
