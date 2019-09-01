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
	"sync/atomic"
)

type ignores map[string]bool

// TODO - add the "ignores" flags which add to these lists

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

var nf int32 = 0
var nd int32 = 0

type pair struct {
	hash string
	path string
}

type fileList []string
type results map[string]fileList

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

func process(path string, pairs chan<- pair, wg *sync.WaitGroup, limits chan bool) {
	// we don't need defer since there's no embedded return points
	// and no exceptions that could prematurely end this function

	limits <- true
	pairs <- hashFile(path)
	<-limits
	wg.Done()
}

func collect(pairs <-chan pair, result chan<- results) {
	hashes := make(results)

	// we keep a multi-map of hash -> file paths that match, so we
	// need to append (not insert) any new data

	for pair := range pairs {
		hashes[pair.hash] = append(hashes[pair.hash], pair.path)
	}

	result <- hashes
}

func walkDir(dir string, pairs chan<- pair, wg *sync.WaitGroup,
	limits chan bool, verb bool) error {
	defer func() {
		<-limits
		wg.Done()
	}()

	visit := func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			// ignore files that disappeared during the walk
			if err == os.ErrNotExist {
				return nil
			} else {
				return err
			}
		}

		// we must ignore the directory passed in or we'll
		// quickly fall into an infinite loop!
		//
		// also don't do the atomics unless we want verbose
		// output since they have a real cost

		if fi.Mode().IsDir() && p != dir {
			if verb {
				atomic.AddInt32(&nd, 1)
			}

			if _, ok := ignoreDirExts[filepath.Ext(p)]; ok {
				return filepath.SkipDir
			}

			if _, ok := ignoreDirs[p]; ok {
				return filepath.SkipDir
			}

			wg.Add(1)
			go walkDir(p, pairs, wg, limits, verb)
			return filepath.SkipDir
		}

		// we ignore zero-length files since they will all
		// naturally be duplicates of each other

		if fi.Mode().IsRegular() && fi.Size() > 0 {
			if verb {
				atomic.AddInt32(&nf, 1)
			}

			if _, ok := ignoreFiles[filepath.Base(p)]; !ok {
				wg.Add(1)
				go process(p, pairs, wg, limits)
			}
		}

		return nil
	}

	limits <- true
	return filepath.Walk(dir, visit)
}

func searchTree(dir string, nworkers int, verb bool) results {
	// we need two primitives to manage our goroutines:
	//
	// 1. because we're creating an unbounded number of goroutines, we
	//    don't use a buffered "done" channel as a semaphore to know
	//    when they're done (see walk/walk2); instead, we use a wait group
	// 2. in order to keep the goroutines from thrashing the system, we
	//    use a buffered channel as a limit on how many can be active
	//    at the same time: only so many things can be pushed on the
	//    channel until it blocks, something gets removed on completion
	//
	// NOTE that a wait group cannot be copied / passed by value;
	// if you do, you will likely see a panic labeled as
	// "sync: WaitGroup misuse: Add called concurrently with Wait"

	wg := new(sync.WaitGroup)
	limits := make(chan bool, nworkers) // limit in-progress work

	// our strategy is to "fan in" hash results from the file processors
	// to a single collector which can't be us, since we couldn't then
	// at the same time monitor the completion of all those workers
	//
	// so we need a channel into the collector and then out again; the
	// input channel has a small buffer so the collector doesn't block
	// the workers

	pairs := make(chan pair, nworkers) // feed hashes to collector
	result := make(chan results)       // get results back from it

	if verb {
		fmt.Fprintf(os.Stderr, "--- start collecting ---\n")
	}

	// we need another goroutine to act as a collector for the
	// workers' output so we don't block here

	go collect(pairs, result)

	if verb {
		fmt.Fprintf(os.Stderr, "--- walk started ---\n")
	}

	// this walk routine will create goroutines to find files in
	// directories as well as to hash files that meet our criteria

	wg.Add(1)

	err := walkDir(dir, pairs, wg, limits, verb)

	if err != nil {
		log.Fatal(err)
	}

	// wait for all the workers to be done

	wg.Wait()

	if verb {
		fmt.Fprintf(os.Stderr, "--- walk ended ---\n")
	}

	// by closing "out" we signal that all the hashes have been collected;
	// we can't really do this in workers unless each would have a separate
	// channel to the collector instead of a shared "fan in" channel

	close(pairs)

	// we will now block until the collector finishes inserting all hashes

	hashes := <-result

	if verb {
		fmt.Fprintf(os.Stderr, "--- done collecting ---\n")
	}

	return hashes
}

func main() {
	nworkers := 4 * runtime.GOMAXPROCS(0)

	dupFlag := flag.Bool("d", false, "only print dups")
	verbFlag := flag.Bool("v", false, "verbose output")
	quoteFlag := flag.Bool("q", false, "quote filenames")
	searchDir := "."

	flag.Parse()

	if len(flag.Args()) > 0 {
		// see if a directory has been specified on the command line

		searchDir = flag.Args()[0]
	}

	if *verbFlag {
		fmt.Fprintf(os.Stderr, "GOMAXPROCS=%v, nworkers=%v\n", runtime.GOMAXPROCS(0),
			nworkers)
	}

	hashes := searchTree(searchDir, nworkers, *verbFlag)

	if *verbFlag {
		fmt.Fprintf(os.Stderr, "found %d dirs, %d files\n", nd, nf)
	}

	for hash, files := range hashes {
		// print only duplicates unless all files are desired

		if (len(files) > 1) || !*dupFlag {
			// use the last 7 digits like git does as a short ID and
			// then print all file paths indented under the summary

			fmt.Println(hash[len(hash)-7:], len(files))

			// TODO - we need a better Mac filename quote routine

			for _, file := range files {
				if *quoteFlag {
					file = strings.Replace(file, " ", "\\ ", -1)
				}

				fmt.Println("   ", file)
			}
		}
	}
}

// Some performance experiments leading to this version (walk4a)
//
// first we tried walk:
// (these first two versions used a fixed-size pool of hash workers
// which fed off a channel "in" of paths from the tree walk)
//
// orig walk, one goroutine walks the tree: 56.11s
//            add buffer[workers] to out:   52.76s
//            add buffer[1024] to in:       no improvement
//            32 (vs 16) worker goroutines: 51.36
//
// from this we see that the tree walker dominates, but was blocked
// whenever the workers were blocked by the collector, so adding
// a buffer to the "out" channel helped keep things moving a bit
//
// then we tried walk2:
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
// [ignore walk3 which does a diff, it's something else entirely]
//
// then we tried walk4 and 4a:
//
// a goroutine per directory and per file:  panic ("runtime: failed to create new OS thread")
//   so limit goroutines to 20 in-progress: 48.19s
//   so limit goroutines to 32 in-progress: 46.93s
//   bump that up to 200:                   bad, bad - thrashing
//   so let's try 64:                       51.41, a little slower
// what if we limit by type separately?
//   split 20 (file) + 20 (walk) routines:  50.26, not better
//   split  8 (file) +  8 (walk) routines:  50.44, not better
//   split 12 (file) + 20 (walk) routines:  52.05, not better
//   split 20 (file) + 12 (walk) routines:  49.95
//
// this version (4a) doesn't limit by type, just by the total number of active
// goroutines (walking or hashing), and outperforms everything else (slightly);
// it actually generates an unbounded number of goroutines most of which are
// short-lived, but only a few can be doing real work at any one time
//
// the runtime failure occurs because threads blocked on syscalls don't count
// towards GOMAXPROCS and we were creating waay to many of those; we just can't
// afford O(10k) goroutines trying to hit the filesystem all at once, so we
// must limit work in progress somehow (memory for the stacks is not an issue)
//
// RESULTS:
// so from all this, just under 50s is about the limit of what we can achieve
// for the given directory (about 128G of files in my Dropbox, or about 6200
// directories and over 42000 files); we need some level of parallelism in both
// the tree walk and the file processing to get the best result, and some
// buffering on the collector's input channel
//
// NOTE - we only use about 600% of the CPU, i.e., about 3 cores or 6 hyper-
// threads (out of 8) on a quad-core i7 laptop when we're running flat out; I
// don't know what's occupying the other 2 HTs (1 core)
//
// the non-parallel version of this code ran about 6 times slower, so that all
// makes sense
//
//
// the output (with -q) looks like:
//
// 3391c3f 2
//     /Users/mholiday/Dropbox/Books/Pocket/Java\ 8\ Pocket\ Guide.pdf
//     /Users/mholiday/Dropbox/Books/Pocket/Java\ Pocket\ SE\ 8.pdf
// de561c1 2
//     /Users/mholiday/Dropbox/Consolidated\ Downloads/FBA\ 131228/fba-1-131228.pdf
//     /Users/mholiday/Dropbox/Old\ Downloads/FBA\ 131228/fba-1-131228.pdf
// ...
