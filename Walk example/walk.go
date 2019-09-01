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
)

type ignores map[string]bool

// TODO - sync these between programs

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

func hashFile(path string) pair {
	f, err := os.Open(path)

	if err != nil && err != os.ErrNotExist {
		log.Fatal(err)
	}

	defer f.Close()

	// MD5 may not be cryptographically secure but it works for
	// finding matching files well enough

	// TODO - handle one-byte files with just '\n' inside

	h := md5.New()

	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	// we need to format the hash since we're using string keys

	return pair{fmt.Sprintf("%x", h.Sum(nil)), path}
}

func processFiles(in <-chan string, out chan<- pair, done chan<- bool) {
	for path := range in {
		out <- hashFile(path)
	}

	done <- true
}

func collectHashes(verb bool, out <-chan pair, result chan<- results) {
	i := 0
	hashes := make(results)

	// we keep a multi-map of hash -> file paths that match, so any
	// new data must be appended

	for pair := range out {
		hashes[pair.hash] = append(hashes[pair.hash], pair.path)
		i++
	}

	if verb {
		fmt.Fprintf(os.Stderr, "%d files processed\n", i)
	}

	result <- hashes
}

func searchTree(verb bool, dir string, ignoreDirs map[string]bool) (results, error) {
	workers := 4 * runtime.GOMAXPROCS(0) // a reasonable worker pool size

	in := make(chan string, 1024)    // feed paths to workers
	out := make(chan pair, workers)  // feed hashes to collector
	done := make(chan bool, workers) // worker semaphores
	result := make(chan results)     // storage we'll give to the collector

	if verb {
		fmt.Fprintf(os.Stderr, "--- start collecting ---\n")
	}

	// we want a pool of workers, not a goroutine per file;
	// each worker gets a path to a file that needs to be hashed
	// and when there are no more paths, it reports done

	for i := 0; i < workers; i++ {
		go processFiles(in, out, done)
	}

	// we need another goroutine to act as a collector for the
	// workers' output so we don't block here

	go collectHashes(verb, out, result)

	if verb {
		fmt.Fprintf(os.Stderr, "--- walk started ---\n")
	}

	// single-threaded walk of the directory tree looking for files; we
	// could create goroutines to handle directories in parallel but
	// the real work is reading all the bytes of each file, and we're
	// doing the walk in parallel with actual processing -- it turns out
	// that a goroutine pools actually doesn't make much of a difference

	err := filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			if err == os.ErrNotExist {
				return nil
			} else {
				return err
			}
		}

		// we will ignore app and framework directories as well as zero-
		// length files (e.g., __init__.py)

		if fi.Mode().IsDir() {
			if _, ok := ignoreDirExts[filepath.Ext(p)]; ok {
				return filepath.SkipDir
			}

			if _, ok := ignoreDirs[p]; ok {
				return filepath.SkipDir
			}
		}
		if fi.Mode().IsRegular() && fi.Size() > 0 {
			if _, ok := ignoreFiles[filepath.Base(p)]; !ok {
				in <- p
			}
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	// signal no more paths to be hashed, which will eventually stop all workers

	close(in)

	if verb {
		fmt.Fprintf(os.Stderr, "--- walk ended ---\n")
	}

	// wait for all the workers to be done

	for i := 0; i < workers; i++ {
		<-done
	}

	// by closing "out" we signal that all the hashes have been collected;
	// we can't really do this in workers unless each would have a separate
	// channel to the collector instead of a shared channel

	close(out)

	hashes := <-result

	if verb {
		fmt.Fprintf(os.Stderr, "--- done collecting ---\n")
	}

	// so now hashes will be up-to-date with all the data

	return hashes, err
}

// stuff to make a command-line flag that can have multiple entries

func (i *ignores) String() string {
	dirs := make([]string, 4)

	for v, _ := range *i {
		dirs = append(dirs, v)
	}

	return fmt.Sprintf("%v", dirs)
}

func (i *ignores) Set(v string) error {
	(*i)[v] = true
	return nil
}

// TODO - need smarter quoting logic to handle "()&" chars, etc.

func main() {
	// usage walk [-d] [-q] [-v] [-n ignore-dir+] [search-dir]

	dupFlag := flag.Bool("d", false, "only print dups")
	verbFlag := flag.Bool("v", false, "verbose output")
	quoteFlag := flag.Bool("q", false, "quote filenames")

	flag.Var(&ignoreDirs, "n", "ignore files under this path")
	flag.Var(&ignoreFiles, "f", "ignore matching filenames")

	flag.Parse()

	if *verbFlag {
		fmt.Fprintf(os.Stderr, "GOMAXPROCS=%v\n", runtime.GOMAXPROCS(0))
	}

	searchDir := "."

	if len(flag.Args()) > 0 {
		searchDir = flag.Args()[0]
	}

	hashes, err := searchTree(*verbFlag, searchDir, ignoreDirs)

	if err != nil {
		log.Fatal(err)
	}

	for hash, files := range hashes {
		// print only duplicates unless all files are desired

		if (len(files) > 1) || !*dupFlag {
			// use the last 7 digits like git does as a short ID and
			// then print all file paths indented under the summary

			fmt.Println(hash[len(hash)-7:], len(files))

			for _, file := range files {
				if *quoteFlag {
					file = strings.Replace(file, " ", "\\ ", -1)
				}

				fmt.Println("   ", file)
			}
		}
	}
}

// the output (with -q) looks like:
//
// 3391c3f 2
//     /Users/mholiday/Dropbox/Books/Pocket/Java\ 8\ Pocket\ Guide.pdf
//     /Users/mholiday/Dropbox/Books/Pocket/Java\ Pocket\ SE\ 8.pdf
// de561c1 2
//     /Users/mholiday/Dropbox/Consolidated\ Downloads/FBA\ 131228/fba-1-131228.pdf
//     /Users/mholiday/Dropbox/Old\ Downloads/FBA\ 131228/fba-1-131228.pdf
// ...
