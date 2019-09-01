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

	"github.com/akutz/sortfold"
)

type ignores map[string]bool

var ignoreDirExts = ignores{
	".app":         true,
	".git":         true, // treated as an extension, not a name
	".pkg":         true,
	".idea":        true, // ditto
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

type hashedFile struct {
	hash string
	path string
}

type oneFileResult map[string]string

type fileList []string
type multiFileResult map[string]fileList

func hashFile(path string) hashedFile {
	f, err := os.Open(path)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	// MD5 may not be cryptographically secure but it works for
	// finding matching files well enough

	h := md5.New()
	_, err = io.Copy(h, f)

	if err != nil {
		log.Fatal(err)
	}

	// we need to format the hash since we're using string keys
	return hashedFile{fmt.Sprintf("%x", h.Sum(nil)), path}
}

func processFile(path string, results chan<- hashedFile, wg *sync.WaitGroup, limits chan bool) {
	limits <- true
	results <- hashFile(path)
	<-limits
	wg.Done()
}

func collectHashes1(in <-chan hashedFile, out chan<- oneFileResult) {
	hashes := make(oneFileResult)

	for result := range in {
		hashes[result.path] = result.hash
	}

	out <- hashes
}

func collectHashes2(in <-chan hashedFile, out chan<- multiFileResult) {
	hashes := make(multiFileResult)

	// we keep a multi-map of hash -> file paths that match, so any
	// new data must be appended

	for result := range in {
		hashes[result.hash] = append(hashes[result.hash], result.path)
	}

	out <- hashes
}

func walkDir(dir1 string, dir2 string, out chan<- hashedFile, wg *sync.WaitGroup, limits chan bool) {
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

		// we ignore our own top-level directory to avoid an infinite loop

		if fi.Mode().IsDir() && p != dir1 {
			// we also skip dir2 since dir1 could be in it
			if p == dir2 {
				return filepath.SkipDir
			}

			if _, ok := ignoreDirExts[filepath.Ext(p)]; ok {
				return filepath.SkipDir
			}

			if _, ok := ignoreDirs[p]; ok {
				return filepath.SkipDir
			}

			wg.Add(1)
			go walkDir(p, dir2, out, wg, limits)
			return filepath.SkipDir
		}

		// we ignore zero-length files since they will be duplicates of each other

		if fi.Mode().IsRegular() && fi.Size() > 0 {
			if _, ok := ignoreFiles[filepath.Base(p)]; !ok {
				wg.Add(1)
				go processFile(p, out, wg, limits)
			}
		}

		return nil
	}

	limits <- true
	err := filepath.Walk(dir1, visit)

	if err != nil {
		log.Fatal(err)
	}
}

func searchTree1(dir1 string, dir2 string, limits chan bool) oneFileResult {
	wg := new(sync.WaitGroup)
	files := make(chan hashedFile, cap(limits)) // feed hashes to collector
	result := make(chan oneFileResult)          // results from to the collector

	go collectHashes1(files, result)

	wg.Add(1)
	walkDir(dir1, dir2, files, wg, limits)
	wg.Wait()
	close(files)

	return <-result
}

func searchTree2(dir1 string, dir2 string, limits chan bool) multiFileResult {
	wg := new(sync.WaitGroup)
	files := make(chan hashedFile, cap(limits)) // feed hashes to collector
	result := make(chan multiFileResult)        // results from the collector

	go collectHashes2(files, result)

	wg.Add(1)
	walkDir(dir2, dir1, files, wg, limits)
	wg.Wait()
	close(files)

	return <-result
}

// utility to strip paths beginning with a prefix, i.e.,
// those in a directory we want to ignore

func removeWithPrefix(paths []string, path string) []string {
	result := make([]string, 0, len(paths))

	for _, v := range paths {
		if !strings.HasPrefix(v, path) {
			result = append(result, v)
		}
	}

	return result
}

// utilities for handling a command-line flag that allows
// itself to be used more than once, adding to a map

func (i *ignores) String() string {
	dirs := make([]string, 0, len(*i))

	for v, _ := range *i {
		dirs = append(dirs, v)
	}

	return fmt.Sprintf("%v", dirs)
}

func (i *ignores) Set(v string) error {
	(*i)[v] = true
	return nil
}

func main() {
	// usage walk [-d] [-q] [-i dir3] [dir1 | dir1 dir2]

	dupFlag := flag.Bool("d", false, "print files that are dups under dir2")
	quoteFlag := flag.Bool("q", false, "quote filenames")

	flag.Var(&ignoreDirs, "i", "ignore files under this path")
	flag.Parse()

	searchDir1 := "."
	searchDir2 := "."
	useIgnore := len(ignoreDirs) > 0
	useOneDir := false

	if len(flag.Args()) > 1 {
		searchDir1 = flag.Args()[0]
		searchDir2 = flag.Args()[1]
	} else if len(flag.Args()) > 0 {
		searchDir2 = flag.Args()[0]
	} else if !*dupFlag {
		log.Fatal("no directories specified\nusage: walk [-d] [-q] [-i dir3] [dir1 | dir1 dir2]")
	}

	if searchDir1 == searchDir2 {
		if *dupFlag {
			useOneDir = true
		} else {
			log.Fatal("same directory specified twice")
		}
	}

	// we create many goroutines but limit the number that can run concurrently

	nworkers := 4 * runtime.GOMAXPROCS(0)
	limits := make(chan bool, nworkers)

	result1 := make(chan oneFileResult)

	go func() {
		hashes := searchTree1(searchDir1, searchDir2, limits)

		result1 <- hashes
	}()

	result2 := make(chan multiFileResult)

	go func() {
		hashes := searchTree2(searchDir1, searchDir2, limits)

		result2 <- hashes
	}()

	dir1Hashes := <-result1 // file -> hash
	dir2Hashes := <-result2 // hash -> file list

	if *dupFlag {
		if useOneDir {
			if useIgnore {
				fmt.Printf("Files duplicated in %s ignoring %s\n\n", searchDir1, ignoreDirs.String())
			} else {
				fmt.Printf("Files duplicated in %s\n\n", searchDir1)
			}
		} else {
			if useIgnore {
				fmt.Printf("Files in %s also in %s ignoring %s\n\n", searchDir1, searchDir2, ignoreDirs.String())
			} else {
				fmt.Printf("Files in %s also in %s\n\n", searchDir1, searchDir2)
			}
		}
	} else {
		if useIgnore {
			fmt.Printf("Files in %s not in %s ignoring %s\n\n", searchDir1, searchDir2, ignoreDirs.String())
		} else {
			fmt.Printf("Files in %s not in %s\n\n", searchDir1, searchDir2)
		}
	}

	// get the sorted filenames from dir1 and iterate over them;
	// we use sortfold to get case-insensitive results (for Mac)

	keys := make([]string, len(dir1Hashes))

	i := 0

	for key := range dir1Hashes {
		keys[i] = key
		i++
	}

	sortfold.Strings(keys)

	if useOneDir {
		for hash, files := range dir2Hashes {
			// print only duplicates unless all files are desired

			if len(files) > 1 {
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
	} else {
		for _, file := range keys {
			hash := dir1Hashes[file]

			// if the file is present in dir2, and we want dups printed, so
			// list the dups; otherwise, list the files missing from dir2

			if files, ok := dir2Hashes[hash]; ok {
				if *dupFlag {
					// if we're ignoring a 3rd directory, remove any files that
					// have that directory as a prefix

					if useIgnore {
						for v, _ := range ignoreDirs {
							files = removeWithPrefix(files, v)
						}

						if len(files) == 0 {
							continue
						}
					}

					// use the last 7 digits like git does as a short ID and
					// then print all file paths indented under the summary

					if *quoteFlag {
						fmt.Println(strings.Replace(file, " ", "\\ ", -1), hash[len(hash)-7:], len(files))
					} else {
						fmt.Println(file, hash[len(hash)-7:], len(files))
					}

					for _, file := range files {
						if *quoteFlag {
							fmt.Println("   ", strings.Replace(file, " ", "\\ ", -1))
						} else {
							fmt.Println("   ", file)
						}
					}
				}
			} else if !*dupFlag {
				// if we're ignoring a 3rd directory, remove any files that
				// have that directory as a prefix

				if useIgnore {
					for v, _ := range ignoreDirs {
						files = removeWithPrefix(files, v)
					}

					if len(files) != 0 {
						continue
					}
				}

				// use the last 7 digits like git does as a short ID and
				// then print the actual filename that's missing

				if *quoteFlag {
					fmt.Println(hash[len(hash)-7:], strings.Replace(file, " ", "\\ ", -1))
				} else {
					fmt.Println(hash[len(hash)-7:], file)
				}
			}
		}
	}
}
