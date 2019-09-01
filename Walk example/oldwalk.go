package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
)

type list []string
type result map[string]list

func hashfile(path string) string {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func search(dir string) (result, error) {
	hashes := make(result)

	err := filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if path.Ext(p) == ".pdf" {
			h := hashfile(p)
			hashes[h] = append(hashes[h], p)
		}
		return nil
	})

	return hashes, err
}

func main() {
	dupFlag := flag.Bool("d", false, "only print dups")
	searchDir := "."

	flag.Parse()

	if len(flag.Args()) > 0 {
		searchDir = flag.Args()[0]
	}

	hashes, err := search(searchDir)

	if err != nil {
		log.Fatal(err)
	}

	for hash, files := range hashes {
		if (len(files) > 1) || !*dupFlag {
			fmt.Println(hash[len(hash)-6:], len(files))
			for _, file := range files {
				fmt.Println("   ", file)
			}
		}
	}
}
