package main

import (
	"flag"
	invIndex "invertedIndex"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

func findDocuments(inv *invIndex.InvIndex) filepath.WalkFunc {
	return func(path string, f os.FileInfo, err error) error {
		if err != nil {
			log.Print(err)
			return nil
		}
		if !f.IsDir() {
			matched, err := regexp.MatchString(".txt", f.Name())
			if err == nil && matched {
				if err := inv.IndexDocument(path); err != nil {
					log.Fatal(err)
				}
				if err != nil {
					log.Fatal(err)
				}
			}
		}
		return nil
	}
}

func main() {
	start := time.Now()

	var filedir string
	flag.StringVar(&filedir, "filedir", "", "Directory you wish to access")
	flag.Parse()
	if filedir == "" {
		log.Fatal("not enough arguments")
	}

	file := invIndex.SafeOpenFile()
	invIndex.WriteHeader(file)
	
	inv := invIndex.NewIndex()
	err := filepath.Walk(filedir, findDocuments(inv))
	if err != nil {
		log.Fatal(err)
	}

	topTen := 10
	results, err := inv.SearchTopKQuery("the big bad wolf", topTen)
	if err != nil {
		log.Fatal()
	}
	for _, res := range results {
		invIndex.WriteToFile(file, res)
	}

	invIndex.CloseFile(file)
	elapsed := time.Since(start)
	log.Printf("index creation took %s", elapsed)

}

