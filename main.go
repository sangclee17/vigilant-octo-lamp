package main

import (
	"bufio"
	"flag"
	"fmt"
	invIndex "invertedIndex"
	"log"
	"os"
	"time"
)

func getInput(input chan string) {
	for {
		query := bufio.NewReader(os.Stdin)
		result, err := query.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		input <- result
	}
}

func main() {
	var filedir string
	flag.StringVar(&filedir, "filedir", "", "Directory you wish to access")
	flag.Parse()
	if filedir == "" {
		log.Fatal("not enough arguments")
	}

	inv := invIndex.NewIndex()

	var checkForNewFile bool
	input := make(chan string, 1)
	go getInput(input)

	for {
		file := invIndex.SafeOpenFile()
		invIndex.WriteHeader(file)
		inv.SearchDocuments(filedir, checkForNewFile)
		checkForNewFile = true
		topTen := 10
		fmt.Println("type your query")
		select {
		case q := <-input:
			results, err := inv.SearchTopKQuery(q, topTen)
			if err != nil {
				log.Fatal()
			}
			for _, res := range results {
				invIndex.WriteToFile(file, res)
			}
		}
		invIndex.CloseFile(file)
		time.Sleep(time.Minute)
	}
}
