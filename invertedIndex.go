package invertedIndex

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Doc struct {
	name  string
	size  int
	score float64
}

type Posting struct {
	docID int
	fdt   int
}
type ByScore []*Doc

var invIndex map[string][]Posting
var indexed []*Doc

func WriteHeader(file *os.File) {
	fmt.Fprintf(file, "File Name, Size(words), Score\n")
}

func SafeOpenFile() *os.File {
	file, err := os.Create("result.csv")
	if err != nil {
		log.Fatal(err)
	}
	return file
}

func WriteToFile(file *os.File, d *Doc) {
	fmt.Fprintf(file, "%s,%d,%.4f\n", d.name, d.size, d.score)
}

func CloseFile(file *os.File) {
	err := file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func NewIndex() {
	invIndex = make(map[string][]Posting)
}

func IndexDocument(path string) error {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	x := len(indexed)
	indexed = append(indexed, &Doc{filepath.Base(path), 0, 0.0})
	pdoc := indexed[x]

	data, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}

	for _, bword := range bytes.Fields(data) {
		bword := bytes.Trim(bword, ".,-~?!\"'`;:()<>[]{}\\|/=_+*&^%$#@")
		word := string(bword)
		lword := strings.ToLower(word)
		list := invIndex[lword]
		l := len(invIndex[lword])
		if l > 0 && list[l-1].docID == x+1 {
			list[l-1].fdt++
			pdoc.size++
			continue
		}
		invIndex[lword] = append(list, Posting{x + 1, 1})
		pdoc.size++

	}
	return nil
}

func ComputeCollectionSize() int {
	var totalNumberOfWords int

	for _, inv := range invIndex {
		for _, in := range inv {
			totalNumberOfWords += in.fdt
		}
	}
	return totalNumberOfWords
}

func NumberOfDocuments() int {
	return len(indexed)
}

func NumOfQueryDocs(term string) int {
	return len(invIndex[term])
}

func SearchTopKQuery(word string, num int) ([]*Doc, error) {
	query := make(map[string]int)

	str := strings.Split(word, " ")
	for _, val := range str {
		(query[strings.ToLower(val)])++
	}
	for i := 0; i < len(query); i++ {
		if num, ok := query[str[i]]; ok {
			k1 := float64(1.2)
			b := float64(0.75)
			k3 := 100000.00
			N := float64(len(indexed))
			ft := float64(len(invIndex[str[i]]))
			fqt := float64(num)
			wqt := math.Log((N-ft+0.5)/(ft+0.5)) * (k3 + 1) * fqt / (k3 + fqt)
			collectionSize := ComputeCollectionSize()
			wa := float64(collectionSize / len(indexed))
			qlist := invIndex[str[i]]
			for i := range qlist {
				fdt := qlist[i].fdt
				wd := float64(indexed[qlist[i].docID-1].size)
				Kd := k1 * ((1 - b) + b*wd/wa)
				wdt := (k1 + 1) * float64(fdt) / (Kd + float64(fdt))
				indexed[qlist[i].docID-1].score += wqt * wdt
			}
		}
	}
	sort.Sort(ByScore(indexed))

	var results []*Doc

	for i := 0; i < num; i++ {
		results = append(results, indexed[i])
	}

	return results, nil
}

func (d ByScore) Len() int { return len(d) }

func (d ByScore) Swap(i, j int) { d[i], d[j] = d[j], d[i] }

func (d ByScore) Less(i, j int) bool { return d[i].score > d[j].score }
