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

type InvIndex struct {
	index       map[string][]Posting
	docsIndexed []*Doc
}

type ByScore []*Doc

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

func NewIndex() *InvIndex {
	var inv InvIndex
	inv.index = make(map[string][]Posting)
	inv.docsIndexed = make([]*Doc, 0, 0)
	return &inv
}

func (inv *InvIndex) IndexDocument(path string) error {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	x := len(inv.docsIndexed)
	inv.docsIndexed = append(inv.docsIndexed, &Doc{filepath.Base(path), 0, 0.0})
	pdoc := inv.docsIndexed[x]

	data, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}

	for _, bword := range bytes.Fields(data) {
		bword := bytes.Trim(bword, ".,-~?!\"'`;:()<>[]{}\\|/=_+*&^%$#@")
		word := string(bword)
		lword := strings.ToLower(word)
		list := inv.index[lword]
		l := len(list)
		if l > 0 && list[l-1].docID == x+1 {
			list[l-1].fdt++
			pdoc.size++
			continue
		}
		inv.index[lword] = append(list, Posting{x + 1, 1})
		pdoc.size++
	}
	return nil
}

func (inv *InvIndex) ComputeCollectionSize() int {
	var totalNumberOfWords int

	for _, in := range inv.index {
		for _, i := range in {
			totalNumberOfWords += i.fdt
		}
	}
	return totalNumberOfWords
}

func (inv *InvIndex) NumberOfDocuments() int {
	var numOfDocs int

	for _, in := range inv.index {
		for _, i := range in {
			if i.docID > numOfDocs {
				numOfDocs = i.docID
			}
		}
	}
	return numOfDocs
}

func (inv *InvIndex) NumOfQueryDocs(term string) int {
	return len(inv.index[term])
}

func (inv *InvIndex) SearchTopKQuery(word string, num int) ([]*Doc, error) {
	query := make(map[string]int)
	collectionSize := float64(inv.ComputeCollectionSize())
	N := float64(inv.NumberOfDocuments())
	k1 := 1.2
	b := 0.75
	k3 := 100000.00

	str := strings.Split(word, " ")
	for _, val := range str {
		(query[strings.ToLower(val)])++
	}

	for i := 0; i < len(query); i++ {
		if num, ok := query[str[i]]; ok {
			ft := float64(len(inv.index[str[i]]))
			fqt := float64(num)
			wqt := math.Log((N-ft+0.5)/(ft+0.5)) * (k3 + 1) * fqt / (k3 + fqt)
			wa := collectionSize / N
			qlist := inv.index[str[i]]
			for j := range qlist {
				fdt := float64(qlist[j].fdt)
				wd := float64(inv.docsIndexed[qlist[j].docID-1].size)
				Kd := k1 * ((1.0 - b) + b*wd/wa)
				wdt := (k1 + 1.0) * fdt / (Kd + fdt)
				inv.docsIndexed[qlist[j].docID-1].score += wqt * wdt
			}
		}
	}
	sort.Sort(ByScore(inv.docsIndexed))

	var results []*Doc

	for i := 0; i < num; i++ {
		results = append(results, inv.docsIndexed[i])
	}

	return results, nil
}

func (d ByScore) Len() int { return len(d) }

func (d ByScore) Swap(i, j int) { d[i], d[j] = d[j], d[i] }

func (d ByScore) Less(i, j int) bool { return d[i].score > d[j].score }
