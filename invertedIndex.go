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

type doc struct {
	name  string
	size  int
	score float64
}

type posting struct {
	docID int
	fdt   int
}

type InvIndex struct {
	index       map[string][]posting
	docsIndexed []doc
}

type byScore []doc

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

func WriteToFile(file *os.File, d doc) {
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
	inv.index = make(map[string][]posting)
	inv.docsIndexed = make([]doc, 0, 0)
	return &inv
}

func (inv *InvIndex) IndexDocument(path string) error {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	x := len(inv.docsIndexed)
	inv.docsIndexed = append(inv.docsIndexed, doc{filepath.Base(path), 0, 0.0})
	pdoc := &inv.docsIndexed[x]

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
		inv.index[lword] = append(list, posting{x + 1, 1})
		pdoc.size++
	}
	return nil
}

func (inv *InvIndex) CompressIndex() []byte {
	var last int
	for i := range inv.index {
		for j := range inv.index[i] {
			current := inv.index[i][j].docID
			inv.index[i][j].docID = current - last
			last = current
		}
	}

	compressed := make([]uint8, 0, 0)
	for i := range inv.index {
		for j := range inv.index[i] {
			vByte := encodeVariant(inv.index[i][j].docID)
			for _, v := range vByte {
				compressed = append(compressed, v)
			}
		}
	}
	return compressed
}

func encodeVariant(num int) []uint8 {
	output := make([]uint8, 0, 4)
	for num > 127 {
		output = append(output, (uint8(num)&127)|128)
		num = num >> 7
	}
	output = append(output, (uint8(num))&127)
	return output
}

func (inv *InvIndex) DecompressIndex(compressed []uint8) {
	result := make([]int, 0, 0)
	var temp int
	var k int

	for i := range compressed {
		temp |= (int(compressed[i]) & 127) << (7 * uint8(k))
		if (compressed[i] & 128) != 128 {
			result = append(result, temp)
			temp, k = 0, 0
			continue
		}
		k++
	}

	var x int
	for i := range inv.index {
		for j := range inv.index[i] {
			inv.index[i][j].docID = result[x]
			x++
		}
	}

	var last int
	for i := range inv.index {
		for j := range inv.index[i] {
			delta := inv.index[i][j].docID
			inv.index[i][j].docID = delta + last
			last = inv.index[i][j].docID
		}
	}
}

func (inv *InvIndex) computeCollectionSize() int {
	var totalNumberOfWords int

	for _, in := range inv.docsIndexed {
		totalNumberOfWords += in.size
	}
	return totalNumberOfWords
}

func (inv *InvIndex) numberOfDocuments() int {
	return len(inv.docsIndexed)
}

func (inv *InvIndex) SearchTopKQuery(word string, num int) ([]doc, error) {
	query := make(map[string]int)
	collectionSize := float64(inv.computeCollectionSize())
	N := float64(inv.numberOfDocuments())
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
	sort.Sort(byScore(inv.docsIndexed))

	var results []doc

	for i := 0; i < num; i++ {
		results = append(results, inv.docsIndexed[i])
	}

	return results, nil
}

func (d byScore) Len() int { return len(d) }

func (d byScore) Swap(i, j int) { d[i], d[j] = d[j], d[i] }

func (d byScore) Less(i, j int) bool { return d[i].score > d[j].score }
