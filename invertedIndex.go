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
	docID []byte
	freq  []byte
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
	termCount := make(map[string]int)
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
		termCount[lword]++
		list := inv.index[lword]
		l := len(list)
		if l > 0 {
			docIDInt := decodeVariant(list[l-1].docID)
			if docIDInt == x+1 {
				vFreq := encodeVariant(termCount[lword])
				list[l-1].freq = vFreq
				pdoc.size++
				continue
			} else {
				vDocID := encodeVariant(x + 1)
				inv.index[lword] = append(list, posting{vDocID, []byte{uint8(1)}})
				pdoc.size++
				continue
			}
		} else {
			vDocID := encodeVariant(x + 1)
			inv.index[lword] = append(list, posting{vDocID, []byte{uint8(1)}})
			pdoc.size++
		}
	}
	return nil
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

func decodeVariant(num []uint8) int {
	var res int
	var i int
	for {
		res |= (int(num[i]) & 127) << (7 * uint8(i))
		if (num[i] & 128) != 128 {
			break
		}
		i++
	}
	return res
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
				currentFreq := decodeVariant(qlist[j].freq)
				fdt := float64(currentFreq)
				currentDocID := decodeVariant(qlist[j].docID)
				wd := float64(inv.docsIndexed[currentDocID-1].size)
				Kd := k1 * ((1.0 - b) + b*wd/wa)
				wdt := (k1 + 1.0) * fdt / (Kd + fdt)
				inv.docsIndexed[currentDocID-1].score += wqt * wdt
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
