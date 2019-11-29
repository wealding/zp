package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ilyaglow/zp"
)

const (
	insertStatement = `INSERT INTO dnszones (rtype, domain, value, tld) VALUES (?, ?, ?, ?)`
	createStatement = `
		CREATE TABLE IF NOT EXISTS dnszones (
			rtype	String,
			domain	String,
			value	String,
			tld		String,
			date	Date DEFAULT today()
		) engine=MergeTree(date,(value,rtype),8192)
	`
	zoneExtension         = "gz"
	exceptionZoneFileName = "net.txt.gz"
	exceptionZone         = "net"
	tSize                 = 10000
)

func main() {
	sd := flag.String("f", "./files", "Directory with zone files with .gz extension")
	nw := flag.Int("workers", 4, "Number of sending workers")
	flag.Parse()

	rc := make(chan zp.Record)

	var wg sync.WaitGroup
	wg.Add(*nw)

	for i := 0; i < *nw; i++ {
		go func() {
			defer wg.Done()
			if err := send(rc); err != nil {
				log.Println(err)
			}
		}()
	}

	filepath.Walk(*sd, func(path string, fi os.FileInfo, err error) error {
		if !strings.HasSuffix(path, zoneExtension) {
			return nil
		}

		if strings.HasSuffix(path, exceptionZoneFileName) {
			if err := zp.FetchZoneFile(path, exceptionZone, rc); err != nil {
				log.Fatal(err)
			}
		} else {
			if err := zp.FetchZoneFile(path, "", rc); err != nil {
				log.Fatal(err)
			}
		}

		return nil
	})

	close(rc)
	wg.Wait()
}

func send(input <-chan zp.Record) error {
	var it uint
	var curdomain, domainName string
	for rec := range input {
		domainName = rec.Domain[0 : len(rec.Domain)-1]
		if domainName != curdomain {
			curdomain = domainName
			log.Println(rec.RType + " - " + domainName + " - " + rec.Value + " - " + rec.TLD)
			it++
			if it == tSize {
				log.Printf("Commit transaction with %d entries", tSize)
				it = 0
			}
		}
	}

	log.Println("Committing the tail")
	return nil
}
