package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wealding/zp"
)

const (
	insertStatement       = `INSERT IGNORE INTO domains (domain, tld) VALUES (?, ?) `
	zoneExtension         = "gz"
	exceptionZoneFileName = "net.txt.gz"
	exceptionZone         = "net"
	tSize                 = 10000
)

func init() {
	iManPid := fmt.Sprint(os.Getpid())
	tmpDir := os.TempDir()

	if err := ProcExist(tmpDir); err == nil {
		pidFile, _ := os.Create(tmpDir + "\\imanPack.pid")
		defer pidFile.Close()

		pidFile.WriteString(iManPid)
	} else {
		os.Exit(1)
	}
}

func ProcExist(tmpDir string) (err error) {
	iManPidFile, err := os.Open(tmpDir + "\\imanPack.pid")
	defer iManPidFile.Close()

	if err == nil {
		filePid, err := ioutil.ReadAll(iManPidFile)
		if err == nil {
			pidStr := fmt.Sprintf("%s", filePid)
			pid, _ := strconv.Atoi(pidStr)
			_, err := os.FindProcess(pid)
			if err == nil {
				return errors.New("[ERROR] iMan升级工具已启动")
			}
		}
	}

	return nil
}

func main() {
	sd := flag.String("f", "./files", "Directory with zone files with .gz extension")
	ch := flag.String("c", "root:7412369Qq@tcp(127.0.0.1:3306)/allji", "Mysql String")
	nw := flag.Int("workers", 10, "Number of sending workers")
	flag.Parse()

	conn, err := sql.Open("mysql", *ch)
	if err != nil {
		log.Fatal(err)
	}

	if err := conn.Ping(); err != nil {
		log.Fatal(err)
	}

	rc := make(chan zp.Record)

	var wg sync.WaitGroup
	wg.Add(*nw)

	for i := 0; i < *nw; i++ {
		go func() {
			defer wg.Done()
			if err := send(conn, rc); err != nil {
				log.Println(err)
			}
		}()
	}

	filepath.Walk(*sd, func(path string, fi os.FileInfo, err error) error {
		if !strings.HasSuffix(path, zoneExtension) {
			return nil
		}
		var fileName, tld string
		fileName = filepath.Base(path)
		tld = strings.Replace(fileName, ".txt.gz", "", -1)
		//执行匹配
		if err := zp.FetchZoneFile(path, tld, rc); err != nil {
			log.Fatal(err)
		}
		//处理完, 挪开gz文件
		timeStr := time.Now().Format("2006-01-02")
		os.MkdirAll("./backup/"+timeStr, os.ModePerm)
		if err := os.Rename(path, "./backup/"+timeStr+"/"+fileName); err != nil {
			log.Fatal(err)
		}
		return nil
	})

	close(rc)
	wg.Wait()
}

func send(conn *sql.DB, input <-chan zp.Record) error {
	var it uint
	var curdomain, domainName string

	tx, err := conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(insertStatement)
	if err != nil {
		return err
	}

	for rec := range input {
		domainName = rec.Domain[0 : len(rec.Domain)-1]
		if domainName != curdomain {
			curdomain = domainName
			if _, err := stmt.Exec(
				rec.Domain,
				rec.TLD); err != nil {
				return err
			}

			it++

			if it == tSize {
				log.Printf("Commit transaction with %d entries", tSize)
				it = 0
				if err := tx.Commit(); err != nil {
					if strings.Contains(err.Error(), "Transaction") {
						log.Println(err)
					} else {
						log.Println("tx.Commit() failed")
						return err
					}
				}
				tx, err = conn.Begin()
				if err != nil {
					return err
				}
				stmt, err = tx.Prepare(insertStatement)
				if err != nil {
					return err
				}
			}
		}
	}

	log.Println("Committing the tail")
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
