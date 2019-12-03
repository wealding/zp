package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wealding/zp"
)

const (
	insertStatement       = `INSERT IGNORE INTO domains (domain) VALUES (?) `
	zoneExtension         = "gz"
	exceptionZoneFileName = "net.txt.gz"
	exceptionZone         = "net"
	tSize                 = 10000
)

func main() {
	var conn *sql.DB
	sd := flag.String("f", "./files", "Directory with zone files with .gz extension")
	flag.Parse()

	conn = connMysql()
	rc := make(chan zp.Record)
	var wg sync.WaitGroup
	makechan(conn, rc, wg)

	for {
		filepath.Walk(*sd, func(path string, fi os.FileInfo, err error) error {
			if !strings.HasSuffix(path, zoneExtension) {
				return nil
			}
			if err := conn.Ping(); err != nil {
				conn = connMysql()
				makechan(conn, rc, wg)
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
		log.Println("waiting...")
		startdown()
		time.Sleep(10 * time.Second)
	}

	close(rc)
	wg.Wait()
}

func makechan(conn *sql.DB, rc <-chan zp.Record, wg sync.WaitGroup) {
	nw := flag.Int("workers", 10, "Number of sending workers")
	flag.Parse()

	wg.Add(*nw)

	for i := 0; i < *nw; i++ {
		go func() {
			defer wg.Done()
			if err := send(conn, rc); err != nil {
				log.Println(err)
			}
		}()
	}
}

func connMysql() *sql.DB {
	ch := flag.String("c", "root:7412369Qq@tcp(127.0.0.1:3306)/allji", "Mysql String")
	flag.Parse()

	conn, err := sql.Open("mysql", *ch)
	if err != nil {
		log.Fatal(err)
	}

	if err := conn.Ping(); err != nil {
		log.Fatal(err)
	}
	return conn
}

func startdown() {
	data, err := ioutil.ReadFile("nextdown.txt")
	if err != nil {
		fmt.Println("File reading error", err)
		return
	}
	filetime, err := strconv.ParseInt(string(data), 10, 64)
	nowtime := time.Now().Unix()
	if nowtime > filetime {
		buf := bytes.Buffer{}
		buf.WriteString(strconv.FormatInt(nowtime+86390, 10))
		_ = ioutil.WriteFile("nextdown.txt", buf.Bytes(), 0666)
		fmt.Println("开始下载，下次下载时间：", buf.Bytes())
		cmd := exec.Command("czds.exe", "download")
		if err := cmd.Start(); err != nil {
			log.Fatal(err)
		}
	}
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
				rec.Domain); err != nil {
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
