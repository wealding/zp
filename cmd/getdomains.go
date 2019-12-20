package main

import (
	"bytes"
	"database/sql"
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
	insertStatement       = `INSERT IGNORE INTO domains (domain,tld) VALUES (?,?) `
	zoneExtension         = "gz"
	exceptionZoneFileName = "net.txt.gz"
	exceptionZone         = "net"
	tSize                 = 15000
)

func main() {
	var conn *sql.DB

	conn = connMysql()
	rc := make(chan zp.Record)
	var wg sync.WaitGroup
	makechan(conn, rc, wg)

	for {
		filepath.Walk("./files", func(path string, fi os.FileInfo, err error) error {
			if !strings.HasSuffix(path, zoneExtension) {
				return nil
			}
			if err := conn.Ping(); err != nil {
				conn = connMysql()
				makechan(conn, rc, wg)
			}
			log.Println(path)
			var fileName, tld string
			fileName = filepath.Base(path)
			tld = strings.Replace(fileName, ".txt.gz", "", -1)
			//处理完, 挪开gz文件
			timeStr := time.Now().Format("2006-01-02")
			os.MkdirAll("./backup/"+timeStr, os.ModePerm)               //先创建文件夹
			exist := fileExists("./backup/" + timeStr + "/" + fileName) //判断是否已经处理过, 处理过的话,直接挪文件
			if exist != true {
				//执行匹配
				if err := zp.FetchZoneFile(path, tld, rc); err != nil {
					log.Println(err)
				}
			}
			if err := os.Rename(path, "./backup/"+timeStr+"/"+fileName); err != nil {
				log.Println(err)
			}
			return nil
		})
		log.Println("waiting...")
		startdown()
		time.Sleep(5 * time.Second)
	}

	close(rc)
	wg.Wait()
}

func fileExists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func makechan(conn *sql.DB, rc <-chan zp.Record, wg sync.WaitGroup) {
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			if err := send(conn, rc); err != nil {
				log.Println(err)
			}
		}()
	}
}

func connMysql() *sql.DB {
	conn, err := sql.Open("mysql", "names:123456Qq@tcp(127.0.0.1:3306)/names")
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
	//次日19点开始下一次
	timeStr := time.Now().Format("2006-01-02")
	t2, _ := time.ParseInLocation("2006-01-02", timeStr, time.Local)
	nexttime := t2.AddDate(0, 0, 1).Unix() + 68400
	tm := time.Unix(nexttime, 0)

	if nowtime > filetime {
		buf := bytes.Buffer{}
		buf.WriteString(strconv.FormatInt(nexttime, 10))
		_ = ioutil.WriteFile("nextdown.txt", buf.Bytes(), 0666)
		fmt.Println("开始下载，下次下载时间：", tm.Format("2006-01-02 15:04:05"))
		var exefile string
		if fileExists("czds.exe") {
			exefile = "czds.exe"
		} else {
			exefile = "./czds"
		}
		cmd := exec.Command(exefile, "download")
		if err := cmd.Start(); err != nil {
			log.Println(err)
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
