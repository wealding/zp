package main

import (
	"bytes"
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

	"github.com/wealding/zp"
)

const (
	zoneExtension         = "gz"
	exceptionZoneFileName = "net.txt.gz"
	exceptionZone         = "net"
	tSize                 = 10000
)

var tldname string

func main() {
	for {
		filepath.Walk("./files", func(path string, fi os.FileInfo, err error) error {
			if !strings.HasSuffix(path, zoneExtension) {
				return nil
			}
			log.Println(path)
			var fileName, tld string
			fileName = filepath.Base(path)
			tld = strings.Replace(fileName, ".txt.gz", "", -1)
			tldname = tld
			//处理完, 挪开gz文件
			timeStr := time.Now().Format("2006-01-02")
			os.MkdirAll("./backup/"+timeStr, os.ModePerm)               //先创建文件夹
			exist := fileExists("./backup/" + timeStr + "/" + fileName) //判断是否已经处理过, 处理过的话,直接挪文件
			if exist != true {
				rc := make(chan zp.Record)
				var wg sync.WaitGroup
				makechan(rc, wg, tld)
				//执行匹配
				if err := zp.FetchZoneFile(path, tld, rc); err != nil {
					log.Println(err)
				}
				close(rc)
				wg.Wait()
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

func makechan(rc <-chan zp.Record, wg sync.WaitGroup, tld string) {
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			if err := writetotxt(rc, tld); err != nil {
				log.Println(err)
			}
		}()
	}
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

func writetotxt(input <-chan zp.Record, tld string) error {
	var it uint
	var domainsStr string

	timeStr := time.Now().Format("2006-01-02")

	_, err := os.Stat("./txt/" + timeStr)

	if os.IsNotExist(err) {
		os.MkdirAll("./txt/"+timeStr, os.ModePerm)
	}

	for rec := range input {
		//这里拼字符串
		domainsStr = domainsStr + rec.Domain + "\n"

		it++
		if it == tSize {
			log.Printf("Write ./txt/"+timeStr+"/"+tld+".txt with %d entries", tSize)
			it = 0
			//写入文件
			writeBytesToFile("./txt/"+timeStr+"/"+tld+".txt", []byte(domainsStr))
			domainsStr = ""
		}
	}

	log.Println("Write the tail")
	//再写一下
	writeBytesToFile("./txt/"+timeStr+"/"+tld+".txt", []byte(domainsStr))
	domainsStr = ""
	return nil
}

func writeBytesToFile(filepath string, content []byte) {
	//打开文件，没有此文件则创建文件，将写入的内容append进去
	w1, error := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if error != nil {
		log.Println(error)
	}
	_, err1 := w1.Write(content)
	if err1 != nil {
		log.Println(err1)
	}
	errC := w1.Close()
	if errC != nil {
		log.Println(errC)
	}
}
