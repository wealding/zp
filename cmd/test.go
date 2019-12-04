package main

import (
	"fmt"
	"time"
)

func main() {
	timeStr := time.Now().Format("2006-01-02")
	nowtime := time.Now().Unix()
	t, _ := time.ParseInLocation("2006-01-02 15:04:05", timeStr+" 23:59:59", time.Local)
	t2, _ := time.ParseInLocation("2006-01-02", timeStr, time.Local)
	fmt.Println(nowtime)
	fmt.Println(t.Unix() + 1)
	fmt.Println(t2.AddDate(0, 0, 1).Unix())
}