package main

import (
	"log"
	"strings"
)

func main() {
	var line, tld string
	tld = "com"
	line = "NS1-0.ENMAXENVISION A 72.29.224.33"
	ss := strings.Split(line, " NS ")
	if len(ss) == 2 {
		line = strings.ToLower(ss[0]) + "." + tld + " NS " + strings.ToLower(ss[1]) + "." + tld
		log.Println(line)
	} else {
		log.Println("NOT!")
	}
}
