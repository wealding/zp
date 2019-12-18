package main

import (
	"log"
	"strings"

	"github.com/miekg/dns"
)

func main() {
	var line, tld string
	var (
		rtype string
		value string
		n     string
		s     []string
	)
	tld = "com"
	line = "SOUPONLY NS F1G1NS1.DNSPOD"
	//line = "SOUPONLY NS F1G1NS1.DNSPOD.NET."
	ss := strings.Split(line, " NS ")
	if len(ss) == 2 {
		line = strings.ToLower(ss[0]) + "." + tld + " NS " + strings.ToLower(ss[1])
		log.Println(line)
	} else {
		log.Println("NOT!")
	}

	rr, err := dns.NewRR(line)
	if err != nil {
		log.Println("NOT!")
	}

	if rr == nil {
		log.Println("empty record")
	}

	if n = rr.Header().Name; n == "" {
		log.Println("no domain found in the record")
	}

	//把n最后面的点去掉,并判断是否包含多于2个点或者没有点(根域)
	n = strings.TrimSuffix(n, ".")
	s = strings.Split(n, ".")
	if len(s) != 2 {
		log.Println("not a valid domain")
	}

	switch rr := rr.(type) {
	case *dns.NS:
		rtype = "NS"
		value = rr.Ns
	case *dns.A:
		rtype = "A"
		value = rr.A.String()
	case *dns.AAAA:
		rtype = "AAAA"
		value = rr.AAAA.String()
	case *dns.TXT:
		rtype = "TXT"
		value = strings.Join(rr.Txt, ", ")
	default:
		log.Println("unsupported record type")
	}
	log.Println(n)
	log.Println(rtype)
	log.Println(value)
}
