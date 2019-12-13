package zp

import (
	"bufio"
	"compress/gzip"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/miekg/dns"
)

// Record represents a single record
type Record struct {
	RType  string `db:"rtype"`  // record type: A, AAAA or NS by now
	Domain string `db:"domain"` // domain name without tld
	Value  string `db:"value"`  // value of the record: IPv4, IPv6 or nameserver domain name
	TLD    string `db:"tld"`    // com, name, ru etc
}

// DBRecord is the Record with a date
type DBRecord struct {
	Record
	Date time.Time `db:"date"` // actual data datetime
}

// NewRecord parses a line to a zone file record
func NewRecord(line string, tld string) (*Record, error) {
	var (
		rtype string
		value string
		n     string
		s     []string
	)

	if tld == "com" {
		ss := strings.Split(line, " NS ")
		if len(ss) == 2 {
			line = strings.ToLower(ss[0]) + "." + tld + " NS " + strings.ToLower(ss[1]) + "." + tld
		}
	}

	rr, err := dns.NewRR(line)
	if err != nil {
		return nil, err
	}

	if rr == nil {
		return nil, errors.New("empty record")
	}

	if n = rr.Header().Name; n == "" {
		return nil, errors.New("no domain found in the record")
	}

	//把n最后面的点去掉,并判断是否包含多于2个点或者没有点(根域)
	n = strings.TrimSuffix(n, ".")
	s = strings.Split(n, ".")
	if len(s) != 2 {
		return nil, errors.New("not a valid domain")
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
		return nil, errors.New("unsupported record type")
	}

	return &Record{
		Domain: n,
		RType:  rtype,
		Value:  value,
		TLD:    tld,
	}, nil
}

// FetchZoneFile fetches gzipped zone file and push Record entries
// to a channel specified in the config
func FetchZoneFile(path string, tld string, rc chan Record) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer g.Close()

	sc := bufio.NewScanner(g)
	for sc.Scan() {
		r, err := NewRecord(sc.Text(), tld)
		if err != nil {
			// log.Println(err)
			continue
		}
		rc <- *r
	}

	return nil
}
