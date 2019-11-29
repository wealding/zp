zp - zone files parser
----------------------

Handy helper to parse zone files into some data store.

# Usage

No need to say that you should get zone files first (czds.icann.org, verisign etc).

## Ingest zonefiles

You do not need to unpack `.gz` files, the example will do it for you.

```
go run cmd/ingest-clickhouse/ingester.go -h
  -c string
    	Clickhouse URL (default "http://127.0.0.1:8123/default")
  -f string
    	Directory with zone files with .gz extension (default ".")
  -workers int
    	Number of sending workers (default 4)
```
