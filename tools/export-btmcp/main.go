// export-btmcp reads a bt-mcp BadgerDB and outputs a binary dump
// compatible with `bti crawl --migrate`.
//
// Output format per entry:
//   [20 bytes] infohash
//   [8 bytes]  u64 BE discovered_at (unix seconds)
//   [8 bytes]  u64 BE size
//   [2 bytes]  u16 BE name_length
//   [name_length bytes] name UTF-8
//
// Usage: go run . /path/to/badger > dump.bin
//        go run . /path/to/badger | bti crawl --migrate -

package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/dgraph-io/badger/v4"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <badger-db-path>\n", os.Args[0])
		os.Exit(1)
	}

	dbPath := os.Args[1]

	opts := badger.DefaultOptions(dbPath).
		WithReadOnly(true).
		WithLogger(nil)

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("open badger: %v", err)
	}
	defer db.Close()

	out := os.Stdout
	count := 0

	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte("T")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// Key format: "T" + 20-byte infohash
			if len(key) != 21 {
				continue
			}

			var infohash [20]byte
			copy(infohash[:], key[1:21])

			err := item.Value(func(val []byte) error {
				// bt-mcp value format (from store.go marshalTorrent):
				// [8 bytes] timestamp (u64 BE, unix seconds)
				// [8 bytes] size (u64 BE)
				// [1 byte]  category
				// [2 bytes] name_len (u16 BE)
				// [name_len bytes] name
				// [2 bytes] file_count (u16 BE)
				// [files...]
				if len(val) < 21 { // 8+8+1+2+0+2 minimum
					return nil
				}

				timestamp := binary.BigEndian.Uint64(val[0:8])
				size := binary.BigEndian.Uint64(val[8:16])
				// skip category byte at val[16]

				nameLen := int(binary.BigEndian.Uint16(val[17:19]))
				if 19+nameLen > len(val) {
					return nil // corrupted entry
				}
				name := val[19 : 19+nameLen]

				// Write binary dump format
				out.Write(infohash[:])

				var tsBuf [8]byte
				binary.BigEndian.PutUint64(tsBuf[:], timestamp)
				out.Write(tsBuf[:])

				var sizeBuf [8]byte
				binary.BigEndian.PutUint64(sizeBuf[:], size)
				out.Write(sizeBuf[:])

				outNameLen := nameLen
				if outNameLen > 65535 {
					outNameLen = 65535
				}
				var nameLenBuf [2]byte
				binary.BigEndian.PutUint16(nameLenBuf[:], uint16(outNameLen))
				out.Write(nameLenBuf[:])
				out.Write(name[:outNameLen])

				count++
				if count%10000 == 0 {
					fmt.Fprintf(os.Stderr, "exported %d entries...\n", count)
				}

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		log.Fatalf("iterate: %v", err)
	}

	fmt.Fprintf(os.Stderr, "export complete: %d entries\n", count)
}
