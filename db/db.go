package db

import (
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vitalvas/zerodb/lib/crc16"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// FOR ZERODB_LOADINDEX
// recomendation: use 1 partition peer 60GB and peer HDD (raid array)
// Ram: 1GB peer 1TB for HDD cache AND 256-386MB peer partition
// example: 16 partition on 1TB and 8GB RAM.

// API is object of DB for use in project
var API db

type db struct {
	DBBlockSize       int
	WriteBuffer       int
	WriteCompact      uint64
	dbWriteCompact    map[int]uint64
	dbMapID           map[string]int
	dbMapName         map[int]string
	dbMap             map[string]*leveldb.DB
	dbCount           int
	dbNoSync          bool
	StoragePath       string
	statsOnStart      bool
	statsOnStartIndex bool
	deleteOnMigrate   bool
	compactOnStart    bool
}

// PartitionStats is ...
type PartitionStats struct {
	Name         string `xml:",attr"`
	OpenedTables string `xml:",attr"`
	CachedBlock  int    `xml:",attr"`
	Level        []PartitionStatsLevel
}

// PartitionStatsLevel is ...
type PartitionStatsLevel struct {
	Level  string `xml:",attr"`
	Tables string `xml:",attr"`
	Size   string `xml:",attr"`
	Time   string `xml:",attr"`
	Read   string `xml:",attr"`
	Write  string `xml:",attr"`
}

func init() {
	var err error

	API.DBBlockSize = 16 * opt.MiB
	API.dbCount = 16
	API.StoragePath = "storage"
	API.WriteBuffer = 2 * API.DBBlockSize
	API.WriteCompact = 100000 // peer partition

	if len(os.Getenv("ZERODB_LOADINDEX")) > 0 {
		if API.dbCount, err = strconv.Atoi(os.Getenv("ZERODB_LOADINDEX")); err != nil {
			log.Fatal(err)
		}
	}

	if len(os.Getenv("ZERODB_STORAGE")) > 0 {
		API.StoragePath = os.Getenv("ZERODB_STORAGE")
	}

	if len(os.Getenv("ZERODB_STATSONSTART")) > 0 {
		if API.statsOnStart, err = strconv.ParseBool(os.Getenv("ZERODB_STATSONSTART")); err != nil {
			log.Fatal(err)
		}
	}

	if len(os.Getenv("ZERODB_COMPACTNSTART")) > 0 {
		if API.compactOnStart, err = strconv.ParseBool(os.Getenv("ZERODB_COMPACTNSTART")); err != nil {
			log.Fatal(err)
		}
	}
	if len(os.Getenv("ZERODB_NOSYNC")) > 0 {
		if API.dbNoSync, err = strconv.ParseBool(os.Getenv("ZERODB_NOSYNC")); err != nil {
			log.Fatal(err)
		}
	}

	if len(os.Getenv("ZERODB_MIGRATE_DELETE")) > 0 {
		if API.deleteOnMigrate, err = strconv.ParseBool(os.Getenv("ZERODB_MIGRATE_DELETE")); err != nil {
			log.Fatal(err)
		}
	}

	if len(os.Getenv("ZERODB_STATSONSTART_INDEX")) > 0 {
		if API.statsOnStartIndex, err = strconv.ParseBool(os.Getenv("ZERODB_STATSONSTART_INDEX")); err != nil {
			log.Fatal(err)
		}
	}

	API.dbMapID = make(map[string]int)
	API.dbMapName = make(map[int]string)
	API.dbMap = make(map[string]*leveldb.DB)
	API.dbWriteCompact = make(map[int]uint64)
}

// OpenDB ...
func (d *db) OpenDB() {
	if d.dbCount < 1 {
		log.Fatal("Error count partition: ", d.dbCount)
	}
	var err error

	getDBName := func(dbcount, c int) string {
		tablePolynomial := crc32.MakeTable(crc32.IEEE)
		hash := crc32.Checksum([]byte(fmt.Sprintf("%d:%d", dbcount, c)), tablePolynomial)
		return fmt.Sprintf("%08x", hash)
	}

	for i := 0; i < d.dbCount; i++ {
		partition := getDBName(d.dbCount, i)

		d.dbMapID[partition] = i
		d.dbMapName[i] = partition
		d.dbWriteCompact[i] = 0

		log.Printf("Starting backend partition: %s, %d of %d", partition, i+1, d.dbCount)

		path := fmt.Sprintf("%s/%s", d.StoragePath, partition)

		dbopt := &opt.Options{
			Filter:                 filter.NewBloomFilter(2 << 3),
			OpenFilesCacheCapacity: 256,
			WriteBuffer:            d.WriteBuffer,
			BlockSize:              16 * opt.KiB,
			CompactionTableSize:    d.DBBlockSize,
			NoSync:                 d.dbNoSync,
		}

		if d.dbMap[partition], err = leveldb.OpenFile(path, dbopt); err != nil {
			log.Fatal(err)
		}
	}

	if len(d.dbMap) != d.dbCount || len(d.dbMapID) != d.dbCount || len(d.dbMapName) != d.dbCount {
		log.Fatalf(
			"Error validate started: dbMap(%d)==dbMapID(%d)==dbMapName(%d)==%d",
			len(d.dbMap), len(d.dbMapID), len(d.dbMapName), d.dbCount,
		)
	}

	d.MigrateOLD()

	if d.compactOnStart {
		d.DBCompact(4)
	}

	if d.statsOnStart {
		d.DBCounter()
	}
}

// CloseDB ...
func (d *db) CloseDB() {
	i := 0
	for name, db := range d.dbMap {
		i++
		log.Printf("Closing partition: %s, %d of %d\n", name, i, d.dbCount)
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

// MigrateOLD is migrate databases from old config to new
func (d *db) MigrateOLD() {
	// log.Println("Search old databases...")
	cacheDir := make(map[string]bool)

	visitPath := func(path string, f os.FileInfo, err error) error {
		if f.IsDir() && path != d.StoragePath {
			shortPath := strings.TrimPrefix(path, fmt.Sprintf("%s/", d.StoragePath))
			// partition not exists on runtime and skip done path
			if d.dbMapName[d.dbMapID[shortPath]] != shortPath {
				if cacheDir[shortPath] == true {
					return nil
				}
				if _, err := os.Stat(fmt.Sprintf("%s/CURRENT", path)); os.IsNotExist(err) {
					log.Printf("Detected trash path on DB path: %s\n", shortPath)
				} else {
					cacheDir[shortPath] = true
					log.Printf("Detected old database: %s Start migration...\n", shortPath)

					now := time.Now()

					opts := &opt.Options{
						ReadOnly:    !d.deleteOnMigrate,
						BlockCacher: opt.NoCacher,
					}
					db, err := leveldb.OpenFile(path, opts)
					if err != nil {
						log.Fatal(err)
						return nil
					}
					defer db.Close()

					iter, i := db.NewIterator(&util.Range{}, nil), 0
					for iter.Next() {
						i++
						if !iter.Valid() {
							log.Println("Found no valid on key", string(iter.Key()), iter.Key())
						} else {
							if err := d.Write(iter.Key(), iter.Value()); err != nil {
								log.Fatal(err)
							} else if d.deleteOnMigrate {
								db.Delete(iter.Key(), nil)
							}
						}
					}
					iter.Release()
					if err := iter.Error(); err != nil {
						log.Fatal(err)
					}

					log.Printf("End migration: %s Duration: %s Keys: %d\n", shortPath, time.Since(now), i)

					if err := os.RemoveAll(path); err != nil {
						log.Fatal(err)
					}
				}
			}
		}
		return nil
	}

	if err := filepath.Walk(d.StoragePath, visitPath); err != nil {
		log.Fatalf("filepath.Walk() returned %v\n", err)
	}
}

// DBCompact is compact database on runtime
func (d *db) DBCompact(cnt int) {
	for name, db := range d.dbMap {
		for i := 0; i < cnt; i++ {
			func(name string, db *leveldb.DB) {
				now := time.Now()
				log.Printf("Start DB Compact on partition: %s, %d of %d\n", name, i+1, cnt)
				db.CompactRange(util.Range{})
				log.Printf("END DB Compact on partition: %s Duration: %s\n", name, time.Since(now))
			}(name, db)
		}
	}
}

func (d *db) DBCompactID(id int) {
	name := d.dbMapName[id]
	now := time.Now()
	log.Printf("Start DB Auto Compact on partition: %s\n", name)
	d.dbMap[name].CompactRange(util.Range{})
	log.Printf("END DB Auto Compact on partition: %s Duration: %s\n", name, time.Since(now))
}

// DBCounter is count all keys in all databases
func (d *db) DBCounter() {
	total := 0
	var wg sync.WaitGroup
	wg.Add(d.dbCount)

	var indexFile *os.File

	if d.statsOnStartIndex {
		var err error
		indexFile, err = os.Create("index.txt")
		if err != nil {
			log.Fatal(err)
		}
		defer indexFile.Close()
	}

	for name, db := range d.dbMap {
		go func(name string, db *leveldb.DB, wg *sync.WaitGroup) {
			defer wg.Done()
			now := time.Now()
			log.Println("Start counter on partition:", name)
			iter, i := db.NewIterator(nil, nil), 0
			for iter.Next() {
				i++
				if d.statsOnStartIndex {
					io.WriteString(indexFile, fmt.Sprintf("%s\t%s\n", name, string(iter.Key())))
				}
			}
			defer iter.Release()
			total = total + i
			log.Printf("Found %d keys on partition: %s Duration: %s\n", i, name, time.Since(now))
		}(name, db, &wg)
	}

	wg.Wait()
	log.Printf("Found total %d keys on all partition\n", total)
}

func (d *db) Background() {
	forever := make(chan bool, 1)
	go func() {
		for {
			<-time.After(time.Minute)
			for id, count := range d.dbWriteCompact {
				if count > d.WriteCompact {
					d.dbWriteCompact[id] = 0
					d.DBCompactID(id)
				}
			}
		}
	}()
	<-forever
}

func (d *db) Stats() (stats []PartitionStats) {
	var derr error
	for name, db := range d.dbMap {
		p := PartitionStats{Name: name}

		p.OpenedTables, derr = db.GetProperty("leveldb.openedtables")
		if derr != nil {
			log.Panic(derr)
		}

		cachedata, err := db.GetProperty("leveldb.cachedblock")
		if err != nil {
			log.Panic(err)
		}

		p.CachedBlock, derr = strconv.Atoi(cachedata)
		if derr != nil {
			log.Panic(derr)
		}

		p.CachedBlock = p.CachedBlock / opt.MiB

		data, err := db.GetProperty("leveldb.stats")
		if err != nil {
			log.Panic(err)
		}

		for _, line := range strings.Split(data, "\n") {
			if strings.Contains(line, "Level") || strings.Contains(line, "-------") || strings.Contains(line, "Compactions") {
				continue
			}
			sline := strings.Split(strings.TrimSpace(line), "|")
			if len(sline) != 6 {
				continue
			}
			p.Level = append(p.Level, PartitionStatsLevel{
				Level:  strings.TrimSpace(sline[0]),
				Tables: strings.TrimSpace(sline[1]),
				Size:   strings.TrimSpace(sline[2]),
				Time:   strings.TrimSpace(sline[3]),
				Read:   strings.TrimSpace(sline[4]),
				Write:  strings.TrimSpace(sline[5]),
			})
		}
		stats = append(stats, p)
	}
	return
}

func (d *db) GetDBCount() int {
	return d.dbCount
}

func (d *db) GetPartName(key []byte) string {
	hash := 0
	if len(d.dbMap) > 1 {
		hash = int(crc16.Crc16(key)) % d.dbCount
	}
	return d.dbMapName[hash]
}

func (d *db) getPartID(key []byte) (hash int) {
	hash = 0
	if len(d.dbMap) > 1 {
		hash = int(crc16.Crc16(key)) % d.dbCount
	}
	return
}

// Read ...
func (d *db) Read(key []byte) ([]byte, error) {
	return d.dbMap[d.GetPartName(key)].Get(key, nil)
}

// Write ...
func (d *db) Write(key, value []byte) error {
	d.dbWriteCompact[d.getPartID(key)]++
	return d.dbMap[d.GetPartName(key)].Put(key, value, nil)
}

// Delete ...
func (d *db) Delete(key []byte) error {
	d.dbWriteCompact[d.getPartID(key)]++
	return d.dbMap[d.GetPartName(key)].Delete(key, nil)
}

// Exists is check if exists key
func (d *db) Exists(key []byte) (bool, error) {
	return d.dbMap[d.GetPartName(key)].Has(key, nil)
}
