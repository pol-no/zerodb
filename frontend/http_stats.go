package frontend

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/vitalvas/zerodb/db"

	"github.com/gorilla/mux"
)

var startTime = time.Now()

func init() {
	HTTPRouter.HandleFunc("/.stats.{type}", httpGetStats).Methods("GET", "HEAD")
}

type httpSysStats struct {
	XMLName      xml.Name `json:"-" xml:"SysStats"`
	Uptime       int64
	NumGoroutine int

	// General statistics.
	MemAllocated uint64
	MemTotal     uint64
	MemSys       uint64
	Lookups      uint64
	MemMallocs   uint64
	MemFrees     uint64

	// Main allocation heap statistics.
	HeapAlloc    uint64
	HeapSys      uint64
	HeapIdle     uint64
	HeapInuse    uint64
	HeapReleased uint64
	HeapObjects  uint64

	// Low-level fixed-size structure allocator statistics.
	StackInuse  uint64
	StackSys    uint64
	MSpanInuse  uint64
	MSpanSys    uint64
	MCacheInuse uint64
	MCacheSys   uint64
	BuckHashSys uint64
	GCSys       uint64
	OtherSys    uint64

	// Garbage collector statistics.
	NextGC       uint64
	LastGC       uint64
	PauseTotalNs uint64
	PauseNs      string
	NumGC        uint32

	Partitions []db.PartitionStats `xml:"Partitions>Partition"`
}

func httpGetStats(w http.ResponseWriter, r *http.Request) {
	respType := mux.Vars(r)["type"]

	m := new(runtime.MemStats)
	runtime.ReadMemStats(m)

	data := &httpSysStats{
		Uptime:       int64(time.Since(startTime).Seconds()),
		NumGoroutine: runtime.NumGoroutine(),

		MemAllocated: m.Alloc,
		MemTotal:     m.TotalAlloc,
		MemSys:       m.Sys,
		Lookups:      m.Lookups,
		MemMallocs:   m.Mallocs,
		MemFrees:     m.Frees,

		HeapAlloc:    m.HeapAlloc,
		HeapSys:      m.HeapSys,
		HeapIdle:     m.HeapIdle,
		HeapInuse:    m.HeapInuse,
		HeapReleased: m.HeapReleased,
		HeapObjects:  m.HeapObjects,

		StackInuse:  m.StackInuse,
		StackSys:    m.StackSys,
		MSpanInuse:  m.MSpanInuse,
		MSpanSys:    m.MSpanSys,
		MCacheInuse: m.MCacheInuse,
		MCacheSys:   m.MCacheSys,
		BuckHashSys: m.BuckHashSys,
		GCSys:       m.GCSys,
		OtherSys:    m.OtherSys,

		NextGC:       m.NextGC,
		LastGC:       uint64(time.Now().UnixNano()-int64(m.LastGC)) / 1000 / 1000 / 1000,
		PauseTotalNs: uint64(m.PauseTotalNs) / 1000 / 1000 / 1000,
		PauseNs:      fmt.Sprintf("%.4f", float64(m.PauseNs[(m.NumGC+255)%256])/1000/1000/1000),
		NumGC:        m.NumGC,

		Partitions: db.API.Stats(),
	}

	switch respType {
	case "json":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)

	case "xml":
		output, _ := xml.MarshalIndent(data, "", " ")
		w.Write([]byte(xml.Header))
		w.Write(output)
	}
}
