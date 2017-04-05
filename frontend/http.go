package frontend

import (
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
)

// HTTPRouter is url router for http frontend
var HTTPRouter = mux.NewRouter().SkipClean(true)

func httpService(wg *sync.WaitGroup) {
	defer wg.Done()

	httpBind := "127.0.0.1:8080"
	if len(os.Getenv("ZERODB_HTTP_BIND")) > 2 {
		httpBind = os.Getenv("ZERODB_HTTP_BIND")
	}

	if httpBind == "0.0.0.0:0" {
		return
	}

	server := &http.Server{
		Addr:    httpBind,
		Handler: HTTPRouter,
	}

	log.Println("HTTP Listen on", server.Addr)

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
