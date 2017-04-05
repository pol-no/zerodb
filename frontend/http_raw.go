package frontend

import (
	"io/ioutil"
	"net/http"

	"github.com/vitalvas/zerodb/db"

	"github.com/gorilla/mux"
)

func init() {
	HTTPRouter.HandleFunc("/raw/{rawkey}", httpGetRawByKey).Methods("GET", "HEAD")
	HTTPRouter.HandleFunc("/raw/{rawkey}", httpPutRawByKey).Methods("PUT")
	HTTPRouter.HandleFunc("/raw/{rawkey}", httpDelRawByKey).Methods("DELETE")
}

func httpGetRawByKey(w http.ResponseWriter, r *http.Request) {
	key := []byte(mux.Vars(r)["rawkey"])

	w.Header().Add("X-Partition-Name", db.API.GetPartName(key))

	if exists, err := db.API.Exists(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if exists {
		if rawdata, err := db.API.Read(key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			if _, err := w.Write(rawdata); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	} else {
		http.Error(w, "404 page not found", http.StatusNotFound)
	}
}

func httpPutRawByKey(w http.ResponseWriter, r *http.Request) {
	key := []byte(mux.Vars(r)["rawkey"])

	r.Body = http.MaxBytesReader(w, r.Body, int64(db.API.DBBlockSize))

	w.Header().Add("X-Partition-Name", db.API.GetPartName(key))

	if data, err := ioutil.ReadAll(r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		if err := db.API.Write(key, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func httpDelRawByKey(w http.ResponseWriter, r *http.Request) {
	key := []byte(mux.Vars(r)["rawkey"])

	if err := db.API.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
