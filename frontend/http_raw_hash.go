package frontend

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/vitalvas/zerodb/db"

	"github.com/gorilla/mux"
)

func init() {
	HTTPRouter.HandleFunc("/rawhash/{rawkey}", httpPutHashByKey).Methods("PUT")
}

func httpPutHashByKey(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, int64(db.API.DBBlockSize))

	if data, err := ioutil.ReadAll(r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		var ext string
		fileName := strings.Split(strings.ToLower(mux.Vars(r)["rawkey"]), ".")
		if len(fileName) > 1 {
			if len(fileName) > 2 {
				ext = fmt.Sprintf(".%s.%s", fileName[len(fileName)-2], fileName[len(fileName)-1])
			} else {
				ext = fmt.Sprintf(".%s", fileName[len(fileName)-1])
			}
		}
		hash := sha1.New()
		hash.Write(data)
		key := fmt.Sprintf("%x%s", hash.Sum(nil), ext)
		if err := db.API.Write([]byte(key), data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.Header().Add("X-Partition-Name", db.API.GetPartName([]byte(key)))
		http.Redirect(w, r, fmt.Sprintf("/raw/%s", key), http.StatusTemporaryRedirect)
	}

}
