package frontend

import (
	"fmt"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func btostr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
