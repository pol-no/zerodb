package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/vitalvas/zerodb/db"
	"github.com/vitalvas/zerodb/frontend"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetOutput(os.Stdout)
}

func main() {
	var globalWait sync.WaitGroup
	log.Printf("Starting database. Process PID: %v\n", os.Getpid())

	db.API.OpenDB()
	go db.API.Background()
	defer db.API.CloseDB()

	go func() {
		sigs := make(chan os.Signal)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGTSTP, syscall.SIGUSR1, syscall.SIGUSR2)

		go func() {
			for {
				sig := <-sigs
				log.Println("Received signal:", sig)
				switch sig {
				case syscall.SIGTERM, syscall.SIGINT, syscall.SIGTSTP:
					db.API.CloseDB()
					os.Exit(0)
				case syscall.SIGUSR1:
					db.API.DBCompact(1)
				case syscall.SIGUSR2:
					// 32 passes for maximum packaging
					db.API.DBCompact(32)
				default:
					log.Println("Unknown signal. Skipping.")
				}
			}
		}()
	}()

	front := frontend.Front{Wait: &globalWait}
	front.Run()

	globalWait.Wait()
}
