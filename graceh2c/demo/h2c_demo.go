package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bookingcom/grace/graceh2c"
	"golang.org/x/net/http2"
)

type response struct {
	Pid   int
	Sleep string
	Error string `json:",omitempty"`
}

func sendRequest(url string) error {
	client := http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, cfg *tls.Config) (conn net.Conn, e error) {
				return net.Dial(network, addr)
			},
		},
	}
	res, err := client.Get(url)
	if err != nil {
		return err
	}
	var r response
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("error decoding json: %s", err)
	}
	if r.Pid == os.Getpid() {
		log.Printf("response -> OK from PID:%d after sleeping %s", r.Pid, r.Sleep)
	}
	res.Body.Close()
	return nil
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/sleep/", func(w http.ResponseWriter, r *http.Request) {
		duration, err := time.ParseDuration(r.FormValue("duration"))
		if err != nil {
			http.Error(w, err.Error(), 400)
		}
		time.Sleep(duration) // sleep some
		json, err := json.Marshal(&response{
			Pid:   os.Getpid(),
			Sleep: r.FormValue("duration"),
		})
		if err != nil {
			log.Fatalf("error encoding json: %s", err)
		}
		w.Header().Set("Content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(json)
	})

	s := graceh2c.NewH2CServer(
		&http.Server{
			Addr:    "localhost:8000",
			Handler: mux,
		},
		graceh2c.Logger(log.New(os.Stderr, "[h2c] ", log.LstdFlags)),
		graceh2c.ContextTimeout(31*time.Second),
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.After(10 * time.Second):
				log.Printf("starting to shutdown server now")
				if err := s.Stop(false); err != nil {
					log.Printf("error closing the server: %s", err)
				}
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Server().ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("error while stopping server: %s", err)
		}
		return
	}()

	count := 0
	serverStarted := make(chan bool, 1)
	go func() {
		for {
			url := fmt.Sprintf("http://127.0.0.1:8000/sleep/?duration=0")
			if err := sendRequest(url); err != nil {
				if strings.HasSuffix(err.Error(), "connection refused") {
					continue // keep trying while the server boots up
				}
				log.Printf("request during normal operation: %s", err)
			}
			count++
			if count > 10 {
				serverStarted <- true
				return
			}
		}
	}()
	<-serverStarted // wait for the server to boot and answer 10 queries successfully

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	for i := 1; i <= 10; i++ {
		go func(counter int) {
			url := fmt.Sprintf("http://127.0.0.1:8000/sleep/?duration=%ds", r1.Intn(30))
			log.Printf("sending request: %s", url)
			if err := sendRequest(url); err != nil {
				log.Printf("request during normal operation: %s", err)
			}
		}(i)
	}
	wg.Wait()
	log.Printf("demo ends")
}
