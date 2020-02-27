package gracemulti

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bookingcom/grace/graceh2c"
	"gitlab.booking.com/go/event-proxy/receiver"
	"golang.org/x/net/http2"
)

func TestMain(m *testing.M) {
	const (
		testbinKey   = "GRACEMULTI_TEST_BIN"
		testbinValue = "1"
	)
	if os.Getenv(testbinKey) == testbinValue {
		testbinMain()
		return
	}
	if err := os.Setenv(testbinKey, testbinValue); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

type response struct {
	Sleep time.Duration
	Pid   int
	Error string `json:",omitempty"`
}

var fh1 *net.UDPConn
var err error

func testbinMain() {
	var (
		h2cAddr    string
		udpAddr    string
		testOption int
	)
	flag.StringVar(&h2cAddr, "h2c", "localhost:48560", "h2c address")
	flag.StringVar(&udpAddr, "udp", "localhost:48561", "udp address")
	flag.IntVar(&testOption, "testOption", -1, "which option to test")
	flag.Parse()

	go func() {
		var wg sync.WaitGroup
		wg.Add(1)
		go wait(&wg, fmt.Sprintf("http://%s/sleep/?duration=1ms", h2cAddr))
		wg.Wait()

		if err := json.NewEncoder(os.Stderr).Encode(&response{Pid: os.Getpid()}); err != nil {
			log.Fatalf("error writing startup json: %s", err)
		}
	}()

	fh1, err = net.DialUDP("udp", nil, &net.UDPAddr{Port: 7777})
	fmt.Println("GOT", fh1, err)

	// debug
	tmpFile, err := ioutil.TempFile(os.TempDir(), "gracemulti-")
	if err != nil {
		panic(err)
	}

	SetLogger(log.New(tmpFile, "[gracemulti] ", log.LstdFlags))

	s := graceh2c.NewH2CServer(&http.Server{
		Addr:    h2cAddr,
		Handler: newHandler(),
	}, graceh2c.Logger(log.New(tmpFile, "[h2c] ", log.LstdFlags)))
	servers := MultiServer{
		H2C: []*graceh2c.H2CServer{s},
		UDP: []*UdpServer{{
			Addr:    udpAddr,
			Network: "udp4",
			Threads: 1,
			Handler: receiver.DatagramHandler,
		}},
	}
	if err := Serve(servers); err != nil {
		panic(err)
	}
}

func newHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/sleep/", func(w http.ResponseWriter, r *http.Request) {
		duration, err := time.ParseDuration(r.FormValue("duration"))
		if err != nil {
			http.Error(w, err.Error(), 400)
		}
		time.Sleep(duration)
		if err := json.NewEncoder(w).Encode(&response{
			Sleep: duration,
			Pid:   os.Getpid(),
		}); err != nil {
			log.Fatalf("error encoding json: %s", err)
		}
	})
	return mux
}

func wait(wg *sync.WaitGroup, url string) {
	var success int
	defer wg.Done()
	for {
		client := http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network, addr string, cfg *tls.Config) (conn net.Conn, e error) {
					return net.Dial(network, addr)
				},
			},
		}
		res, err := client.Get(url)
		if err == nil {
			var r response
			if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
				log.Fatalf("error decoding json: %s", err)
			}
			if r.Pid == os.Getpid() {
				success++
				if success == 10 {
					return
				}
				continue
			}
			res.Body.Close()
		} else {
			success = 0
			if !strings.HasSuffix(err.Error(), "connection refused") {
				if e2 := json.NewEncoder(os.Stderr).Encode(&response{
					Error: err.Error(),
					Pid:   os.Getpid(),
				}); e2 != nil {
					log.Fatalf("error writing error json: %s", e2)
				}
			}
		}
	}
}
