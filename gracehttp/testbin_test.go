package gracehttp_test

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bookingcom/grace/gracehttp"
)

const preStartProcessEnv = "GRACEHTTP_PRE_START_PROCESS"

func TestMain(m *testing.M) {
	const (
		testbinKey   = "GRACEHTTP_TEST_BIN"
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

// Wait for 10 consecutive responses from our own pid.
//
// This prevents flaky tests that arise from the fact that we have the
// perfectly acceptable (read: not a bug) condition where both the new and the
// old servers are accepting requests. In fact the amount of time both are
// accepting at the same time and the number of requests that flip flop between
// them is unbounded and in the hands of the various kernels our code tends to
// run on.
//
// In order to combat this, we wait for 10 successful responses from our own
// pid. This is a somewhat reliable way to ensure the old server isn't
// serving anymore.
func wait(wg *sync.WaitGroup, url string) {
	var success int
	defer wg.Done()
	for {
		res, err := http.Get(url)
		if err == nil {
			// ensure it isn't a response from a previous instance
			defer res.Body.Close()
			var r response
			if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
				log.Fatalf("Error decoding json: %s", err)
			}
			if r.Pid == os.Getpid() {
				success++
				if success == 10 {
					return
				}
				continue
			}
		} else {
			success = 0
			// we expect connection refused
			if !strings.HasSuffix(err.Error(), "connection refused") {
				e2 := json.NewEncoder(os.Stderr).Encode(&response{
					Error: err.Error(),
					Pid:   os.Getpid(),
				})
				if e2 != nil {
					log.Fatalf("Error writing error json: %s", e2)
				}
			}
		}
	}
}

func httpsServer(addr string) *http.Server {
	cert, err := tls.X509KeyPair(localhostCert, localhostKey)
	if err != nil {
		log.Fatalf("error loading cert: %v", err)
	}
	return &http.Server{
		Addr:    addr,
		Handler: newHandler(),
		TLSConfig: &tls.Config{
			NextProtos:   []string{"http/1.1"},
			Certificates: []tls.Certificate{cert},
		},
	}
}

var fh1 *net.UDPConn

func testbinMain() {
	out, err := exec.Command("lsof", "-p", fmt.Sprintf("%d", os.Getpid())).Output()
	fmt.Println(string(out), err)

	var httpAddr, httpsAddr string
	var testOption int
	flag.StringVar(&httpAddr, "http", ":48560", "http address to bind to")
	flag.StringVar(&httpsAddr, "https", ":48561", "https address to bind to")
	flag.IntVar(&testOption, "testOption", -1, "which option to test on ServeWithOptions")
	flag.Parse()

	// we have self signed certs
	http.DefaultTransport = &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	// print json to stderr once we can successfully connect to all three
	// addresses. the ensures we only print the line once we're ready to serve.
	go func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go wait(&wg, fmt.Sprintf("http://%s/sleep/?duration=1ms", httpAddr))
		go wait(&wg, fmt.Sprintf("https://%s/sleep/?duration=1ms", httpsAddr))
		wg.Wait()

		err := json.NewEncoder(os.Stderr).Encode(&response{Pid: os.Getpid()})
		if err != nil {
			log.Fatalf("Error writing startup json: %s", err)
		}
	}()

	fh1, err = net.DialUDP("udp", nil, &net.UDPAddr{Port: 7777})
	fmt.Println("GOT", fh1, err)

	servers := []*http.Server{
		&http.Server{Addr: httpAddr, Handler: newHandler()},
		httpsServer(httpsAddr),
	}

	if testOption == -1 {
		err := gracehttp.Serve(servers...)
		if err != nil {
			log.Fatalf("Error in gracehttp.Serve: %s", err)
		}
	} else {
		if testOption == testPreStartProcess {
			switch os.Getenv(preStartProcessEnv) {
			case "":
				err := os.Setenv(preStartProcessEnv, "READY")
				if err != nil {
					log.Fatalf("testbin (first incarnation) could not set %v to 'ready': %v", preStartProcessEnv, err)
				}
			case "FIRED":
				// all good, reset for next round
				err := os.Setenv(preStartProcessEnv, "READY")
				if err != nil {
					log.Fatalf("testbin (second incarnation) could not reset %v to 'ready': %v", preStartProcessEnv, err)
				}
			case "READY":
				log.Fatalf("failure to update startup hook before new process started")
			default:
				log.Fatalf("something strange happened with %v: it ended up as %v, which is not '', 'FIRED', or 'READY'", preStartProcessEnv, os.Getenv(preStartProcessEnv))
			}

			err := gracehttp.ServeWithOptions(
				servers,
				gracehttp.PreStartProcess(func() error {
					err := os.Setenv(preStartProcessEnv, "FIRED")
					if err != nil {
						log.Fatalf("startup hook could not set %v to 'fired': %v", preStartProcessEnv, err)
					}
					return nil
				}),
			)
			if err != nil {
				log.Fatalf("Error in gracehttp.Serve: %s", err)
			}
		}
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
		err = json.NewEncoder(w).Encode(&response{
			Sleep: duration,
			Pid:   os.Getpid(),
		})
		if err != nil {
			log.Fatalf("Error encoding json: %s", err)
		}
	})
	return mux
}

// localhostCert is a PEM-encoded TLS cert with SAN IPs
// "127.0.0.1" and "[::1]", expiring at the last second of 2049 (the end
// of ASN.1 time).
// generated from src/pkg/crypto/tls:
// go run generate_cert.go  --rsa-bits 512 --host 127.0.0.1,::1,example.com --ca --start-date "Jan 1 00:00:00 1970" --duration=1000000h
var localhostCert = []byte(`-----BEGIN CERTIFICATE-----
MIIBjzCCATmgAwIBAgIRAIQqL/JUYTOx1yWeFAmn33owDQYJKoZIhvcNAQELBQAw
EjEQMA4GA1UEChMHQWNtZSBDbzAgFw03MDAxMDEwMDAwMDBaGA8yMDg0MDEyOTE2
MDAwMFowEjEQMA4GA1UEChMHQWNtZSBDbzBcMA0GCSqGSIb3DQEBAQUAA0sAMEgC
QQDKBxsqlcBmhxTKhc8YAWC7qVeGtwfx//hTERsvhCI9lf+IzOD6zoTfblnOtuwr
WmeLYNhBMTEDq21nqtDqbwrNAgMBAAGjaDBmMA4GA1UdDwEB/wQEAwICpDATBgNV
HSUEDDAKBggrBgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MC4GA1UdEQQnMCWCC2V4
YW1wbGUuY29thwR/AAABhxAAAAAAAAAAAAAAAAAAAAABMA0GCSqGSIb3DQEBCwUA
A0EALBvsKv7jdYqGSNwDmbhsJXIaz6/M5crGuf4g/BykLGPdCLvETzaLXCUeXxRK
cgLn+nAB1AgkkttIYH4MJDhfVw==
-----END CERTIFICATE-----`)

// localhostKey is the private key for localhostCert.
var localhostKey = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIBOQIBAAJBAMoHGyqVwGaHFMqFzxgBYLupV4a3B/H/+FMRGy+EIj2V/4jM4PrO
hN9uWc627CtaZ4tg2EExMQOrbWeq0OpvCs0CAwEAAQJAGZISry4rHw8D46pSDwDF
pJsHeAacm9XBMYpdvYLk7pRe9UmMvQmdoHFVL8Eogg1XAwiHibfgs8sPp1bu7NXS
oQIhANHhqxYfdShPITE+0CJoSebfnwTQ9+dXKDYd8bgz2iB1AiEA9mulM61C36H0
qGPCHbM0w3HX58y0SmTT2DCdOFKwdfkCIGJR0iDZ+bs5XnZAU6ZarowOI+NQtWFV
TgwT+QFuoPdlAiAt5v9vEOdUISch9vNB9Q/vGFXPqCIteXq82tFunHiigQIgSkta
C5rVfhscaE8mnHkGMmLEEBY3d6CDugYxZQcscOI=
-----END RSA PRIVATE KEY-----`)
