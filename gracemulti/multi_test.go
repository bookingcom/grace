package gracemulti_test

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/facebookarchive/freeport"
	"golang.org/x/net/http2"
)

var debugLog = flag.Bool("debug", false, "enable debug logging")

func debug(format string, a ...interface{}) {
	if *debugLog {
		println(fmt.Sprintf(format, a...))
	}
}

type harness struct {
	T                *testing.T
	h2cAddr          string
	Process          []*os.Process
	ProcessMutex     sync.Mutex
	RequestWaitGroup sync.WaitGroup
	newProcess       chan bool
	requestCount     int
	requestCountMux  sync.Mutex
	serveOption      int
}

type response struct {
	Sleep time.Duration
	Pid   int
	Error string `json:",omitempty"`
}

func newHarness(t *testing.T) *harness {
	return &harness{
		T:           t,
		newProcess:  make(chan bool),
		serveOption: -1,
	}
}

func (h *harness) setupAddr() {
	port, err := freeport.Get()
	if err != nil {
		h.T.Fatalf("failed to find a free port: %s", err)
	}
	h.h2cAddr = fmt.Sprintf("127.0.0.1:%d", port)
	debug("Setting up server with address: %s\n", h.h2cAddr)
}

func (h *harness) Start() {
	h.setupAddr()
	cmd := exec.Command(os.Args[0], "-h2c", h.h2cAddr)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		h.T.Fatal(err)
	}
	go func() {
		reader := bufio.NewReader(stderr)
		for {
			line, isPrefix, err := reader.ReadLine()
			if err == io.EOF {
				return
			}
			if err != nil {
				println(fmt.Sprintf("failed to read line from server process: %s", err))
			}
			if isPrefix {
				println(fmt.Sprintf("deal with isPrefix for line: %s", line))
			}
			res := &response{}
			err = json.Unmarshal(line, res)
			if err != nil {
				println(fmt.Sprintf("couldn't parse json from stderr: %s: %s", line, err))
			}
			if res.Error != "" {
				println(fmt.Sprintf("got error from process: %v", res))
			}
			process, err := os.FindProcess(res.Pid)
			if err != nil {
				println(fmt.Sprintf("couldn't find process with pid: %d", res.Pid))
			}
			h.ProcessMutex.Lock()
			h.Process = append(h.Process, process)
			h.ProcessMutex.Unlock()
			h.newProcess <- true
		}
	}()
	if err := cmd.Start(); err != nil {
		h.T.Fatalf("failed to start command: %s", err)
	}
	<-h.newProcess
}

// get global request count and increment it
func (h *harness) RequestCount() int {
	h.requestCountMux.Lock()
	defer h.requestCountMux.Unlock()
	c := h.requestCount
	h.requestCount++
	return c
}

func (h *harness) MostRecentProcess() *os.Process {
	h.ProcessMutex.Lock()
	defer h.ProcessMutex.Unlock()
	l := len(h.Process)
	if l == 0 {
		h.T.Fatalf("don't known any processes yet")
	}
	return h.Process[l-1]
}

func (h *harness) SendSingleRequest(dialGroup *sync.WaitGroup, url string, pid int) {
	defer h.RequestWaitGroup.Done()
	count := h.RequestCount()
	debug("send %02d pid=%d url=%s", count, pid, url)
	client := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				defer func() {
					time.Sleep(50 * time.Millisecond)
					dialGroup.Done()
				}()
				return net.Dial(network, addr)
			},
		},
	}
	r, err := client.Get(url)
	if err != nil {
		h.T.Fatalf("failed request %02d to %s pid=%d: %s", count, url, pid, err)
	}
	debug("body %02d pid=%d url=%s", count, pid, url)
	defer r.Body.Close()
	res := &response{}
	err = json.NewDecoder(r.Body).Decode(res)
	if err != nil {
		h.T.Fatalf("failed to ready decode json response body pid=%d, %s", pid, err)
	}
	if pid != res.Pid {
		for _, old := range h.Process[0 : len(h.Process)-1] {
			if res.Pid == old.Pid {
				h.T.Logf("found old pid %d, ignoring this discrepancy", res.Pid)
				return
			}
			h.T.Fatalf("didn't get expected pid %d, instead got %d", pid, res.Pid)
		}
	}
	debug("done %02d pid=%d url=%s", count, pid, url)
}

func (h *harness) SendRequest() {
	pid := h.MostRecentProcess().Pid

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	nrRequests := 10
	h.RequestWaitGroup.Add(nrRequests)
	var dialGroup sync.WaitGroup
	for i := 0; i < nrRequests; i++ {
		url := fmt.Sprintf("http://%s/sleep/?duration=%ds",
			h.h2cAddr,
			r1.Intn(10),
		)
		dialGroup.Add(1)
		go h.SendSingleRequest(&dialGroup, url, pid)
	}

	debug("added requests pid=%d", pid)
	dialGroup.Wait()
	debug("dialed requests pid=%d", pid)
}

func (h *harness) Restart() {
	err := h.MostRecentProcess().Signal(syscall.SIGUSR2)
	if err != nil {
		h.T.Fatalf("failed to send SIGUSR2 and restart process: %s", err)
	}
	<-h.newProcess
}

func (h *harness) Stop() {
	err := h.MostRecentProcess().Signal(syscall.SIGTERM)
	if err != nil {
		h.T.Fatalf("failed to send SIGTERM and stop process: %s", err)
	}
}

func (h *harness) Wait() {
	h.RequestWaitGroup.Wait()
}

func TestFirst(t *testing.T) {
	t.Parallel()
	debug("started TestFirst")
	h := newHarness(t)
	debug("initial start")
	h.Start()
	debug("send request 1")
	h.SendRequest()
	debug("restart 1")
	h.Restart()
	debug("send request 2")
	h.SendRequest()
	debug("restart 2")
	h.Restart()
	debug("send request 3")
	h.SendRequest()
	debug("stopping")
	h.Stop()
	debug("waiting")
	h.Wait()
}

func TestFirstAgain(t *testing.T) {
	t.Parallel()
	debug("started TestFirstAgain")
	h := newHarness(t)
	debug("initial start")
	h.Start()
	debug("send request 1")
	h.SendRequest()
	debug("restart 1")
	h.Restart()
	debug("send request 2")
	h.SendRequest()
	debug("restart 2")
	h.Restart()
	debug("send request 3")
	h.SendRequest()
	debug("stopping")
	h.Stop()
	debug("waiting")
	h.Wait()
}
