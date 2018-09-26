// Package gracemulti provides easy to use graceful restart
// functionality for HTTP & UDP servers.
package gracemulti

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/facebookgo/httpdown"
	"github.com/bookingcom/grace/gracenet"
	"github.com/bookingcom/grace/gracenetpacket"
	"os/exec"
	"strings"
	"time"
)

const (
	// Used to indicate a graceful restart in the new process.
	envCountListners    = "LISTEN_FDS"
	envCountConnections = "CONN_FDS"

	// default timeout is 5 seconds
	defaultTimeout = 5 * time.Second
)

// In order to keep the working directory the same as when we started we record it at startup.
var originalWD, _ = os.Getwd()

type option func(*app)

// timeout error
var ErrTimeout = errors.New("shutdown wait timed out")

var (
	logger     *log.Logger
	didInherit = (os.Getenv(envCountListners) != "") || (os.Getenv(envCountConnections) != "")
	ppid       = os.Getppid()
)

type UdpServer struct {
	Addr    string
	Network string
	Threads int
	Data    interface{}
	Handler func(*net.UDPConn, interface{})
}

type MultiServer struct {
	HTTP []*http.Server
	UDP  []*UdpServer
}

// An app contains one or more servers and associated configuration.
type app struct {
	servers         MultiServer
	http            *httpdown.HTTP
	net             *gracenet.Net
	packetnet       *gracenetpacket.Net
	listeners       []net.Listener
	connections     []*net.UDPConn
	sds             []httpdown.Server
	errors          chan error
	preStartProcess func() error
	preKillProcess  func() error
}

func newApp(servers MultiServer) *app {
	len_http := len(servers.HTTP)
	len_udp := len(servers.UDP)

	return &app{
		servers:         servers,
		http:            &httpdown.HTTP{},
		net:             &gracenet.Net{},
		packetnet:       &gracenetpacket.Net{},
		listeners:       make([]net.Listener, 0, len_http),
		connections:     make([]*net.UDPConn, 0, len_udp),
		sds:             make([]httpdown.Server, 0, len_http),
		errors:          make(chan error, 1+(len_http+len_udp)*2),
		preStartProcess: func() error { return nil },
		preKillProcess:  func() error { return nil },
	}
}

func (a *app) listen() error {
	for _, s := range a.servers.HTTP {
		l, err := a.net.Listen("tcp", s.Addr)
		if err != nil {
			return err
		}
		a.listeners = append(a.listeners, l)
	}
	a.packetnet.SetFdStart(len(a.listeners) + 3)
	for _, s := range a.servers.UDP {
		l, err := a.packetnet.ListenPacket(s.Network, s.Addr)
		if err != nil {
			return err
		}
		a.connections = append(a.connections, l.(*net.UDPConn))
	}
	return nil
}

func (a *app) serveHttp(wg *sync.WaitGroup) {
	for i, s := range a.servers.HTTP {
		a.sds = append(a.sds, a.http.Serve(s, a.listeners[i]))
	}

	for _, s := range a.sds {
		go func(s httpdown.Server) {
			defer wg.Done()
			if err := s.Wait(); err != nil {
				a.errors <- err
			}
		}(s)
	}
}

func (a *app) serveUdp(wg *sync.WaitGroup) {
	for i, s := range a.servers.UDP {
		if s.Threads == 0 {
			s.Threads++
		}
		for t := 0; t < s.Threads; t++ {
			go func(s_index int, t_index int, server *UdpServer) {
				defer wg.Done()
				wg.Add(1) // Wait for the thread
				server.Handler(a.connections[s_index], server.Data)
			}(i, t, s)
		}
	}
}

func (a *app) wait() {
	var wg sync.WaitGroup
	go a.signalHandler(&wg)
	wg.Add(2*len(a.listeners) + len(a.connections)) // Wait (http & udp) & Stop (http)
	a.serveHttp(&wg)
	a.serveUdp(&wg)
	wg.Wait()
}

func (a *app) term(wg *sync.WaitGroup) {
	for _, s := range a.sds {
		go func(s httpdown.Server) {
			defer wg.Done()
			if err := s.Stop(); err != nil {
				a.errors <- err
			}
		}(s)
	}
	for _, c := range a.connections {
		go func(c *net.UDPConn) {
			defer wg.Done()
			if err := c.Close(); err != nil {
				a.errors <- err
			}
		}(c)
	}
}

func (a *app) signalHandler(wg *sync.WaitGroup) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			// this ensures a subsequent INT/TERM will trigger standard go behaviour of terminating.
			signal.Stop(ch)
			// this ensures process terminates after a timeout
			go func() {
				time.Sleep(defaultTimeout)
				a.errors <- ErrTimeout
			}()
			a.term(wg)
			return
		case syscall.SIGUSR2:
			if err := a.preStartProcess(); err != nil {
				a.errors <- err
			}
			// we only return here if there's an error, otherwise the new process
			// will send us a TERM when it's ready to trigger the actual shutdown.
			if _, err := a.StartProcess(); err != nil {
				a.errors <- err
			}
		}
	}
}

func (a *app) StartProcess() (int, error) {
	listeners, err := a.net.GetActiveListeners()
	if err != nil {
		return 0, err
	}

	connections, err := a.packetnet.GetActiveListeners()
	if err != nil {
		return 0, err
	}

	// Extract the fds from the listeners.
	files := make([]*os.File, len(listeners)+len(connections))
	var fdl, fdc []string

	i := 0
	for _, l := range listeners {
		files[i], err = l.(filer).File()
		if err != nil {
			return 0, err
		}
		fd, _ := syscall.Dup(int(files[i].Fd()))
		fdl = append(fdl, fmt.Sprint(fd))
		defer files[i].Close()
		i++
	}

	for _, l := range connections {
		files[i], err = l.(filer).File()
		if err != nil {
			return 0, err
		}
		fd, _ := syscall.Dup(int(files[i].Fd()))
		fdc = append(fdc, fmt.Sprint(fd))
		defer files[i].Close()
		i++
	}

	// Use the original binary location. This works with symlinks such that if
	// the file it points to has been changed we will use the updated symlink.
	argv0, err := exec.LookPath(os.Args[0])
	if err != nil {
		return 0, err
	}

	var env []string
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, envCountListners) && !strings.HasPrefix(v, envCountConnections) {
			env = append(env, v)
		}
	}
	env = append(env, fmt.Sprintf("%s=%d", envCountListners, len(fdl)))
	env = append(env, fmt.Sprintf("%s=%d", envCountConnections, len(fdc)))

	allFiles := append([]*os.File{os.Stdin, os.Stdout, os.Stderr}, files...)
	process, err := os.StartProcess(argv0, os.Args, &os.ProcAttr{
		Dir:   originalWD,
		Env:   env,
		Files: allFiles,
	})
	if err != nil {
		return 0, err
	}
	return process.Pid, nil
}

func Serve(servers MultiServer, options ...option) error {
	a := newApp(servers)
	for _, opt := range options {
		opt(a)
	}
	// Acquire Listeners
	if err := a.listen(); err != nil {
		return err
	}

	// Some useful logging.
	if logger != nil {
		if didInherit {
			if ppid == 1 {
				logger.Printf("Listening on init activated %s", a.pprintAddr)
			} else {
				const msg = "Graceful handoff of %s with new pid %d and old pid %d"
				logger.Printf(msg, a.pprintAddr, os.Getpid(), ppid)
			}
		} else {
			const msg = "Serving %s with pid %d"
			logger.Printf(msg, a.pprintAddr, os.Getpid())
		}
	}

	// callback before killing parent process
	if err := a.preKillProcess(); err != nil {
		a.errors <- err
	}
	// Close the parent if we inherited and it wasn't init that started us.
	if didInherit && ppid != 1 {
		if err := syscall.Kill(ppid, syscall.SIGTERM); err != nil {
			return fmt.Errorf("failed to close parent: %s", err)
		}
	}

	waitdone := make(chan struct{})
	go func() {
		defer close(waitdone)
		a.wait()
	}()

	select {
	case err := <-a.errors:
		if err == nil {
			panic("unexpected nil error")
		}
		return err
	case <-waitdone:
		if logger != nil {
			logger.Printf("Exiting pid %d.", os.Getpid())
		}
		return nil
	}
}

// PreStartProcess configures a callback to trigger during graceful restart
// directly before starting the successor process. This allows the current
// process to release holds on resources that the new process will need.
func PreStartProcess(hook func() error) option {
	return func(a *app) {
		a.preStartProcess = hook
	}
}

// PreKillProcess configures a callback to trigger after graceful restart
// directly before killing the parent process. This allows the current
// process to acquire holds on resources that the new process will need.
func PreKillProcess(hook func() error) option {
	return func(a *app) {
		a.preKillProcess = hook
	}
}

func (a *app) pprintAddr() []byte {
	var out bytes.Buffer
	fmt.Fprint(&out, "[HTTP]")
	for i, l := range a.listeners {
		if i != 0 {
			fmt.Fprint(&out, ", ")
		}
		fmt.Fprint(&out, l.Addr())
	}
	fmt.Fprint(&out, " [UDP]")
	for i, l := range a.connections {
		if i != 0 {
			fmt.Fprint(&out, ", ")
		}
		fmt.Fprint(&out, l.LocalAddr())
	}
	return out.Bytes()
}

func SetLogger(l *log.Logger) {
	logger = l
}

type filer interface {
	File() (*os.File, error)
}
