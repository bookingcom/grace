// Package gracemulti provides easy to use graceful restart
// functionality for HTTP, H2C & UDP servers.
package gracemulti

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bookingcom/grace/graceh2c"
	"github.com/bookingcom/grace/gracenet"
	"github.com/bookingcom/grace/gracenetpacket"
	"github.com/facebookgo/httpdown"
)

const (
	// Used to indicate a graceful restart in the new process.
	envCountTCPListners  = "LISTEN_FDS"
	envCountUDPListeners = "CONN_FDS"

	// default timeout is 60 seconds
	defaultTimeout = 60 * time.Second
)

// In order to keep the working directory the same as when we started we record it at startup.
var originalWD, _ = os.Getwd()

// timeout error
var ErrTimeout = errors.New("shutdown wait timed out")

var (
	logger     *log.Logger
	didInherit = (os.Getenv(envCountTCPListners) != "") || (os.Getenv(envCountUDPListeners) != "")
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
	H2C  []*graceh2c.H2CServer
}

// An app contains one or more servers and associated configuration.
type app struct {
	// Set of servers provided as an input for an app to manage
	// the lifecycle of
	servers MultiServer

	// H2C servers and their internal dependencies
	h2cListeners []net.Listener

	// HTTP servers and their internal dependencies
	http         *httpdown.HTTP
	net          *gracenet.Net
	tcpListeners []net.Listener
	sds          []httpdown.Server

	// UDP servers and their internal dependencies
	packetnet    *gracenetpacket.Net
	udpListeners []*net.UDPConn

	// application-specific internals that help manage its lifecycle
	childPID int
	errors   chan error

	// callbacks that can be configured externally by a client to
	// perform a certain set of actions during the lifecycle of the
	// application
	preStartProcess func() error
	preKillProcess  func() error
	postKilledChild func() error

	// allow non-graceful terminations
	allowNonGracefulTerminations bool
}

func newApp(servers MultiServer) *app {
	lenH2C := len(servers.H2C)
	lenHttp := len(servers.HTTP)
	lenUdp := len(servers.UDP)

	return &app{
		servers: servers,

		http:         &httpdown.HTTP{},
		h2cListeners: make([]net.Listener, 0, lenH2C),
		tcpListeners: make([]net.Listener, 0, lenHttp),
		sds:          make([]httpdown.Server, 0, lenHttp),

		net:          &gracenet.Net{},
		packetnet:    &gracenetpacket.Net{},
		udpListeners: make([]*net.UDPConn, 0, lenUdp),

		childPID: 0,
		errors:   make(chan error, 1+(lenH2C+lenHttp+lenUdp)*2),

		preStartProcess: func() error { return nil },
		preKillProcess:  func() error { return nil },
		postKilledChild: func() error { return nil },

		allowNonGracefulTerminations: false,
	}
}

type option func(*app)

// Serve is the main driver function that a client can invoke to start
// serving requests using all requested servers passed as a `MultiServer`
// object
func Serve(servers MultiServer, options ...option) error {
	// configure the internal app object
	a := newApp(servers)
	for _, opt := range options {
		opt(a)
	}
	// acquire listeners to be able to do the graceful dance later
	if err := a.acquireListeners(); err != nil {
		return err
	}
	// some useful logging
	if logger != nil {
		if didInherit {
			if ppid == 1 {
				logger.Printf("Listening on init activated %s", a.pprintAddr())
			} else {
				const msg = "Graceful handoff of %s with new pid %d and old pid %d"
				logger.Printf(msg, a.pprintAddr(), os.Getpid(), ppid)
			}
		} else {
			const msg = "Serving %s with pid %d"
			logger.Printf(msg, a.pprintAddr(), os.Getpid())
		}
	}
	// callback before killing parent process
	if err := a.preKillProcess(); err != nil {
		a.errors <- err
	}
	// close the parent if we inherited and it wasn't init that started us.
	if didInherit && ppid != 1 {
		if err := syscall.Kill(ppid, syscall.SIGTERM); err != nil {
			return fmt.Errorf("failed to close parent: %s", err)
		}
	}

	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		a.wait()
	}()

	select {
	case err := <-a.errors:
		if err == nil {
			panic("unexpected nil error")
		}
		return err
	case <-waitDone:
		if logger != nil {
			logger.Printf("Exiting pid %d.", os.Getpid())
		}
		return nil
	}
}

// Allow non-graceful terminations when a SIGTERM is signalled to the process
// This is useful when we want all servers being managed to sever their connections
// immediately. Only supported by H2C servers for now because they can wait upto
// `defaultTimeout = 60s` before beginning to terminate their connections.
func AllowNonGracefulTerminations(allowNonGracefulTerminations bool) option {
	return func(a *app) {
		a.allowNonGracefulTerminations = allowNonGracefulTerminations
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

// PostKilledProcess configures a callback to trigger when forked child dies
// abruptly after graceful restart. This allows the parent process to handle
// cleanup, logging & resources that the child process might have used.
func PostKilledChild(hook func() error) option {
	return func(a *app) {
		a.postKilledChild = hook
	}
}

// ------------------- internal methods -----------------------
func (a *app) getHTTPListener(server *http.Server) (net.Listener, error) {
	var (
		l   net.Listener
		err error
	)
	if server.Addr[:5] == "unix:" {
		l, err = a.net.Listen("unix", server.Addr[5:])
	} else {
		l, err = a.net.Listen("tcp", server.Addr)
	}
	if err != nil {
		return nil, err
	}
	return l, err
}

func (a *app) acquireListeners() error {
	for _, s := range a.servers.H2C {
		srv := s.Server()
		l, err := a.getHTTPListener(srv)
		if err != nil {
			return err
		}
		a.h2cListeners = append(a.h2cListeners, l)
	}
	for _, s := range a.servers.HTTP {
		l, err := a.getHTTPListener(s)
		if err != nil {
			return err
		}
		a.tcpListeners = append(a.tcpListeners, l)
	}
	a.packetnet.SetFdStart(len(a.h2cListeners) + len(a.tcpListeners) + 3)
	for _, s := range a.servers.UDP {
		l, err := a.packetnet.ListenPacket(s.Network, s.Addr)
		if err != nil {
			return err
		}
		a.udpListeners = append(a.udpListeners, l.(*net.UDPConn))
	}
	return nil
}

// wait() initiates servers to start serving requests, then awaits
// their termination
func (a *app) wait() {
	var wg sync.WaitGroup
	go a.signalHandler(&wg)
	a.serveH2C(&wg)
	a.serveHttp(&wg)
	a.serveUdp(&wg)
	wg.Wait()
}

// signalHandler manages event-handling corresponding to all external
// signals that the application must react to. This is therefore, also
// the place that initiates the graceful killing and spawning of a new
// process for the application under consideration
func (a *app) signalHandler(wg *sync.WaitGroup) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2, syscall.SIGCHLD)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGCHLD:
			// the forked process was killed before it could send INT/TERM signal.
			// handle cleanup procedure
			a.handleKilledChild()
		case syscall.SIGINT, syscall.SIGTERM:
			// this ensures a subsequent INT/TERM will trigger standard go behaviour of terminating.
			signal.Stop(ch)
			// this ensures process terminates after a timeout eventually to avoid leaking
			// processes when the termination does something awry
			go func() {
				time.Sleep(defaultTimeout)
				a.errors <- ErrTimeout
			}()
			var (
				forceClose bool
			)
			if a.allowNonGracefulTerminations {
				// only check status of the child when non-graceful terminations are
				// allowed on the process managing the servers
				if a.childExists() {
					forceClose = false // the child asking the parent to die
				} else {
					forceClose = true // usual SIGTERMs
				}
			}
			a.terminateProcess(wg, forceClose)
			return
		case syscall.SIGUSR2:
			if !a.childExists() {
				if err := a.preStartProcess(); err != nil {
					a.errors <- err
				}
				// we only return here if there's an error, otherwise the new process
				// will send us a TERM when it's ready to trigger the actual shutdown.
				if _, err := a.startProcess(); err != nil {
					a.errors <- err
				}
			}
		}
	}
}

func (a *app) startProcess() (int, error) {
	tcpListeners, err := a.net.GetActiveListeners()
	if err != nil {
		return 0, err
	}

	udpListeners, err := a.packetnet.GetActiveListeners()
	if err != nil {
		return 0, err
	}

	// Extract the fds from the listeners.
	files := make([]*os.File, len(tcpListeners)+len(udpListeners))
	i := 0
	for _, l := range tcpListeners {
		files[i], err = l.(filer).File()
		if err != nil {
			return 0, err
		}
		defer files[i].Close()
		i++
	}

	for _, l := range udpListeners {
		files[i], err = l.(filer).File()
		if err != nil {
			return 0, err
		}
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
		if !strings.HasPrefix(v, envCountTCPListners) && !strings.HasPrefix(v, envCountUDPListeners) {
			env = append(env, v)
		}
	}
	env = append(env, fmt.Sprintf("%s=%d", envCountTCPListners, len(tcpListeners)))
	env = append(env, fmt.Sprintf("%s=%d", envCountUDPListeners, len(udpListeners)))

	allFiles := append([]*os.File{os.Stdin, os.Stdout, os.Stderr}, files...)
	process, err := os.StartProcess(argv0, os.Args, &os.ProcAttr{
		Dir:   originalWD,
		Env:   env,
		Files: allFiles,
	})
	if err != nil {
		return 0, err
	}
	a.childPID = process.Pid
	return process.Pid, nil
}

func (a *app) terminateProcess(wg *sync.WaitGroup, forceClose bool) {
	for _, s := range a.servers.H2C {
		wg.Add(1)
		go func(s *graceh2c.H2CServer) {
			defer wg.Done()
			if err := s.Stop(forceClose); err != nil {
				a.errors <- err
			}
		}(s)
	}
	for _, s := range a.sds {
		wg.Add(1)
		go func(s httpdown.Server) {
			defer wg.Done()
			if err := s.Stop(); err != nil {
				a.errors <- err
			}
		}(s)
	}
	for _, c := range a.udpListeners {
		wg.Add(1)
		go func(c *net.UDPConn) {
			defer wg.Done()
			if err := c.Close(); err != nil {
				a.errors <- err
			}
		}(c)
	}
}

func (a *app) serveH2C(wg *sync.WaitGroup) {
	for index, s := range a.servers.H2C {
		wg.Add(1)
		go func(s *graceh2c.H2CServer) {
			defer wg.Done()
			srv := s.Server()
			if err := srv.Serve(a.h2cListeners[index]); err != nil && err != http.ErrServerClosed {
				if logger != nil {
					logger.Printf("error serving on %s: %s", srv.Addr, err)
				}
				a.errors <- err
			}
		}(s)
	}
}

func (a *app) serveHttp(wg *sync.WaitGroup) {
	for i, s := range a.servers.HTTP {
		a.sds = append(a.sds, a.http.Serve(s, a.tcpListeners[i]))
	}

	for _, s := range a.sds {
		wg.Add(1)
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
				server.Handler(a.udpListeners[s_index], server.Data)
			}(i, t, s)
		}
	}
}

func (a *app) pprintAddr() []byte {
	var out bytes.Buffer
	fmt.Fprint(&out, " [H2C] ")
	for i, l := range a.h2cListeners {
		if i != 0 {
			fmt.Fprint(&out, ", ")
		}
		fmt.Fprint(&out, l.Addr())
	}
	fmt.Fprint(&out, " [HTTP] ")
	for i, l := range a.tcpListeners {
		if i != 0 {
			fmt.Fprint(&out, ", ")
		}
		fmt.Fprint(&out, l.Addr())
	}
	fmt.Fprint(&out, " [UDP] ")
	for i, l := range a.udpListeners {
		if i != 0 {
			fmt.Fprint(&out, ", ")
		}
		fmt.Fprint(&out, l.LocalAddr())
	}
	return out.Bytes()
}

func (a *app) childExists() bool {
	if a.childPID == 0 {
		return false
	}
	childProcess, err := os.FindProcess(a.childPID)
	if err != nil {
		return false
	}
	err = childProcess.Signal(syscall.Signal(0))
	return err == nil
}

func (a *app) handleKilledChild() {
	// We can receive SIGCHLDs from any os.exec calls as well as during the graceful restart
	// of the main process, so we need to be careful to process all of them without blocking.
	// There is also no guarantee that every child exiting will produce a unique SIGCHLD, hence
	// the loop.
	for {
		var waitStatus syscall.WaitStatus
		terminatedPID, err := syscall.Wait4(-1, &waitStatus, syscall.WNOHANG, nil)
		if (err != nil) || (terminatedPID <= 0) {
			// No more zombies awaiting processing.
			return
		}
		if (a.childPID != 0) && (a.childPID == terminatedPID) {
			a.childPID = 0
			a.postKilledChild()
		}
	}
}

type filer interface {
	File() (*os.File, error)
}

func SetLogger(l *log.Logger) {
	logger = l
}
