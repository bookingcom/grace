package gracenetpacket

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

const (
	// Used to indicate a graceful restart in the new process.
	envCountKey       = "CONN_FDS"
	envCountKeyPrefix = envCountKey + "="
)

// In order to keep the working directory the same as when we started we record it at startup.
var originalWD, _ = os.Getwd()

// Net provides the family of Packet Listen functions and maintains the associated
// state. Typically you will have only once instance of Net per application.
type Net struct {
	inherited   []net.PacketConn
	active      []net.PacketConn
	mutex       sync.Mutex
	inheritOnce sync.Once

	// used in tests to override the default behavior of starting from fd 3.
	fdStart int
}

func (n *Net) SetFdStart(fd int) {
	n.fdStart = fd
}

func (n *Net) inherit() error {
	var retErr error
	n.inheritOnce.Do(func() {
		n.mutex.Lock()
		defer n.mutex.Unlock()
		countStr := os.Getenv(envCountKey)
		if countStr == "" {
			return
		}
		count, err := strconv.Atoi(countStr)
		if err != nil {
			retErr = fmt.Errorf("found invalid count value: %s=%s", envCountKey, countStr)
			return
		}

		fdStart := n.fdStart
		if fdStart == 0 {
			fdStart = 3
		}

		for i := fdStart; i < fdStart+count; i++ {
			file := os.NewFile(uintptr(i), "packet_listener")
			l, err := net.FilePacketConn(file)
			if err != nil {
				file.Close()
				retErr = fmt.Errorf("error inheriting socket fd %d: %s", i, err)
				return
			}
			if err := file.Close(); err != nil {
				retErr = fmt.Errorf("error closing inherited socket fd %d: %s", i, err)
				return
			}
			n.inherited = append(n.inherited, l)
		}
	})
	return retErr
}

// ListenPacket announces on the local network address laddr. The network must
// be "udp", "udp4" or "udp6". It returns an inherited net.PacketConn for the
// matching network and address, or creates a new one using net.ListenPacket.
func (n *Net) ListenPacket(nett, laddr string) (net.PacketConn, error) {
	switch nett {
	default:
		return nil, net.UnknownNetworkError(nett)
	case "udp", "udp4", "udp6":
		addr, err := net.ResolveUDPAddr(nett, laddr)
		if err != nil {
			return nil, err
		}
		return n.ListenUDP(nett, addr)
	}
}

// ListenUDP announces on the local network address laddr. The network net must
// be: "udp", "udp4" or "udp6". It returns an inherited net.PacketConn for the
// matching network and address, or creates a new one using net.ListenUDP.
func (n *Net) ListenUDP(nett string, laddr *net.UDPAddr) (*net.UDPConn, error) {
	if err := n.inherit(); err != nil {
		return nil, err
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	// look for an inherited listener
	for i, l := range n.inherited {
		if l == nil { // we nil used inherited listeners
			continue
		}
		if isSameAddr(l.LocalAddr(), laddr) {
			n.inherited[i] = nil
			n.active = append(n.active, l)
			return l.(*net.UDPConn), nil
		}
	}

	// make a fresh listener
	l, err := net.ListenUDP(nett, laddr)
	if err != nil {
		return nil, err
	}
	n.active = append(n.active, l)
	return l, nil
}

// activeListeners returns a snapshot copy of the active listeners.
func (n *Net) activeListeners() ([]net.PacketConn, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	ls := make([]net.PacketConn, len(n.active))
	copy(ls, n.active)
	return ls, nil
}

func (n *Net) GetActiveListeners() ([]net.PacketConn, error) {
	return n.activeListeners()
}

func isSameAddr(a1, a2 net.Addr) bool {
	if a1.Network() != a2.Network() {
		return false
	}
	a1s := a1.String()
	a2s := a2.String()
	if a1s == a2s {
		return true
	}

	// This allows for ipv6 vs ipv4 local addresses to compare as equal. This
	// scenario is common when listening on localhost.
	const ipv6prefix = "[::]"
	a1s = strings.TrimPrefix(a1s, ipv6prefix)
	a2s = strings.TrimPrefix(a2s, ipv6prefix)
	const ipv4prefix = "0.0.0.0"
	a1s = strings.TrimPrefix(a1s, ipv4prefix)
	a2s = strings.TrimPrefix(a2s, ipv4prefix)
	return a1s == a2s
}

// StartProcess starts a new process passing it the active listeners. It
// doesn't fork, but starts a new process using the same environment and
// arguments as when it was originally started. This allows for a newly
// deployed binary to be started. It returns the pid of the newly started
// process when successful.
func (n *Net) StartProcess() (int, error) {
	listeners, err := n.activeListeners()
	if err != nil {
		return 0, err
	}

	// Extract the fds from the listeners.
	files := make([]*os.File, len(listeners))
	for i, l := range listeners {
		files[i], err = l.(filer).File()
		if err != nil {
			return 0, err
		}
		defer files[i].Close()
	}
	argv0, err := exec.LookPath(os.Args[0])
	if err != nil {
		return 0, err
	}

	// Pass on the environment and replace the old count key with the new one.
	var env []string
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, envCountKeyPrefix) {
			env = append(env, v)
		}
	}
	env = append(env, fmt.Sprintf("%s%d", envCountKeyPrefix, len(listeners)))

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

type filer interface {
	File() (*os.File, error)
}
