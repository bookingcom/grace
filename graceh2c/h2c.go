package graceh2c

import (
	"bufio"
	"net"
	"net/http"
	"sync"

	mapset "github.com/deckarep/golang-set"
	"golang.org/x/net/http2"
)

type h2cInfo struct {
	h2s   *http2.Server
	conns mapset.Set
}

type hijackedConn struct {
	net.Conn
	onClose   func(*hijackedConn)
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *hijackedConn) Close() error {
	defer c.closeOnce.Do(func() {
		close(c.closed)
		c.onClose(c)
	})
	return c.Conn.Close()
}

type h2cHijacker struct {
	http.ResponseWriter
	http.Hijacker
	onHijack func(*hijackedConn)
}

func (h h2cHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c, r, err := h.Hijacker.Hijack()
	conn := &hijackedConn{
		Conn:   c,
		closed: make(chan struct{}),
	}
	h.onHijack(conn)
	return conn, r, err
}
