package graceh2c

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set"
	"golang.org/x/net/http2"
	H2C "golang.org/x/net/http2/h2c"
)

const (
	defaultTimeout = 60 * time.Second
)

type H2CServer struct {
	// server that accepts H2C connections
	srv *http.Server

	// state channels that notify the caller of the state
	// of when the server begins to shutdown and when it
	// completes shutting down
	shuttingDown chan struct{}
	shutdownDone chan struct{}
	forceClose   chan struct{}

	// helps manage shutdown hooks that are called after
	// the server has been instructed to shut down
	shutdownHooks      []func(ctx context.Context)
	shutdownHooksMutex sync.Mutex

	// internal cache that keeps account of all hijacked
	// connections on the given server
	connectionCache *h2cInfo

	shutdownOnce   sync.Once
	shutdownErr    error
	contextTimeout time.Duration

	logger *log.Logger
}

type option func(*H2CServer)

func Logger(l *log.Logger) option {
	return func(a *H2CServer) {
		a.logger = l
	}
}

// ContextTimeout is the maximum time for which the H2C server
// waits after calling Stop() on it while the current connections
// close; after which the server closes them forcefully and exits.
//
// defaults to 60s
func ContextTimeout(timeout time.Duration) option {
	return func(a *H2CServer) {
		a.contextTimeout = timeout
	}
}

func NewH2CServer(server *http.Server, options ...option) *H2CServer {
	h2cServer := &H2CServer{
		srv:            server,
		shuttingDown:   make(chan struct{}),
		shutdownDone:   make(chan struct{}),
		forceClose:     make(chan struct{}),
		contextTimeout: defaultTimeout,
	}
	// functional options
	for _, opt := range options {
		opt(h2cServer)
	}
	// get a connection cache
	h2cServer.connectionCache = &h2cInfo{
		h2s:   &http2.Server{},
		conns: mapset.NewSet(),
	}
	// setup shutdown hook
	h2cServer.addShutdownHook(func(ctx context.Context) {
		handleGracefulShutdown(ctx, h2cServer.connectionCache, h2cServer.forceClose)
	})
	// setup request hijacker
	h2cServer.setupHijacker()
	return h2cServer
}

func (s *H2CServer) Server() *http.Server {
	return s.srv
}

func (s *H2CServer) ShuttingDown() chan struct{} {
	return s.shuttingDown
}

func (s *H2CServer) ShutdownDone() chan struct{} {
	return s.shutdownDone
}

func (s *H2CServer) Stop(forceClose bool) error {
	s.shutdownHooksMutex.Lock()
	defer s.shutdownHooksMutex.Unlock()

	if forceClose {
		close(s.forceClose)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.contextTimeout)
	defer cancel()

	s.shutdownErr = nil
	s.shutdownOnce.Do(func() {
		// close the shuttingDown channel to notify that the server
		// has commenced shutting down
		close(s.shuttingDown)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.srv.Shutdown(ctx); err != nil {
				// graceful shutdown failed
				if err := s.srv.Close(); err != nil {
					// forceful shutdown failed
					if s.logger != nil {
						s.logger.Printf("both graceful and forceful shutdown failed: %s", err)
					}
					s.shutdownErr = err
				}
			}
		}()
		// execute all shutdown hooks
		for _, f := range s.shutdownHooks {
			f := f // capturing range variable
			wg.Add(1)
			go func() {
				defer wg.Done()
				f(ctx)
			}()
		}
		wg.Wait()
		// close the shutdownDone channel to notify that the server
		// has finished shutting down, shutdownErr should report how
		// it went
		close(s.shutdownDone)
	})
	return s.shutdownErr
}

// ===================== internal methods ========================== //
func (s *H2CServer) addShutdownHook(f func(ctx context.Context)) {
	s.shutdownHooksMutex.Lock()
	defer s.shutdownHooksMutex.Unlock()
	s.shutdownHooks = append(s.shutdownHooks, f)
}

func (s *H2CServer) setupHijacker() {
	originalHandler := s.srv.Handler
	serverLifetimeHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// check if the server has been closed
		select {
		case _, ok := <-s.ShuttingDown():
			if !ok {
				w.Header().Set("Connection", "close")
			}
		default: // non-blocking
		}
		originalHandler.ServeHTTP(w, r)
	})
	s.srv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			if s.logger != nil {
				s.logger.Fatalf("couldn't hijack the response writer")
			}
			return
		}
		// setup
		hw := h2cHijacker{
			ResponseWriter: w,
			Hijacker:       hijacker,
			onHijack: func(c *hijackedConn) {
				s.connectionCache.conns.Add(c)
				c.onClose = func(c *hijackedConn) {
					s.connectionCache.conns.Remove(c)
				}
			},
		}
		// now serve with the original handler, but with the custom responseWriter
		H2C.NewHandler(serverLifetimeHandler, s.connectionCache.h2s).ServeHTTP(hw, r)
	})
}

func handleGracefulShutdown(ctx context.Context, h2c *h2cInfo, forceClose chan struct{}) {
LOOP:
	for _, _c := range h2c.conns.ToSlice() {
		c := _c.(*hijackedConn)
		select {
		case <-forceClose:
			c.Close()
			continue LOOP
		case <-ctx.Done():
			break LOOP
		case <-c.closed:
			h2c.conns.Remove(c)
		}
	}
	// if any connection remain after the first run, close them forcefully
	for _, _c := range h2c.conns.ToSlice() {
		c := _c.(*hijackedConn)
		c.Close()
		h2c.conns.Remove(c)
	}
}
