package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

// proxy forwards TCP connections to a database, and can hold all traffic in
// both directions, as a network partition does. Nothing is lost: what either
// side sent during a cut arrives when the cut ends, as TCP delivers it once a
// short partition heals.
type proxy struct {
	ln     net.Listener
	target string

	mu   sync.Mutex
	open chan struct{} // closed while traffic flows

	late atomic.Int64 // bytes sent to the database during a cut, delivered after it
}

// newProxy listens on a free local port and forwards to target.
func newProxy(ctx context.Context, target string) *proxy {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		fail(err)
	}
	p := &proxy{ln: ln, target: target, open: make(chan struct{})}
	close(p.open)
	go p.serve(ctx)
	return p
}

func (p *proxy) addr() string { return p.ln.Addr().String() }

func (p *proxy) close() { _ = p.ln.Close() }

// cut holds all traffic until heal.
func (p *proxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.open = make(chan struct{})
}

func (p *proxy) heal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.open)
}

// gate waits while the traffic is cut, and reports whether it was.
func (p *proxy) gate() (held bool) {
	p.mu.Lock()
	open := p.open
	p.mu.Unlock()
	select {
	case <-open:
		return false
	default:
	}
	<-open
	return true
}

func (p *proxy) serve(ctx context.Context) {
	var d net.Dialer
	for {
		client, err := p.ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			continue
		}
		go func() {
			server, err := d.DialContext(ctx, "tcp", p.target)
			if err != nil {
				_ = client.Close()
				return
			}
			go p.pipe(client, server, nil)
			p.pipe(server, client, &p.late)
		}()
	}
}

// pipe copies from src to dst until either fails, holding each chunk while
// the traffic is cut. It counts in late the bytes it held.
func (p *proxy) pipe(dst, src net.Conn, late *atomic.Int64) {
	defer func() { _ = dst.Close() }()
	defer func() { _ = src.Close() }()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if p.gate() && late != nil {
				late.Add(int64(n))
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
