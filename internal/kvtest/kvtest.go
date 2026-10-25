// Package kvtest runs the KV owner (the Rust kvd, kvd-rs/) for Go tests:
// a kvd process on a local store directory, so tests need no object store
// service. Restart kills it with SIGKILL and starts another on the same
// address and directory, as after a crash.
//
// The binary is $KVD_BIN, or kvd-rs/target/release/kvd in this module,
// built with cargo on first use if it is missing.
package kvtest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/kvrpc"
)

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// Root returns the module root (the directory holding go.mod).
func Root() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("kvtest: go.mod not found")
		}
		dir = parent
	}
}

// BinaryPath returns the kvd binary, building it if needed.
func BinaryPath() (string, error) {
	buildOnce.Do(func() {
		if b := os.Getenv("KVD_BIN"); b != "" {
			binPath, buildErr = filepath.Abs(b)
			return
		}
		root, err := Root()
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(root, "kvd-rs", "target", "release", "kvd")
		if _, err := os.Stat(binPath); err == nil {
			return
		}
		cmd := exec.Command("cargo", "build", "--release", "--manifest-path", filepath.Join(root, "kvd-rs", "Cargo.toml"))
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("kvtest: building kvd-rs: %v\n%s", err, out)
		}
	})
	return binPath, buildErr
}

// Binary is BinaryPath for tests.
func Binary(t testing.TB) string {
	t.Helper()
	b, err := BinaryPath()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// FreeAddr returns a free local TCP address.
func FreeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

// Launch starts a kvd listening on addr with its data in storeDir, logging
// to logPath, and waits until it answers. exited is closed when it exits.
func Launch(addr, storeDir, logPath string, args ...string) (cmd *exec.Cmd, exited <-chan struct{}, err error) {
	bin, err := BinaryPath()
	if err != nil {
		return nil, nil, err
	}
	log, err := os.Create(logPath)
	if err != nil {
		return nil, nil, err
	}
	cmd = exec.Command(bin, append([]string{"-listen", addr, "-store-dir", storeDir}, args...)...)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.Env = append(os.Environ(), "RUST_LOG=warn")
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, nil, err
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); log.Close(); close(done) }()
	c, err := kvrpc.Dial(addr)
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, nil, err
	}
	defer c.Close()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := c.Ping(ctx)
		cancel()
		if err == nil {
			return cmd, done, nil
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			<-done
			b, _ := os.ReadFile(logPath)
			return nil, nil, fmt.Errorf("kvd did not start: %v\n%s", err, b)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Server is a running kvd.
type Server struct {
	Addr string // gRPC address
	Dir  string // the local store directory

	t      testing.TB
	args   []string
	mu     sync.Mutex
	cmd    *exec.Cmd
	exited <-chan struct{}
	n      int
}

// Start runs a kvd on a fresh store directory, stopped with t. args are
// extra flags (e.g. "-txn-lease", "3s").
func Start(t testing.TB, args ...string) *Server {
	t.Helper()
	addr, err := FreeAddr()
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Addr: addr, Dir: t.TempDir(), t: t, args: args}
	s.start()
	t.Cleanup(s.Stop)
	return s
}

func (s *Server) start() {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	args := append([]string{"-owner", fmt.Sprintf("kvtest-%d", s.n)}, s.args...)
	cmd, exited, err := Launch(s.Addr, filepath.Join(s.Dir, "store"), filepath.Join(s.Dir, fmt.Sprintf("kvd%d.log", s.n)), args...)
	if err != nil {
		s.t.Fatal(err)
	}
	s.cmd, s.exited = cmd, exited
}

// Restart kills the kvd with SIGKILL and starts another on the same
// address and store: the next request gets EpochChanged and the partition
// is taken over.
func (s *Server) Restart() {
	s.t.Helper()
	s.kill(syscall.SIGKILL)
	s.start()
}

// Stop ends the kvd (SIGTERM: it flushes and closes its partitions).
func (s *Server) Stop() { s.kill(syscall.SIGTERM) }

func (s *Server) kill(sig syscall.Signal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd == nil {
		return
	}
	_ = s.cmd.Process.Signal(sig)
	<-s.exited
	s.cmd = nil
}

// Restore runs `kvd restore` on the server's store: partition dst becomes
// partition src as it was at time at.
func (s *Server) Restore(src, dst uint64, at time.Time) error {
	out, err := exec.Command(Binary(s.t), "restore", "-store-dir", filepath.Join(s.Dir, "store"),
		"-src", fmt.Sprint(src), "-dst", fmt.Sprint(dst), "-at", at.UTC().Format(time.RFC3339Nano)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kvd restore: %v\n%s", err, out)
	}
	return nil
}

// Proc is a kvd started with Spawn.
type Proc struct {
	Cmd    *exec.Cmd
	Exited chan struct{} // closed when the process has exited
}

// Spawn starts a kvd on the store directory storeDir with args (for a
// directory-managed node: -directory, -node-id, -listen 127.0.0.1:0, ...),
// as dird -spawn-kvd does, and does not wait for it. The process is killed
// with t.
func Spawn(t testing.TB, storeDir string, args ...string) *Proc {
	t.Helper()
	log, err := os.CreateTemp(t.TempDir(), "kvd-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(Binary(t), append([]string{"-store-dir", storeDir}, args...)...)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.Env = append(os.Environ(), "RUST_LOG=warn")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &Proc{Cmd: cmd, Exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		log.Close()
		close(p.Exited)
	}()
	t.Cleanup(func() { p.Kill() })
	return p
}

// Kill ends the process with SIGKILL (no release, no flush) and waits.
func (p *Proc) Kill() {
	_ = p.Cmd.Process.Signal(syscall.SIGKILL)
	<-p.Exited
}
