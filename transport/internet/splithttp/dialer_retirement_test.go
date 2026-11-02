package splithttp

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

type retirementTestClient struct {
	closeCalls atomic.Int32
	onClose    func()
}

func (c *retirementTestClient) IsClosed() bool { return c.closeCalls.Load() != 0 }

func (c *retirementTestClient) Close() error {
	c.closeCalls.Add(1)
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}

func (*retirementTestClient) OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	panic("unexpected network operation")
}

func (*retirementTestClient) PostPacket(context.Context, string, string, string, buf.MultiBuffer) error {
	panic("unexpected network operation")
}

func retirementTestPool(t *testing.T) (net.Destination, *internet.MemoryStreamConfig, *XmuxManager) {
	t.Helper()
	dest := net.TCPDestination(net.LocalHostIP, 80)
	settings := &internet.MemoryStreamConfig{ProtocolSettings: &Config{}}
	manager := NewXmuxManager(XmuxConfig{}, func() XmuxConn { return &retirementTestClient{} })
	globalDialerAccess.Lock()
	previous := globalDialerMap
	globalDialerMap = map[dialerConf]*XmuxManager{{dest, settings}: manager}
	globalDialerAccess.Unlock()
	t.Cleanup(func() {
		RetireHTTPClients()
		globalDialerAccess.Lock()
		globalDialerMap = previous
		globalDialerAccess.Unlock()
	})
	return dest, settings, manager
}

func TestRetirementReservesSelectedStream(t *testing.T) {
	dest, settings, _ := retirementTestPool(t)
	transport, client := getHTTPClient(context.Background(), dest, settings)
	if got := client.Running.Load(); got != 1 {
		t.Fatalf("selected stream has %d references, want 1", got)
	}
	if got := RetireHTTPClients(); got != 1 {
		t.Fatalf("retired %d clients, want 1", got)
	}
	if transport.IsClosed() {
		t.Fatal("retirement closed a selected stream before it started")
	}
	if got := RetireHTTPClients(); got != 0 {
		t.Fatalf("repeat retirement found %d clients", got)
	}
	client.DoneRunning()
	if got := transport.(*retirementTestClient).closeCalls.Load(); got != 1 {
		t.Fatalf("drained transport closed %d times, want 1", got)
	}
	_, replacement := getHTTPClient(context.Background(), dest, settings)
	if replacement == client {
		t.Fatal("new stream selected a retired transport")
	}
	replacement.DoneRunning()
}

func TestRetirementWaitsForLastUploadRequest(t *testing.T) {
	dest, settings, _ := retirementTestPool(t)
	transport, client := getHTTPClient(context.Background(), dest, settings)
	client.addRequest() // An upload POST still uses this client after rotation.
	RetireHTTPClients()
	client.DoneRunning() // Release the session's reference.
	if transport.IsClosed() {
		t.Fatal("retirement closed an in-flight upload")
	}
	client.doneRequest()
	if got := transport.(*retirementTestClient).closeCalls.Load(); got != 1 {
		t.Fatalf("drained upload closed %d times, want 1", got)
	}
}

func TestRetirementClosesOutsidePoolLock(t *testing.T) {
	dest, settings, manager := retirementTestPool(t)
	client := manager.GetXmuxClient(context.Background())
	closing, release, retired := make(chan struct{}), make(chan struct{}), make(chan struct{})
	client.XmuxConn.(*retirementTestClient).onClose = func() {
		close(closing)
		<-release
	}
	go func() {
		RetireHTTPClients()
		close(retired)
	}()
	<-closing
	selected := make(chan *XmuxClient, 1)
	go func() {
		_, replacement := getHTTPClient(context.Background(), dest, settings)
		selected <- replacement
	}()
	select {
	case replacement := <-selected:
		replacement.DoneRunning()
	case <-time.After(5 * time.Second):
		close(release)
		<-retired
		t.Fatal("transport cleanup blocked selection of a new client")
	}
	close(release)
	<-retired
}

type retirementPacketClient struct {
	retirementTestClient
	started chan<- *retirementPacketClient
	release <-chan struct{}
	closed  chan struct{}
}

func (*retirementPacketClient) OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	return io.NopCloser(strings.NewReader("")), &net.TCPAddr{}, &net.TCPAddr{}, nil
}

func (c *retirementPacketClient) PostPacket(_ context.Context, _, _, _ string, body buf.MultiBuffer) error {
	defer buf.ReleaseMulti(body)
	c.started <- c
	<-c.release
	return nil
}

func TestRetirementDrainsRotatedPacketUpload(t *testing.T) {
	dest, settings, manager := retirementTestPool(t)
	manager.xmuxConfig.HMaxRequestTimes = &RangeConfig{From: 1, To: 1}
	started, release := make(chan *retirementPacketClient, 1), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	manager.newConnFunc = func() XmuxConn {
		client := &retirementPacketClient{started: started, release: release, closed: make(chan struct{})}
		client.onClose = func() { close(client.closed) }
		return client
	}
	conn, err := Dial(context.Background(), dest, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("upload")); err != nil {
		t.Fatal(err)
	}
	var upload *retirementPacketClient
	select {
	case upload = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("packet upload did not start")
	}
	if got := manager.xmuxClients[0].Running.Load(); got != 1 {
		t.Fatalf("upload POST changed the stream concurrency count to %d, want 1", got)
	}
	// Request-budget expiry already detached the original download client.
	if got := RetireHTTPClients(); got != 1 {
		t.Fatalf("retired %d clients, want the rotated upload", got)
	}
	if upload.IsClosed() {
		t.Fatal("retirement closed the rotated upload while its POST was in flight")
	}
	conn.Close()
	if upload.IsClosed() {
		t.Fatal("session close released an upload still in flight")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-upload.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("drained upload transport was not closed")
	}
	if got := upload.closeCalls.Load(); got != 1 {
		t.Fatalf("drained upload closed %d times, want 1", got)
	}
}
