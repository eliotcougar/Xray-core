package commander_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/commander"
	"github.com/xtls/xray-core/transport/internet"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestRequireLocal(t *testing.T) {
	tcp := func(ip string) net.Addr { return &net.TCPAddr{IP: net.ParseIP(ip), Port: 8080} }
	for _, tc := range []struct {
		name          string
		remote, local net.Addr
		allow         bool
	}{
		{"loopback IPv4", tcp("127.0.0.2"), tcp("127.0.0.1"), true},
		{"loopback IPv6", tcp("::1"), tcp("::1"), true},
		{"mapped loopback", tcp("::ffff:127.0.0.1"), tcp("::1"), true},
		{"LAN peer", tcp("192.0.2.1"), tcp("192.0.2.2"), false},
		{"nonloopback listener", tcp("127.0.0.1"), tcp("192.0.2.2"), false},
		{"routed API", tcp("0.0.0.0"), tcp("0.0.0.0"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: tc.remote, LocalAddr: tc.local})
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-forwarded-for", "127.0.0.1"))
			err := commander.RequireLocal(ctx)
			if (err == nil) != tc.allow || (err != nil && status.Code(err) != codes.PermissionDenied) {
				t.Fatal(err)
			}
		})
	}
	if err := commander.RequireLocal(context.Background()); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
}

func TestRequireLocalUnixSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api.sock")
	l, err := internet.ListenSystem(context.Background(), &net.UnixAddr{Name: path, Net: "unix"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := commander.RequireLocal(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}))
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	go s.Serve(l)
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///local", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", path)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
}
