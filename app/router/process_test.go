package router

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/xtls/xray-core/common/geodata"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	"github.com/xtls/xray-core/testing/mocks"
)

func processTestContext(network net.Network) routing.Context {
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{Source: net.Destination{
		Network: network, Address: net.ParseAddress("172.16.0.1"), Port: 45678,
	}})
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: net.Destination{
		Network: network, Address: net.ParseAddress("192.0.2.1"), Port: 443,
	}}})
	return routing_session.AsRoutingContext(ctx)
}

func processTestConfig() *Config {
	return &Config{Rule: []*RoutingRule{
		{Process: []string{"-1"}, TargetTag: &RoutingRule_Tag{Tag: "block"}},
		{Process: []string{"10001"}, TargetTag: &RoutingRule_Tag{Tag: "special"}},
		{Networks: []net.Network{net.Network_TCP, net.Network_UDP}, TargetTag: &RoutingRule_Tag{Tag: "main"}},
	}}
}

func TestProcessRoutingOwnerSnapshot(t *testing.T) {
	for _, network := range []net.Network{net.Network_TCP, net.Network_UDP} {
		for _, tc := range []struct {
			name string
			uids []int
			want string
		}{
			{"stable owner", []int{10001, 10001}, "special"},
			{"owner disappears between rules", []int{10001, -1}, "special"},
			{"unidentified owner", []int{-1, 10001}, "block"},
			{"different owner appears between rules", []int{10002, 10001}, "main"},
		} {
			t.Run(network.String()+"/"+tc.name, func(t *testing.T) {
				r := new(Router)
				if err := r.Init(context.Background(), processTestConfig(), nil, nil, nil); err != nil {
					t.Fatal(err)
				}
				calls := 0
				r.processLookup = func(proto, src string, srcPort uint16, dst string, dstPort uint16) (int, string, string, error) {
					if proto != network.SystemString() || src != "172.16.0.1" || srcPort != 45678 || dst != "192.0.2.1" || dstPort != 443 {
						t.Fatalf("wrong socket tuple: %s %s:%d -> %s:%d", proto, src, srcPort, dst, dstPort)
					}
					uid := tc.uids[calls%len(tc.uids)]
					calls++
					return uid, strconv.Itoa(uid), "", nil
				}
				route, err := r.PickRoute(processTestContext(network))
				if err != nil {
					t.Fatal(err)
				}
				if got := route.GetOutboundTag(); got != tc.want {
					t.Fatalf("wanted %s, got %s", tc.want, got)
				}
				if calls != 1 {
					t.Fatalf("wanted one owner lookup, got %d", calls)
				}
			})
		}
	}
}

type processDomainContext struct{ routing.Context }

func (processDomainContext) GetTargetDomain() string { return "example.test" }

func TestProcessRoutingSnapshotAcrossDNS(t *testing.T) {
	for _, strategy := range []Config_DomainStrategy{Config_IpOnDemand, Config_IpIfNonMatch} {
		t.Run(strategy.String(), func(t *testing.T) {
			ctl := gomock.NewController(t)
			dns := mocks.NewDNSClient(ctl)
			dns.EXPECT().LookupIP("example.test", gomock.Any()).Return([]net.IP{{203, 0, 113, 9}}, uint32(60), nil).Times(1)
			config := processTestConfig()
			config.DomainStrategy = strategy
			config.Rule = config.Rule[:2]
			config.Rule[1].Ip = []*geodata.IPRule{{Value: &geodata.IPRule_Custom{Custom: &geodata.CIDRRule{Cidr: &geodata.CIDR{Ip: []byte{203, 0, 113, 9}, Prefix: 32}}}}}
			r := new(Router)
			if err := r.Init(context.Background(), config, dns, nil, nil); err != nil {
				t.Fatal(err)
			}
			calls := 0
			r.processLookup = func(_, _ string, _ uint16, dst string, _ uint16) (int, string, string, error) {
				if dst != "192.0.2.1" {
					t.Fatalf("lookup used DNS replacement %s instead of original destination", dst)
				}
				calls++
				if calls > 1 {
					return -1, "-1", "", nil
				}
				return 10001, "10001", "", nil
			}
			route, err := r.PickRoute(processDomainContext{processTestContext(net.Network_TCP)})
			if err != nil {
				t.Fatal(err)
			}
			if route.GetOutboundTag() != "special" || calls != 1 {
				t.Fatalf("tag=%s lookups=%d", route.GetOutboundTag(), calls)
			}
			route, err = r.PickRoute(route)
			if err != nil {
				t.Fatal(err)
			}
			if route.GetOutboundTag() != "block" || calls != 2 {
				t.Fatalf("fresh DNS decision: tag=%s lookups=%d", route.GetOutboundTag(), calls)
			}
		})
	}
}

func TestProcessRoutingFreshDecision(t *testing.T) {
	r := new(Router)
	if err := r.Init(context.Background(), processTestConfig(), nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	calls := 0
	r.processLookup = func(string, string, uint16, string, uint16) (int, string, string, error) {
		calls++
		if calls == 1 {
			return 10001, "10001", "", nil
		}
		return -1, "-1", "", nil
	}
	route, err := r.PickRoute(processTestContext(net.Network_TCP))
	if err != nil {
		t.Fatal(err)
	}
	if route.GetOutboundTag() != "special" {
		t.Fatal(route.GetOutboundTag())
	}
	// Reusing even the returned routing context must create a fresh snapshot.
	route, err = r.PickRoute(route)
	if err != nil {
		t.Fatal(err)
	}
	if route.GetOutboundTag() != "block" || calls != 2 {
		t.Fatalf("tag=%s lookups=%d", route.GetOutboundTag(), calls)
	}
}

func TestProcessRoutingLookupIsLazy(t *testing.T) {
	r := new(Router)
	config := processTestConfig()
	config.Rule = config.Rule[2:]
	if err := r.Init(context.Background(), config, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	r.processLookup = func(string, string, uint16, string, uint16) (int, string, string, error) {
		t.Fatal("lookup without process rules")
		return 0, "", "", nil
	}
	if _, err := r.PickRoute(processTestContext(net.Network_TCP)); err != nil {
		t.Fatal(err)
	}
}

func TestProcessRoutingCachesErrors(t *testing.T) {
	expected := errors.New("lookup failed")
	calls := 0
	ctx := &processRoutingContext{findProcess: func(string, string, uint16, string, uint16) (int, string, string, error) {
		calls++
		return 0, "", "", expected
	}}
	for range 2 {
		_, _, _, err := ctx.lookup("tcp", "172.16.0.1", 45678, "192.0.2.1", 443)
		if err != expected {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatal(calls)
	}
}

func TestProcessRoutingConcurrentDecisions(t *testing.T) {
	r := new(Router)
	if err := r.Init(context.Background(), processTestConfig(), nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	r.processLookup = func(string, string, uint16, string, uint16) (int, string, string, error) {
		calls.Add(1)
		return 10001, "10001", "", nil
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			route, err := r.PickRoute(processTestContext(net.Network_TCP))
			if err != nil {
				t.Error(err)
				return
			}
			if route.GetOutboundTag() != "special" {
				t.Error(route.GetOutboundTag())
			}
		})
	}
	wg.Wait()
	if calls.Load() != 32 {
		t.Fatal(calls.Load())
	}
}

func TestProcessRoutingDomainDestinationFallback(t *testing.T) {
	ctl := gomock.NewController(t)
	dns := mocks.NewDNSClient(ctl)
	dns.EXPECT().LookupIP("example.test", gomock.Any()).Return([]net.IP{{203, 0, 113, 9}}, uint32(60), nil).Times(1)
	config := processTestConfig()
	config.DomainStrategy = Config_IpOnDemand
	r := new(Router)
	if err := r.Init(context.Background(), config, dns, nil, nil); err != nil {
		t.Fatal(err)
	}
	calls := 0
	r.processLookup = func(_, _ string, _ uint16, dst string, dstPort uint16) (int, string, string, error) {
		calls++
		if dst != "203.0.113.9" || dstPort != 443 {
			t.Fatalf("wrong domain fallback %s:%d", dst, dstPort)
		}
		return 10001, "10001", "", nil
	}
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{Source: net.TCPDestination(net.ParseAddress("172.16.0.1"), 45678)})
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("example.test"), 443)}})
	route, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if route.GetOutboundTag() != "special" || calls != 1 {
		t.Fatalf("tag=%s lookups=%d", route.GetOutboundTag(), calls)
	}
}
