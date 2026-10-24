package dns

import (
	"context"
	go_errors "errors"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/dns"
)

type CachedNameserver interface {
	getCacheController() *CacheController

	sendQuery(ctx context.Context, noResponseErrCh chan<- error, fqdn string, option dns.IPOption)
}

// queryIP is called from dns.Server->queryIPTimeout
func queryIP(ctx context.Context, s CachedNameserver, domain string, option dns.IPOption) ([]net.IP, uint32, error) {
	fqdn := Fqdn(domain)

	cache := s.getCacheController()
	if !cache.disableCache {
		if rec := cache.findRecords(fqdn); rec != nil {
			ips, ttl, err := merge(option, rec.A, rec.AAAA)
			if !go_errors.Is(err, errRecordNotFound) {
				if ttl > 0 {
					errors.LogDebugInner(ctx, err, cache.name, " cache HIT ", fqdn, " -> ", ips)
					log.Record(&log.DNSLog{Server: cache.name, Domain: fqdn, Result: ips, Status: log.DNSCacheHit, Elapsed: 0, Error: err})
					return ips, uint32(ttl), err
				}
				if cache.serveStale && (cache.serveExpiredTTL == 0 || cache.serveExpiredTTL < ttl) {
					errors.LogDebugInner(ctx, err, cache.name, " cache OPTIMISTE ", fqdn, " -> ", ips)
					log.Record(&log.DNSLog{Server: cache.name, Domain: fqdn, Result: ips, Status: log.DNSCacheOptimiste, Elapsed: 0, Error: err})
					go pull(ctx, s, fqdn, option)
					return ips, 1, err
				}
			}
		}
	} else {
		errors.LogDebug(ctx, "DNS cache is disabled. Querying IP for ", fqdn, " at ", cache.name)
	}

	return fetch(ctx, s, fqdn, option)
}

func pull(ctx context.Context, s CachedNameserver, fqdn string, option dns.IPOption) {
	nctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedQueryTimeout)
	defer cancel()

	fetch(nctx, s, fqdn, option)
}

func fetch(ctx context.Context, s CachedNameserver, fqdn string, option dns.IPOption) ([]net.IP, uint32, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	cache := s.getCacheController()
	join := func(ipv4 bool) (*sharedQuery, func()) {
		family := "6"
		if ipv4 {
			family = "4"
		}
		return cache.requestGroup.join(ctx, fqdn+family, cache.queryTimeout, func(queryCtx context.Context) result {
			return doFetch(queryCtx, s, fqdn, dns.IPOption{IPv4Enable: ipv4, IPv6Enable: !ipv4})
		})
	}
	var q4, q6 *sharedQuery
	if option.IPv4Enable {
		q, release := join(true)
		q4 = q
		defer release()
	}
	if option.IPv6Enable {
		q, release := join(false)
		q6 = q
		defer release()
	}
	var ips []net.IP
	ttl := uint32(dns.DefaultTTL)
	var errs []error
	for _, q := range []*sharedQuery{q4, q6} {
		if q == nil {
			continue
		}
		r := q.wait(ctx)
		if len(r.ips) > 0 {
			if option.IPv4Enable != option.IPv6Enable {
				return r.ips, r.ttl, nil
			}
			ips = append(ips, r.ips...)
			ttl = min(ttl, r.ttl)
		} else if r.error != nil {
			errs = append(errs, r.error)
		}
	}
	if len(ips) > 0 {
		return ips, ttl, nil
	}
	if len(errs) > 0 {
		if len(errs) == 1 || go_errors.Is(errs[0], errs[1]) {
			return nil, 0, errs[0]
		}
		return nil, 0, errors.Combine(errs...)
	}
	return nil, 0, dns.ErrEmptyResponse
}

type result struct {
	ips []net.IP
	ttl uint32
	error
}

func doFetch(ctx context.Context, s CachedNameserver, fqdn string, option dns.IPOption) result {
	sub4, sub6 := s.getCacheController().registerSubscribers(fqdn, option)
	defer closeSubscribers(sub4, sub6)
	sub := sub4
	if sub == nil {
		sub = sub6
	}

	// fetch shares each family separately, including transport errors.
	noResponseErrCh := make(chan error, 1)
	start := time.Now()
	s.sendQuery(ctx, noResponseErrCh, fqdn, option)
	var ips []net.IP
	var ttl int32
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-noResponseErrCh:
	case msg := <-sub.Wait():
		ips, ttl, err = msg.(*IPRecord).getIPs()
	}
	var rTTL uint32
	if ttl > 0 {
		rTTL = uint32(ttl)
	} else if len(ips) > 0 { // the reply's TTL may expire during the query
		rTTL = 1
	}

	log.Record(&log.DNSLog{Server: s.getCacheController().name, Domain: fqdn, Result: ips, Status: log.DNSQueried, Elapsed: time.Since(start), Error: err})
	return result{ips, rTTL, err}
}

func merge(option dns.IPOption, rec4 *IPRecord, rec6 *IPRecord, errs ...error) ([]net.IP, int32, error) {
	var allIPs []net.IP
	var rTTL int32 = dns.DefaultTTL

	mergeReq := option.IPv4Enable && option.IPv6Enable

	if option.IPv4Enable {
		ips, ttl, err := rec4.getIPs() // it's safe
		if !mergeReq || go_errors.Is(err, errRecordNotFound) {
			return ips, ttl, err
		}
		if ttl < rTTL {
			rTTL = ttl
		}
		if len(ips) > 0 {
			allIPs = append(allIPs, ips...)
		} else {
			errs = append(errs, err)
		}
	}

	if option.IPv6Enable {
		ips, ttl, err := rec6.getIPs() // it's safe
		if !mergeReq || go_errors.Is(err, errRecordNotFound) {
			return ips, ttl, err
		}
		if ttl < rTTL {
			rTTL = ttl
		}
		if len(ips) > 0 {
			allIPs = append(allIPs, ips...)
		} else {
			errs = append(errs, err)
		}
	}

	if len(allIPs) > 0 {
		return allIPs, rTTL, nil
	}
	if len(errs) == 2 && go_errors.Is(errs[0], errs[1]) {
		return nil, rTTL, errs[0]
	}
	return nil, rTTL, errors.Combine(errs...)
}
