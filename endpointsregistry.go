package kubetransport

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

type endpointsRegistry struct {
	backgroundCtx   context.Context
	endpointsGetter v1.EndpointsGetter
	m               sync.Map
}

func newEndpointsRegistry(backgroundCtx context.Context, endpointsGetter v1.EndpointsGetter, tickPeriod time.Duration) *endpointsRegistry {
	var er endpointsRegistry
	er.backgroundCtx = backgroundCtx
	er.endpointsGetter = endpointsGetter
	go func() {
		ticker := time.NewTicker(tickPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-backgroundCtx.Done():
				return
			case <-ticker.C:
				er.clearIdleWatches()
			}
		}
	}()
	return &er
}

func (er *endpointsRegistry) clearIdleWatches() {
	er.m.Range(func(_, value interface{}) bool {
		if ipAddressesCache, ok := value.(*ipAddressesCache); ok {
			if hitCount := atomic.LoadInt64(ipAddressesCache.HitCount); hitCount == 0 {
				ipAddressesCache.Source.ClearWatch()
			} else {
				atomic.CompareAndSwapInt64(ipAddressesCache.HitCount, hitCount, 0)
			}
		}
		return true
	})
}

func (er *endpointsRegistry) GetIPAddresses(ctx context.Context, namespace string, endpointsName string) ([]string, error) {
	endpointKey := endpointKey{namespace, endpointsName}
	value, ok := er.m.Load(endpointKey)
	if !ok {
		results := getIPAddressesResults{
			Waiter: make(chan struct{}),
		}
		value, ok = er.m.LoadOrStore(endpointKey, &results)
		if !ok {
			er.doGetIPAddresses(endpointKey, &results)
		}
	}
	switch value := value.(type) {
	case *getIPAddressesResults:
		results := value
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-results.Waiter:
			return results.IPAddresses, results.Err
		}
	case *ipAddressesCache:
		ipAddressesCache := value
		atomic.AddInt64(ipAddressesCache.HitCount, 1)
		return ipAddressesCache.Value, nil
	default:
		panic("unreachable code")
	}
}

func (er *endpointsRegistry) doGetIPAddresses(endpointKey endpointKey, results *getIPAddressesResults) {
	ipAddressSource := newIPAddressSource(er.backgroundCtx, er.endpointsGetter, endpointKey.Namespace, endpointKey.EndpointsName)
	hitCount := int64(1)
	ipAddressSource.GetValuesAndSetWatch(func(ipAddresses []string, err error) {
		if err == nil {
			ipAddressesCache := ipAddressesCache{
				Source:   ipAddressSource,
				Value:    ipAddresses,
				HitCount: &hitCount,
			}
			er.m.Store(endpointKey, &ipAddressesCache)
		} else {
			er.m.Delete(endpointKey)
		}
		if results != nil {
			results.IPAddresses, results.Err = ipAddresses, err
			close(results.Waiter)
			results = nil
		}
	})
}

type endpointsRegistryState struct {
	IPAddressesCacheInfos []ipAddressesCacheInfo
}

type ipAddressesCacheInfo struct {
	Value    []string
	HitCount int64
}

func (er *endpointsRegistry) State() endpointsRegistryState {
	var ipAddressesCacheInfos []ipAddressesCacheInfo
	er.m.Range(func(_, value interface{}) bool {
		ipAddressesCache, ok := value.(*ipAddressesCache)
		if !ok {
			return true
		}
		ipAddressesCacheInfo := ipAddressesCacheInfo{
			Value:    ipAddressesCache.Value,
			HitCount: atomic.LoadInt64(ipAddressesCache.HitCount),
		}
		ipAddressesCacheInfos = append(ipAddressesCacheInfos, ipAddressesCacheInfo)
		return true
	})
	sort.Slice(ipAddressesCacheInfos, func(i, j int) bool {
		return stringSliceIsLess(ipAddressesCacheInfos[i].Value, ipAddressesCacheInfos[j].Value)
	})
	state := endpointsRegistryState{
		IPAddressesCacheInfos: ipAddressesCacheInfos,
	}
	return state
}

func stringSliceIsLess(a, b []string) bool {
	n1 := len(a)
	n2 := len(b)
	var n int
	if n1 < n2 {
		n = n1
	} else {
		n = n2
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return n1 < n2
}

type endpointKey struct {
	Namespace     string
	EndpointsName string
}

type getIPAddressesResults struct {
	Waiter      chan struct{}
	IPAddresses []string
	Err         error
}

type ipAddressesCache struct {
	Source   *ipAddressSource
	Value    []string
	HitCount *int64
}
