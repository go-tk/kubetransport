package kubetransport

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-tk/kubetransport/internal/k8sclient"
)

type endpointsRegistry struct {
	backgroundCtx    context.Context
	stop             context.CancelFunc
	k8sClient        k8sclient.K8sClient
	ipAddressesCache sync.Map
}

func newEndpointsRegistry(backgroundCtx context.Context, k8sClient k8sclient.K8sClient, tickInterval time.Duration) *endpointsRegistry {
	var er endpointsRegistry
	er.backgroundCtx, er.stop = context.WithCancel(backgroundCtx)
	er.k8sClient = k8sClient
	go er.tick(tickInterval)
	return &er
}

func (er *endpointsRegistry) tick(tickInterval time.Duration) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-er.backgroundCtx.Done():
			return
		case <-ticker.C:
			er.evictIPAddressesCache()
		}
	}
}

func (er *endpointsRegistry) evictIPAddressesCache() {
	er.ipAddressesCache.Range(func(_, value interface{}) bool {
		if cachedIPAddresses, ok := value.(*cachedIPAddresses); ok {
			if hitCount := atomic.LoadInt64(&cachedIPAddresses.HitCount); hitCount == 0 {
				cachedIPAddresses.Source.Stop()
			} else {
				atomic.CompareAndSwapInt64(&cachedIPAddresses.HitCount, hitCount, 0)
			}
		}
		return true
	})
}

func (er *endpointsRegistry) GetIPAddresses(ctx context.Context, namespace string, endpointsName string) ([]string, error) {
	if namespace == "" {
		namespace = er.k8sClient.Namespace()
	}
	endpointKey := endpointKey{namespace, endpointsName}
	value, ok := er.ipAddressesCache.Load(endpointKey)
	if !ok {
		results := getIPAddressesResults{Waiter: make(chan struct{})}
		value, ok = er.ipAddressesCache.LoadOrStore(endpointKey, &results)
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
	case *cachedIPAddresses:
		cachedIPAddresses := value
		atomic.AddInt64(&cachedIPAddresses.HitCount, 1)
		return cachedIPAddresses.Value, nil
	default:
		panic("unreachable code")
	}
}

func (er *endpointsRegistry) doGetIPAddresses(endpointKey endpointKey, results *getIPAddressesResults) {
	ipAddressesCallback := func(ipAddressesSource *ipAddressesSource, ipAddresses []string, err error) {
		if err == nil {
			cachedIPAddresses := cachedIPAddresses{
				Source:   ipAddressesSource,
				Value:    ipAddresses,
				HitCount: 1,
			}
			er.ipAddressesCache.Store(endpointKey, &cachedIPAddresses)
		} else {
			er.ipAddressesCache.Delete(endpointKey)
		}
		if results != nil {
			results.IPAddresses, results.Err = ipAddresses, err
			close(results.Waiter)
			results = nil
		}
	}
	newIPAddressesSource(er.backgroundCtx, er.k8sClient, endpointKey.Namespace, endpointKey.EndpointsName, ipAddressesCallback)
}

func (er *endpointsRegistry) Stop() { er.stop() }

type endpointKey struct {
	Namespace     string
	EndpointsName string
}

type getIPAddressesResults struct {
	Waiter      chan struct{}
	IPAddresses []string
	Err         error
}

type cachedIPAddresses struct {
	Source   *ipAddressesSource
	Value    []string
	HitCount int64
}
