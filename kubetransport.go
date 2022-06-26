package kubetransport

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
)

type kubeTransport struct {
	endpointsRegistry *endpointsRegistry
	transport         http.RoundTripper
	seed              uint64
}

var _ http.RoundTripper = (*kubeTransport)(nil)

func newKubeTransport(endpointsRegistry *endpointsRegistry, transport http.RoundTripper, seed uint64) *kubeTransport {
	var kt kubeTransport
	kt.endpointsRegistry = endpointsRegistry
	kt.transport = transport
	kt.seed = seed
	return &kt
}

func (kt *kubeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := kt.resolveHostname(request); err != nil {
		return nil, err
	}
	return kt.transport.RoundTrip(request)
}

func (kt *kubeTransport) resolveHostname(request *http.Request) error {
	url := request.URL
	const schemePrefix = "kube-"
	if !strings.HasPrefix(url.Scheme, schemePrefix) {
		return nil
	}
	url.Scheme = url.Scheme[len(schemePrefix):]
	hostname := url.Host
	var port string
	if i := strings.LastIndexByte(hostname, ':'); i >= 0 {
		port = hostname[i:]
		hostname = hostname[:i]
	}
	endpointsName := strings.TrimSuffix(hostname, ".svc.cluster.local")
	var namespace string
	if i := strings.LastIndexByte(endpointsName, '.'); i >= 0 {
		namespace = endpointsName[i+1:]
		endpointsName = endpointsName[:i]
	}
	ipAddresses, err := kt.endpointsRegistry.GetIPAddresses(request.Context(), namespace, endpointsName)
	if err != nil {
		return fmt.Errorf("get ip addresses; namespace=%q endpointsName=%q: %w", namespace, endpointsName, err)
	}
	if len(ipAddresses) == 0 {
		var err error
		if ipAddresses == nil {
			err = ErrEndpointsNotFound
		} else {
			err = ErrNoIPAddress
		}
		return fmt.Errorf("%w; namespace=%q endpointsName=%q", err, namespace, endpointsName)
	}
	ipAddress := kt.pickIPAddress(ipAddresses)
	url.Host = ipAddress + port
	return nil
}

var (
	// ErrEndpointsNotFound is returned when the endpoints does not exist.
	ErrEndpointsNotFound = errors.New("kubetransport: endpoints not found")

	// ErrNoIPAddress is returned when there is no ip address of the endpoints.
	ErrNoIPAddress = errors.New("kubetransport: no ip address")
)

func (kt *kubeTransport) pickIPAddress(ipAddresses []string) string {
	x := splitmix64(&kt.seed)
	i := int(x % uint64(len(ipAddresses)))
	ipAddress := ipAddresses[i]
	return ipAddress
}

func splitmix64(seed *uint64) uint64 {
	z := atomic.AddUint64(seed, 0x9E3779B97F4A7C15)
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}
