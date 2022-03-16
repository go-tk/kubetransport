package kubetransport

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/valyala/fastrand"
)

type kubeTransport struct {
	endpointsRegistry *endpointsRegistry
	currentNamespace  string
	transport         http.RoundTripper
}

var _ http.RoundTripper = (*kubeTransport)(nil)

func newKubeTransport(endpointsRegistry *endpointsRegistry, currentNamespace string, transport http.RoundTripper) *kubeTransport {
	var kt kubeTransport
	kt.endpointsRegistry = endpointsRegistry
	kt.currentNamespace = currentNamespace
	kt.transport = transport
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
	if i := strings.LastIndexByte(endpointsName, '.'); i < 0 {
		namespace = kt.currentNamespace
	} else {
		namespace = endpointsName[i+1:]
		endpointsName = endpointsName[:i]
	}
	ipAddresses, err := kt.endpointsRegistry.GetIPAddresses(request.Context(), namespace, endpointsName)
	if err != nil {
		return fmt.Errorf("get ip addresses; hostname=%q: %w", hostname, err)
	}
	if ipAddresses == nil {
		return fmt.Errorf("%w; namespace=%q endpointsName=%q", errEndpointsNotFound, namespace, endpointsName)
	}
	n := len(ipAddresses)
	if n == 0 {
		return fmt.Errorf("%w; namespace=%q endpointsName=%q", errNoIPAddress, namespace, endpointsName)
	}
	i := int(fastrand.Uint32n(uint32(n)))
	ipAddress := ipAddresses[i]
	url.Host = ipAddress + port
	return nil
}

var errEndpointsNotFound = errors.New("kubetransport: endpoints not found")
var errNoIPAddress = errors.New("kubetransport: no ip address")
