package kubetransport

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-tk/kubetransport/internal/k8sclient"
)

// MustWrapTransport likes WrapTransport but panics when an error occurs.
func MustWrapTransport(transport http.RoundTripper) http.RoundTripper {
	transport, err := WrapTransport(transport)
	if err != nil {
		panic(fmt.Sprintf("wrap transport: %v", err))
	}
	return transport
}

// WrapTransport wraps the given transport for client-side load balancing in Kubernetes.
func WrapTransport(transport http.RoundTripper) (http.RoundTripper, error) {
	newTransportWrapperOnce.Do(func() {
		newTransportWrapperResults.TransportWrapper, newTransportWrapperResults.Err = newTransportWrapper()
	})
	transportWrapper, err := newTransportWrapperResults.TransportWrapper, newTransportWrapperResults.Err
	if err != nil {
		return nil, newTransportWrapperResults.Err
	}
	return transportWrapper.WrapTransport(transport), nil
}

var (
	newTransportWrapperOnce    sync.Once
	newTransportWrapperResults struct {
		TransportWrapper *transportWrapper
		Err              error
	}
)

type transportWrapper struct {
	endpointsRegistry *endpointsRegistry
}

func newTransportWrapper() (*transportWrapper, error) {
	var tw transportWrapper
	k8sClient, err := k8sclient.New()
	if err != nil {
		return nil, err
	}
	tw.endpointsRegistry = newEndpointsRegistry(context.Background(), k8sClient, 1*time.Minute)
	return &tw, nil
}

func (tw *transportWrapper) WrapTransport(transport http.RoundTripper) http.RoundTripper {
	return newKubeTransport(tw.endpointsRegistry, transport, uint64(time.Now().UnixNano()))
}
