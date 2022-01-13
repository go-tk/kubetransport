package kubetransport

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

var makeTransportWrapperResults struct {
	TransportWrapper func(http.RoundTripper) http.RoundTripper
	Err              error
}

var makeTransportWrapperOnce sync.Once

func WrapTransport(transport http.RoundTripper) (http.RoundTripper, error) {
	transportWrapper := &makeTransportWrapperResults.TransportWrapper
	err := &makeTransportWrapperResults.Err
	makeTransportWrapperOnce.Do(func() {
		backgroundCtx, cancel := context.WithCancel(context.Background())
		*transportWrapper, *err = makeTransportWrapper(backgroundCtx)
		if *err == nil {
			_ = cancel
		} else {
			cancel()
		}
	})
	if *err != nil {
		return nil, *err
	}
	transport = (*transportWrapper)(transport)
	return transport, nil
}

func MustWrapTransport(transport http.RoundTripper) http.RoundTripper {
	transport, err := WrapTransport(transport)
	if err != nil {
		panic(fmt.Sprintf("wrap transport: %v", err))
	}
	return transport
}

func makeTransportWrapper(backgroundCtx context.Context) (_ func(http.RoundTripper) http.RoundTripper, returnedErr error) {
	config, err := restclient.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("build config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("new clientset: %w", err)
	}
	endpointsRegistry := newEndpointsRegistry(backgroundCtx, clientset.CoreV1(), 1*time.Minute)
	currentNamespace, err := getCurrentNamespace()
	if err != nil {
		return nil, err
	}
	return func(transport http.RoundTripper) http.RoundTripper {
		return newKubeTransport(endpointsRegistry, currentNamespace, transport)
	}, nil
}

func getCurrentNamespace() (string, error) {
	data, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", fmt.Errorf("read file: %w", err)
	}
	currentNamespace := strings.TrimSpace(string(data))
	return currentNamespace, nil
}
