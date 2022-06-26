package k8sclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/adammck/venv"
	"github.com/benbjohnson/clock"
	"github.com/spf13/afero"
)

type K8sClient interface {
	Namespace() (namespace string)
	GetEndpoints(ctx context.Context, namespace, endpointsName string) (endpoints *Endpoints, err error)
	WatchEndpoints(ctx context.Context, namespace, endpointsName, resourceVersion string, callback WatchEndpointsCallback) (err error)
}

type Metadata struct {
	ResourceVersion string `json:"resourceVersion"`
}

type Endpoints struct {
	Metadata Metadata         `json:"metadata"`
	Subsets  []EndpointSubset `json:"subsets"`
}

type EndpointSubset struct {
	Addresses []EndpointAddress `json:"addresses"`
}

type EndpointAddress struct {
	IP string `json:"ip"`
}

type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"

	eventError EventType = "ERROR"
)

type WatchEndpointsCallback func(eventType EventType, endpoints *Endpoints) (ok bool)

func New() (K8sClient, error) {
	return doNew(
		afero.NewOsFs(),
		dummyTransportReplacer,
		venv.OS(),
		clock.New(),
	)
}

func doNew(fs afero.Fs, transportReplacer transportReplacer, env venv.Env, clock clock.Clock) (*k8sClient, error) {
	var kc k8sClient
	kc.fs = fs
	kc.clock = clock
	transport, err := makeTransport(fs)
	if err != nil {
		return nil, err
	}
	kc.client.Transport = transportReplacer(transport)
	kc.serviceHostPort, err = getServiceHostPort(env)
	if err != nil {
		return nil, err
	}
	kc.namespace, err = getNamespace(fs)
	if err != nil {
		return nil, err
	}
	if _, err := kc.token.Get(clock, fs); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	}
	return &kc, nil
}

func dummyTransportReplacer(transport http.RoundTripper) http.RoundTripper { return transport }

const (
	serviceHostEnvVarName = "KUBERNETES_SERVICE_HOST"
	servicePortEnvVarName = "KUBERNETES_SERVICE_PORT"
	tokenFilePath         = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caCertFilePath        = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	namespaceFilePath     = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

const tokenRefreshInterval = 1 * time.Minute

type k8sClient struct {
	fs              afero.Fs
	clock           clock.Clock
	client          http.Client
	serviceHostPort string
	namespace       string
	token           token
}

type transportReplacer func(oldTransport http.RoundTripper) (newTransport http.RoundTripper)

func makeTransport(fs afero.Fs) (*http.Transport, error) {
	certPool := x509.NewCertPool()
	caCertData, err := afero.ReadFile(fs, caCertFilePath)
	if err != nil {
		return nil, fmt.Errorf("read ca certificate file; filePath=%q: %w", caCertFilePath, err)
	}
	if !certPool.AppendCertsFromPEM(caCertData) {
		return nil, errors.New("can't add ca certificate")
	}
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    certPool,
		},
	}, nil
}

func getServiceHostPort(env venv.Env) (string, error) {
	serviceHost, ok := env.LookupEnv(serviceHostEnvVarName)
	if !ok {
		return "", errors.New("can't find environment variable " + serviceHostEnvVarName)
	}
	servicePort, ok := env.LookupEnv(servicePortEnvVarName)
	if !ok {
		return "", errors.New("can't find environment variable " + servicePortEnvVarName)
	}
	return net.JoinHostPort(serviceHost, servicePort), nil
}

func getNamespace(fs afero.Fs) (string, error) {
	namespaceData, err := afero.ReadFile(fs, namespaceFilePath)
	if err != nil {
		return "", fmt.Errorf("read namespace file; filePath=%q: %w", namespaceFilePath, err)
	}
	return string(namespaceData), nil
}

func (kc *k8sClient) Namespace() string { return kc.namespace }

func (kc *k8sClient) GetEndpoints(ctx context.Context, namespace, endpointsName string) (*Endpoints, error) {
	url := kc.makeURL("/api/v1/namespaces/%s/endpoints/%s", namespace, endpointsName)
	response, err := kc.doGetRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("get %q; statusCode=%v", url, response.StatusCode)
	}
	var endpoints Endpoints
	if err := json.NewDecoder(response.Body).Decode(&endpoints); err != nil {
		return nil, fmt.Errorf("decode endpoints json: %w", err)
	}
	return &endpoints, nil
}

func (kc *k8sClient) WatchEndpoints(ctx context.Context, namespace, endpointsName, resourceVersion string, callback WatchEndpointsCallback) error {
	err := kc.doWatchEndpoints(ctx, namespace, endpointsName, resourceVersion, callback)
	if status := (*status)(nil); errors.As(err, &status) && status.Code == http.StatusGone && resourceVersion != "" {
		err = kc.doWatchEndpoints(ctx, namespace, endpointsName, "", func(eventType EventType, endpoints *Endpoints) bool {
			if endpoints != nil && endpoints.Metadata.ResourceVersion == resourceVersion {
				return true
			}
			return callback(eventType, endpoints)
		})
	}
	return err
}

func (kc *k8sClient) doWatchEndpoints(ctx context.Context, namespace, endpointsName, resourceVersion string, callback WatchEndpointsCallback) error {
	var url string
	if resourceVersion == "" {
		url = kc.makeURL("/api/v1/watch/namespaces/%s/endpoints/%s", namespace, endpointsName)
	} else {
		url = kc.makeURL("/api/v1/watch/namespaces/%s/endpoints/%s?resourceVersion=%s", namespace, endpointsName, resourceVersion)
	}
	response, err := kc.doGetRequest(ctx, url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("get %q; statusCode=%v", url, response.StatusCode)
	}
	decoder := json.NewDecoder(response.Body)
	for {
		var endpoints *Endpoints
		event := event{
			Object: &endpoints,
		}
		if err := decoder.Decode(&event); err != nil {
			return fmt.Errorf("decode event json: %w", err)
		}
		if event.Type == eventError {
			return fmt.Errorf("receive error event: %w", event.Object.(*status))
		}
		if !callback(event.Type, endpoints) {
			return nil
		}
	}
}

func (kc *k8sClient) makeURL(urlPathTemplate string, args ...interface{}) string {
	return fmt.Sprintf("https://"+kc.serviceHostPort+urlPathTemplate, args...)
}

func (kc *k8sClient) doGetRequest(ctx context.Context, url string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("new get request; url=%q: %w", url, err)
	}
	token, err := kc.token.Get(kc.clock, kc.fs)
	if err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	}
	request.Header["Authorization"] = []string{"Bearer " + token}
	response, err := kc.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", url, err)
	}
	if response.StatusCode == http.StatusUnauthorized {
		kc.token.Reset()
	}
	return response, nil
}

type token struct {
	lock   sync.Mutex
	state1 unsafe.Pointer
}

type tokenState struct {
	Value           string
	NextRefreshTime time.Time
}

func (t *token) Get(clock clock.Clock, fs afero.Fs) (string, error) {
	now := clock.Now()
	if state := t.state(); state != nil && state.NextRefreshTime.After(now) {
		return state.Value, nil
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	if state := t.state(); state != nil && state.NextRefreshTime.After(now) {
		return state.Value, nil
	}
	state, err := t.newState(fs, now)
	if err != nil {
		return "", err
	}
	t.setState(state)
	return state.Value, nil
}

func (t *token) Reset() { t.setState(nil) }

func (t *token) newState(fs afero.Fs, now time.Time) (*tokenState, error) {
	tokenData, err := afero.ReadFile(fs, tokenFilePath)
	if err != nil {
		return nil, fmt.Errorf("read token file; filePath=%q: %w", tokenFilePath, err)
	}
	return &tokenState{
		Value:           string(tokenData),
		NextRefreshTime: now.Add(tokenRefreshInterval),
	}, nil
}

func (t *token) state() *tokenState         { return (*tokenState)(atomic.LoadPointer(&t.state1)) }
func (t *token) setState(state *tokenState) { atomic.StorePointer(&t.state1, unsafe.Pointer(state)) }

type event struct {
	Type   EventType
	Object interface{}
}

var _ json.Unmarshaler = (*event)(nil)

type status struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

var _ error = (*status)(nil)

func (s *status) Error() string { return s.Message }

func (e *event) UnmarshalJSON(data []byte) error {
	rawEvent := struct {
		Type   EventType       `json:"type"`
		Object json.RawMessage `json:"object"`
	}{}
	if err := json.Unmarshal(data, &rawEvent); err != nil {
		return err
	}
	e.Type = rawEvent.Type
	if e.Type == eventError {
		e.Object = &status{}
	}
	return json.Unmarshal(rawEvent.Object, e.Object)
}
