package kubetransport_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
	"unsafe"

	. "github.com/go-tk/kubetransport"
	"github.com/go-tk/kubetransport/internal/k8sclient"
	mock_k8sclient "github.com/go-tk/kubetransport/internal/k8sclient/mock"
	"github.com/go-tk/testcase"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestKubeTransport_RoundTrip(t *testing.T) {
	type Workspace struct {
		Init struct {
			EndpointsRegistry *EndpointsRegistry
			TransportFunc     transportFunc
			Seed              uint64
		}
		In struct {
			Request *http.Request
		}
		ExpOut, ActOut struct {
			Response unsafe.Pointer
			Err      error
			ErrStr   string
		}

		MockK8sClient *mock_k8sclient.MockK8sClient
		KT            *KubeTransport
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			ctrl := gomock.NewController(t)
			w.MockK8sClient = mock_k8sclient.NewMockK8sClient(ctrl)
			w.Init.EndpointsRegistry = NewEndpointsRegistry(context.Background(), w.MockK8sClient, 24*time.Hour)
			t.Cleanup(w.Init.EndpointsRegistry.Stop)
			var response http.Response
			w.Init.TransportFunc = func(*http.Request) (*http.Response, error) { return &response, nil }
			w.Init.Seed = 100
			w.ExpOut.Response = unsafe.Pointer(&response)
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			w.KT = NewKubeTransport(w.Init.EndpointsRegistry, w.Init.TransportFunc, w.Init.Seed)
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			response, err := w.KT.RoundTrip(w.In.Request)
			w.ActOut.Response = unsafe.Pointer(response)
			if err != nil {
				w.ActOut.Err = err
				w.ActOut.ErrStr = err.Error()
			}
		}).
		Step(3, func(t *testing.T, w *Workspace) {
			if w.ExpOut.Err == nil || errors.Is(w.ActOut.Err, w.ExpOut.Err) {
				w.ExpOut.Err = w.ActOut.Err
			}
			assert.Equal(t, w.ExpOut, w.ActOut)
		})
	testcase.RunListParallel(t,
		tc.Copy().
			Step(1.5, func(t *testing.T, w *Workspace) {
				var err error
				w.In.Request, err = http.NewRequest("GET", "https://google.com", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.MockK8sClient.EXPECT().Namespace().Return("foo").Times(2)
				w.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("my-app")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return &k8sclient.Endpoints{
							Metadata: k8sclient.Metadata{
								ResourceVersion: "8910",
							},
							Subsets: []k8sclient.EndpointSubset{
								{
									Addresses: []k8sclient.EndpointAddress{
										{IP: "1.2.3.4"},
										{IP: "2.3.4.5"},
									},
								},
								{
									Addresses: []k8sclient.EndpointAddress{
										{IP: "7.7.7.7"},
										{IP: "8.8.8.8"},
									},
								},
							},
						}, nil
					})
				w.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("my-app"), gomock.Eq("8910"), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						<-ctx.Done()
						return ctx.Err()
					}).MinTimes(0)
				var response http.Response
				var n int
				w.Init.TransportFunc = func(request *http.Request) (*http.Response, error) {
					n++
					switch n {
					case 1:
						assert.Equal(t, "https://8.8.8.8/aa/bb", request.URL.String())
					case 2:
						assert.Equal(t, "https://1.2.3.4/aa/bb", request.URL.String())
					case 3:
						t.Fatal()
					}
					return &response, nil
				}
				w.Init.Seed = 99
				w.ExpOut.Response = unsafe.Pointer(&response)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				var err error
				w.In.Request, err = http.NewRequest("GET", "kube-https://my-app/aa/bb", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			}).
			Step(3.5, func(t *testing.T, w *Workspace) {
				if t.Failed() {
					return
				}
				request, err := http.NewRequest("GET", "kube-https://my-app/aa/bb", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.KT.RoundTrip(request)
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("test"), gomock.Eq("my-app")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return &k8sclient.Endpoints{
							Metadata: k8sclient.Metadata{
								ResourceVersion: "8910",
							},
							Subsets: []k8sclient.EndpointSubset{
								{
									Addresses: []k8sclient.EndpointAddress{
										{IP: "1.2.3.4"},
									},
								},
							},
						}, nil
					})
				w.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("test"), gomock.Eq("my-app"), gomock.Eq("8910"), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						<-ctx.Done()
						return ctx.Err()
					}).MinTimes(0)
				var response http.Response
				w.Init.TransportFunc = func(request *http.Request) (*http.Response, error) {
					assert.Equal(t, "https://1.2.3.4/aa/bb", request.URL.String())
					return &response, nil
				}
				w.ExpOut.Response = unsafe.Pointer(&response)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				var err error
				w.In.Request, err = http.NewRequest("GET", "kube-https://my-app.test/aa/bb", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("test"), gomock.Eq("my-app")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return &k8sclient.Endpoints{
							Metadata: k8sclient.Metadata{
								ResourceVersion: "8910",
							},
							Subsets: []k8sclient.EndpointSubset{
								{
									Addresses: []k8sclient.EndpointAddress{
										{IP: "1.2.3.4"},
									},
								},
							},
						}, nil
					})
				w.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("test"), gomock.Eq("my-app"), gomock.Eq("8910"), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						<-ctx.Done()
						return ctx.Err()
					}).MinTimes(0)
				var response http.Response
				w.Init.TransportFunc = func(request *http.Request) (*http.Response, error) {
					assert.Equal(t, "https://1.2.3.4:8888/aa/bb", request.URL.String())
					return &response, nil
				}
				w.ExpOut.Response = unsafe.Pointer(&response)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				var err error
				w.In.Request, err = http.NewRequest("GET", "kube-https://my-app.test.svc.cluster.local:8888/aa/bb", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("test"), gomock.Eq("my-app")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return nil, nil
					})
				w.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("test"), gomock.Eq("my-app"), gomock.Eq(""), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						<-ctx.Done()
						return ctx.Err()
					}).MinTimes(0)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				var err error
				w.In.Request, err = http.NewRequest("GET", "kube-https://my-app.test/aa/bb", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.ExpOut.Response = nil
				w.ExpOut.Err = ErrEndpointsNotFound
				w.ExpOut.ErrStr = "kubetransport: endpoints not found; namespace=\"test\" endpointsName=\"my-app\""
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("test"), gomock.Eq("my-app")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return &k8sclient.Endpoints{
							Metadata: k8sclient.Metadata{
								ResourceVersion: "8910",
							},
						}, nil
					})
				w.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("test"), gomock.Eq("my-app"), gomock.Eq("8910"), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						<-ctx.Done()
						return ctx.Err()
					}).MinTimes(0)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				var err error
				w.In.Request, err = http.NewRequest("GET", "kube-https://my-app.test/aa/bb", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.ExpOut.Response = nil
				w.ExpOut.Err = ErrNoIPAddress
				w.ExpOut.ErrStr = "kubetransport: no ip address; namespace=\"test\" endpointsName=\"my-app\""
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("test"), gomock.Eq("my-app")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return nil, context.DeadlineExceeded
					})
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				var err error
				w.In.Request, err = http.NewRequest("GET", "kube-https://my-app.test/aa/bb", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.ExpOut.Response = nil
				w.ExpOut.Err = context.DeadlineExceeded
				w.ExpOut.ErrStr = "get ip addresses; namespace=\"test\" endpointsName=\"my-app\": get endpoints; namespace=\"test\" endpointsName=\"my-app\": context deadline exceeded"
			}),
	)
}

type transportFunc func(*http.Request) (*http.Response, error)

var _ http.RoundTripper = transportFunc(nil)

func (tf transportFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return tf(request)
}
