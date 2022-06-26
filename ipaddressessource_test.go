package kubetransport_test

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	. "github.com/go-tk/kubetransport"
	"github.com/go-tk/kubetransport/internal/k8sclient"
	mock_k8sclient "github.com/go-tk/kubetransport/internal/k8sclient/mock"
	"github.com/go-tk/testcase"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestNewIPAddressesSource(t *testing.T) {
	type CallbackArgs struct {
		IPAddressesSource unsafe.Pointer
		IPAddresses       []string
		Err               error
		ErrStr            string
	}
	type Workspace struct {
		Init struct {
			BackgroundCtx context.Context
			MockK8sClient *mock_k8sclient.MockK8sClient
			Namespace     string
			EndpointsName string
		}
		ExpOut, ActOut struct {
			CAs []CallbackArgs
		}

		CAs  chan CallbackArgs
		IPAS *IPAddressesSource
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			w.Init.BackgroundCtx = context.Background()
			ctrl := gomock.NewController(t)
			w.Init.MockK8sClient = mock_k8sclient.NewMockK8sClient(ctrl)
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			ipAddressesCallback := func(ipAddressesSource *IPAddressesSource, ipAddresses []string, err error) {
				var errStr string
				if err != nil {
					errStr = err.Error()
				}
				w.CAs <- CallbackArgs{
					IPAddressesSource: unsafe.Pointer(ipAddressesSource),
					IPAddresses:       ipAddresses,
					Err:               err,
					ErrStr:            errStr,
				}
				if err != nil {
					close(w.CAs)
				}
			}
			w.CAs = make(chan CallbackArgs)
			w.IPAS = NewIPAddressesSource(w.Init.BackgroundCtx, w.Init.MockK8sClient, w.Init.Namespace, w.Init.EndpointsName, ipAddressesCallback)
			t.Cleanup(w.IPAS.Close)
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			for ca := range w.CAs {
				w.ActOut.CAs = append(w.ActOut.CAs, ca)
			}
			if !assert.Len(t, w.ActOut.CAs, len(w.ExpOut.CAs)) {
				t.FailNow()
			}
			for i := range w.ExpOut.CAs {
				expCA := &w.ExpOut.CAs[i]
				actCA := &w.ActOut.CAs[i]
				expCA.IPAddressesSource = unsafe.Pointer(w.IPAS)
				if expCA.Err == nil || errors.Is(actCA.Err, expCA.Err) {
					expCA.Err = actCA.Err
				}
			}
			assert.Equal(t, w.ExpOut, w.ActOut)
		})
	testcase.RunListParallel(t,
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.Init.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return nil, nil
					})
				w.Init.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar"), gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						callback(k8sclient.EventAdded, &k8sclient.Endpoints{
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
						})
						callback(k8sclient.EventModified, &k8sclient.Endpoints{
							Subsets: []k8sclient.EndpointSubset{
								{
									Addresses: []k8sclient.EndpointAddress{
										{IP: "1.2.3.4"},
									},
								},
								{
									Addresses: []k8sclient.EndpointAddress{
										{IP: "8.8.8.8"},
										{IP: "9.9.9.9"},
									},
								},
							},
						})
						<-ctx.Done()
						return ctx.Err()
					})
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.IPAS.Close()
				w.ExpOut.CAs = []CallbackArgs{
					{},
					{
						IPAddresses: []string{"1.2.3.4", "2.3.4.5", "7.7.7.7", "8.8.8.8"},
					},
					{
						IPAddresses: []string{"1.2.3.4", "8.8.8.8", "9.9.9.9"},
					},
					{
						Err:    context.Canceled,
						ErrStr: "watch endpoints; namespace=\"foo\" endpointsName=\"bar\": context canceled",
					},
				}
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.Init.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar")).
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
				w.Init.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar"), gomock.Eq("8910"), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						callback(k8sclient.EventAdded, &k8sclient.Endpoints{
							Subsets: []k8sclient.EndpointSubset{
								{
									Addresses: []k8sclient.EndpointAddress{
										{IP: "1.2.3.4"},
									},
								},
								{
									Addresses: []k8sclient.EndpointAddress{
										{IP: "8.8.8.8"},
										{IP: "9.9.9.9"},
									},
								},
							},
						})
						callback(k8sclient.EventModified, &k8sclient.Endpoints{})
						callback(k8sclient.EventDeleted, &k8sclient.Endpoints{})
						<-ctx.Done()
						return ctx.Err()
					})
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.IPAS.Close()
				w.ExpOut.CAs = []CallbackArgs{
					{
						IPAddresses: []string{"1.2.3.4", "2.3.4.5", "7.7.7.7", "8.8.8.8"},
					},
					{
						IPAddresses: []string{"1.2.3.4", "8.8.8.8", "9.9.9.9"},
					},
					{
						IPAddresses: []string{},
					},
					{},
					{
						Err:    context.Canceled,
						ErrStr: "watch endpoints; namespace=\"foo\" endpointsName=\"bar\": context canceled",
					},
				}
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				_ = cancel
				w.Init.BackgroundCtx = ctx
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.Init.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					})
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.ExpOut.CAs = []CallbackArgs{
					{
						Err:    context.DeadlineExceeded,
						ErrStr: "get endpoints; namespace=\"foo\" endpointsName=\"bar\": context deadline exceeded",
					},
				}
			}),
	)
}
