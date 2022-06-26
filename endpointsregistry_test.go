package kubetransport_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/go-tk/kubetransport"
	"github.com/go-tk/kubetransport/internal/k8sclient"
	mock_k8sclient "github.com/go-tk/kubetransport/internal/k8sclient/mock"
	"github.com/go-tk/testcase"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestEndpointsRegistry_GetIPAddresses(t *testing.T) {
	type Workspace struct {
		Init struct {
			BackgroundCtx context.Context
			MockK8sClient *mock_k8sclient.MockK8sClient
			TickInterval  time.Duration
		}
		In struct {
			Ctx           context.Context
			Namespace     string
			EndpointsName string
		}
		ExpOut, ActOut struct {
			IPAddresses []string
			Err         error
			ErrStr      string
		}

		ER *EndpointsRegistry
		WG sync.WaitGroup
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			w.Init.BackgroundCtx = context.Background()
			ctrl := gomock.NewController(t)
			w.Init.MockK8sClient = mock_k8sclient.NewMockK8sClient(ctrl)
			w.Init.TickInterval = 24 * time.Hour
			w.In.Ctx = context.Background()
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			w.ER = NewEndpointsRegistry(w.Init.BackgroundCtx, w.Init.MockK8sClient, w.Init.TickInterval)
			t.Cleanup(w.ER.Close)
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			w.ActOut.IPAddresses, w.ActOut.Err = w.ER.GetIPAddresses(w.In.Ctx, w.In.Namespace, w.In.EndpointsName)
			if w.ActOut.Err != nil {
				w.ActOut.ErrStr = w.ActOut.Err.Error()
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
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return nil, nil
					})
				w.Init.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar"), gomock.Eq(""), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						<-ctx.Done()
						return ctx.Err()
					}).MinTimes(0)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
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
						<-ctx.Done()
						return ctx.Err()
					}).MinTimes(0)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.IPAddresses = []string{"1.2.3.4", "2.3.4.5", "7.7.7.7", "8.8.8.8"}
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockK8sClient.EXPECT().Namespace().Return("foo")
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
				w.WG.Add(1)
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
						w.WG.Done()
						<-ctx.Done()
						return ctx.Err()
					})
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				ipAddresses, err := w.ER.GetIPAddresses(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.Equal(t, []string{"1.2.3.4", "2.3.4.5", "7.7.7.7", "8.8.8.8"}, ipAddresses) {
					t.FailNow()
				}
				w.WG.Wait()
				w.In.Namespace = ""
				w.In.EndpointsName = "bar"
				w.ExpOut.IPAddresses = []string{"1.2.3.4", "8.8.8.8", "9.9.9.9"}
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.TickInterval = time.Second / 4
				w.Init.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return nil, nil
					})
				w.Init.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar"), gomock.Eq(""), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						<-ctx.Done()
						return ctx.Err()
					})
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
									},
								},
							},
						}, nil
					})
				w.Init.MockK8sClient.EXPECT().WatchEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar"), gomock.Eq("8910"), gomock.Any()).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName, resourceVersion string, callback k8sclient.WatchEndpointsCallback) error {
						<-ctx.Done()
						return ctx.Err()
					}).MinTimes(0)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				ipAddresses, err := w.ER.GetIPAddresses(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.Equal(t, []string(nil), ipAddresses) {
					t.FailNow()
				}
				time.Sleep(time.Second)
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.IPAddresses = []string{"1.2.3.4"}
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						return nil, context.Canceled
					})
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.Err = context.Canceled
				w.ExpOut.ErrStr = "get endpoints; namespace=\"foo\" endpointsName=\"bar\": context canceled"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockK8sClient.EXPECT().GetEndpoints(gomock.Any(), gomock.Eq("foo"), gomock.Eq("bar")).
					DoAndReturn(func(ctx context.Context, namespace, endpointsName string) (*k8sclient.Endpoints, error) {
						ctx.Done()
						return nil, ctx.Err()
					}).MinTimes(0)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				_ = cancel
				w.In.Ctx = ctx
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.Err = context.DeadlineExceeded
				w.ExpOut.ErrStr = context.DeadlineExceeded.Error()
			}),
	)
}
