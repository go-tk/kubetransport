package kubetransport_test

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/go-tk/kubetransport"
	"github.com/go-tk/testcase"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestEndpointsRegistry_GetIPAddresses(t *testing.T) {
	type Workspace struct {
		Init struct {
			TickPeriod time.Duration
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
		ExpSt, ActSt EndpointsRegistryState
		Ctx          context.Context
		Cancel       context.CancelFunc
		Clientset    *fakeclient.Clientset
		ER           *EndpointsRegistry
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			w.Init.TickPeriod = 1 * time.Minute
			w.In.Ctx = context.Background()
			w.Ctx, w.Cancel = context.WithCancel(context.Background())
			t.Cleanup(w.Cancel)
			w.Clientset = fakeclient.NewSimpleClientset()
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			w.ER = NewEndpointsRegistry(w.Ctx, w.Clientset.CoreV1(), w.Init.TickPeriod)
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			ipAddresses, err := w.ER.GetIPAddresses(w.In.Ctx, w.In.Namespace, w.In.EndpointsName)
			w.ActOut.IPAddresses = ipAddresses
			if err != nil {
				w.ActOut.Err = err
				w.ActOut.ErrStr = err.Error()
			}
		}).
		Step(3, func(t *testing.T, w *Workspace) {
			assert.Equal(t, w.ExpOut, w.ActOut)
		}).
		Step(4, func(t *testing.T, w *Workspace) {
			w.ActSt = w.ER.State()
		}).
		Step(5, func(t *testing.T, w *Workspace) {
			assert.Equal(t, w.ExpSt, w.ActSt)
		})
	errSomethingWrong := errors.New("something wrong")
	testcase.RunListParallel(t,
		tc.Copy().
			Then("should succeed (1.a)").
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpSt.IPAddressesCacheInfos = []IPAddressesCacheInfo{
					{
						HitCount: 1,
					},
				}
			}),
		tc.Copy().
			Then("should succeed (1.b)").
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.TickPeriod = 50 * time.Millisecond
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
			}).
			Step(3.5, func(t *testing.T, w *Workspace) {
				time.Sleep(200 * time.Millisecond)
			}),
		tc.Copy().
			Then("should succeed (2.a)").
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.Clientset.CoreV1().Endpoints("foo").Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
					Subsets: []v1.EndpointSubset{
						{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}, {IP: "2.3.4.5"}}},
						{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}, {IP: "4.5.6.7"}}},
					},
				}, metav1.CreateOptions{})
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.IPAddresses = []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"}
				w.ExpSt.IPAddressesCacheInfos = []IPAddressesCacheInfo{
					{
						Value:    []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"},
						HitCount: 1,
					},
				}
			}),
		tc.Copy().
			Then("should succeed (2.b)").
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.Clientset.CoreV1().Endpoints("foo").Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
					Subsets: []v1.EndpointSubset{
						{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}, {IP: "2.3.4.5"}}},
						{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}, {IP: "4.5.6.7"}}},
					},
				}, metav1.CreateOptions{})
				ipAddresses, err := w.ER.GetIPAddresses(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				assert.Equal(t, []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"}, ipAddresses)
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.IPAddresses = []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"}
				w.ExpSt.IPAddressesCacheInfos = []IPAddressesCacheInfo{
					{
						Value:    []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"},
						HitCount: 2,
					},
				}
			}),
		tc.Copy().
			Then("should succeed (3)").
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.Clientset.CoreV1().Endpoints("foo").Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
					Subsets: []v1.EndpointSubset{
						{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}, {IP: "2.3.4.5"}}},
						{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}, {IP: "4.5.6.7"}}},
					},
				}, metav1.CreateOptions{})
				ipAddresses, err := w.ER.GetIPAddresses(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				assert.Equal(t, []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"}, ipAddresses)
				w.Clientset.CoreV1().Endpoints("foo").Update(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version2",
					},
					Subsets: []v1.EndpointSubset{
						{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}}},
						{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}}},
					},
				}, metav1.UpdateOptions{})
				time.Sleep(100 * time.Millisecond)
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.IPAddresses = []string{"1.2.3.4", "3.4.5.6"}
				w.ExpSt.IPAddressesCacheInfos = []IPAddressesCacheInfo{
					{
						Value:    []string{"1.2.3.4", "3.4.5.6"},
						HitCount: 2,
					},
				}
			}),
		tc.Copy().
			Then("should succeed (4)").
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.Clientset.PrependWatchReactor("endpoints", func(action clienttesting.Action) (handled bool, ret watch.Interface, err error) {
					watch := watch.NewRaceFreeFake()
					go func() {
						watch.Error(&metav1.Status{
							Reason:  metav1.StatusReasonTooManyRequests,
							Message: "too many requests",
						})
					}()
					return true, watch, nil
				})
				ipAddresses, err := w.ER.GetIPAddresses(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				assert.Equal(t, []string(nil), ipAddresses)
				time.Sleep(100 * time.Millisecond)
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpSt.IPAddressesCacheInfos = []IPAddressesCacheInfo{
					{
						HitCount: 1,
					},
				}
			}),
		tc.Copy().
			Given("error while getting endpoints").
			Then("should fail").
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.ErrStr = "get endpoints; namespace=\"foo\" endpointsName=\"bar\": something wrong"
				w.Clientset.PrependReactor("get", "endpoints", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, errSomethingWrong
				})
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.Err
				if assert.ErrorIs(t, *err, errSomethingWrong) {
					*err = nil
				}
			}),
		tc.Copy().
			Given("context timeout").
			Then("should fail").
			Step(1.5, func(t *testing.T, w *Workspace) {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				_ = cancel
				w.In.Ctx = ctx
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.ErrStr = "context deadline exceeded"
				w.Clientset.PrependReactor("get", "endpoints", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
					time.Sleep(200 * time.Millisecond)
					return false, nil, nil
				})
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.Err
				if assert.ErrorIs(t, *err, context.DeadlineExceeded) {
					*err = nil
				}
			}),
	)
}
