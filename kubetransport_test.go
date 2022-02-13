package kubetransport_test

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	. "github.com/go-tk/kubetransport"
	"github.com/go-tk/testcase"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestKubeTransport_RoundTrip(t *testing.T) {
	type Workspace struct {
		Init struct {
			EndpointsRegistry *EndpointsRegistry
			CurrentNamespace  string
			Transport         http.RoundTripper
		}
		In struct {
			Request *http.Request
		}
		ExpOut, ActOut struct {
			URL    *url.URL
			Err    error
			ErrStr string
		}
		Clientset *fakeclient.Clientset
		KT        *KubeTransport
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			backgroundCtx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			clientset := fakeclient.NewSimpleClientset()
			w.Init.EndpointsRegistry = NewEndpointsRegistry(backgroundCtx, clientset.CoreV1(), 1*time.Minute)
			w.Init.CurrentNamespace = "hello"
			w.Init.Transport = transportFunc(func(request *http.Request) (*http.Response, error) {
				w.ActOut.URL = request.URL
				return nil, nil
			})
			w.Clientset = clientset
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			w.KT = NewKubeTransport(w.Init.EndpointsRegistry, w.Init.CurrentNamespace, w.Init.Transport)
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			_, err := w.KT.RoundTrip(w.In.Request)
			if err != nil {
				w.ActOut.Err = err
				w.ActOut.ErrStr = err.Error()
			}
		}).
		Step(3, func(t *testing.T, w *Workspace) {
			assert.Equal(t, w.ExpOut, w.ActOut)
		})
	errSomethingWrong := errors.New("something wrong")
	testcase.RunListParallel(t,
		tc.Copy().
			Then("should succeed (1)").
			Step(1.5, func(t *testing.T, w *Workspace) {
				request, err := http.NewRequest("GET", "http://abc.com/", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Request = request
				w.ExpOut.URL = &url.URL{Scheme: "http", Host: "abc.com", Path: "/"}
			}),
		tc.Copy().
			Then("should succeed (2)").
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.Clientset.CoreV1().Endpoints("foo").Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
					Subsets: []v1.EndpointSubset{
						{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}}},
					},
				}, metav1.CreateOptions{})
				request, err := http.NewRequest("GET", "kube-http://bar.foo/", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Request = request
				w.ExpOut.URL = &url.URL{Scheme: "http", Host: "1.2.3.4", Path: "/"}
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
						{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}}},
					},
				}, metav1.CreateOptions{})
				request, err := http.NewRequest("GET", "kube-http://bar.foo.svc.cluster.local:2220/", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Request = request
				w.ExpOut.URL = &url.URL{Scheme: "http", Host: "1.2.3.4:2220", Path: "/"}
			}),
		tc.Copy().
			Given("endpoints not found (1)").
			Then("should fail").
			Step(1.5, func(t *testing.T, w *Workspace) {
				request, err := http.NewRequest("GET", "kube-http://world/", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Request = request
				w.ExpOut.ErrStr = "kubetransport: endpoints not found; namespace=\"hello\" endpointsName=\"world\""
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.Err
				if assert.ErrorIs(t, *err, ErrEndpointsNotFound) {
					*err = nil
				}
			}),
		tc.Copy().
			Given("endpoints not found (2)").
			Then("should fail").
			Step(1.5, func(t *testing.T, w *Workspace) {
				request, err := http.NewRequest("GET", "kube-http://bar.foo/", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Request = request
				w.ExpOut.ErrStr = "kubetransport: endpoints not found; namespace=\"foo\" endpointsName=\"bar\""
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.Err
				if assert.ErrorIs(t, *err, ErrEndpointsNotFound) {
					*err = nil
				}
			}),
		tc.Copy().
			Given("no ip address").
			Then("should fail").
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.Clientset.CoreV1().Endpoints("foo").Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
				}, metav1.CreateOptions{})
				request, err := http.NewRequest("GET", "kube-http://bar.foo/", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Request = request
				w.ExpOut.ErrStr = "kubetransport: no ip address; namespace=\"foo\" endpointsName=\"bar\""
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.Err
				if assert.ErrorIs(t, *err, ErrNoIPAddress) {
					*err = nil
				}
			}),
		tc.Copy().
			Given("getting ip addresses error").
			Then("should fail").
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.Clientset.PrependReactor("get", "endpoints", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, errSomethingWrong
				})
				request, err := http.NewRequest("GET", "kube-http://bar.foo.svc.cluster.local:2220/", nil)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Request = request
				w.ExpOut.ErrStr = "get ip addresses; hostname=\"bar.foo.svc.cluster.local\": get endpoints; namespace=\"foo\" endpointsName=\"bar\": something wrong"
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.Err
				if assert.ErrorIs(t, *err, errSomethingWrong) {
					*err = nil
				}
			}),
	)
}

type transportFunc func(*http.Request) (*http.Response, error)

var _ http.RoundTripper = transportFunc(nil)

func (tf transportFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return tf(request)
}
