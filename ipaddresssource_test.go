package kubetransport_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-tk/testcase"
	. "github.com/roy2220/kubetransport"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestIPAddressSource_GetValuesAndSetWatch(t *testing.T) {
	type Results struct {
		Values []string
		Err    error
		ErrStr string
	}
	type Workspace struct {
		Init struct {
			Namespace     string
			EndpointsName string
		}
		ExpOut, ActOut struct {
			ResultsList []Results
		}
		Ctx       context.Context
		Cancel    context.CancelFunc
		Clientset *fakeclient.Clientset
		IPAS      *IPAddressSource
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			w.Ctx, w.Cancel = context.WithCancel(context.Background())
			t.Cleanup(func() {
				c := w.Cancel
				w.Cancel = nil
				c()
			})
			w.Clientset = fakeclient.NewSimpleClientset()
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			w.IPAS = NewIPAddressSource(w.Ctx, w.Clientset.CoreV1(), w.Init.Namespace, w.Init.EndpointsName)
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			var wg sync.WaitGroup
			wg.Add(len(w.ExpOut.ResultsList))
			w.IPAS.GetValuesAndSetWatch(func(values []string, err error) {
				if w.Cancel == nil {
					return
				}
				var errStr string
				if err != nil {
					errStr = err.Error()
				}
				results := Results{values, err, errStr}
				w.ActOut.ResultsList = append(w.ActOut.ResultsList, results)
				wg.Done()
			})
			wg.Wait()
		}).
		Step(3, func(t *testing.T, w *Workspace) {
			assert.Equal(t, w.ExpOut.ResultsList, w.ActOut.ResultsList)
		})
	errSomethingWrong := errors.New("something wrong")
	testcase.RunListParallel(t,
		tc.Copy().
			Given("error while getting endpoints").
			Then("should fail").
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Clientset.PrependReactor("get", "endpoints", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
					assert.Equal(t, "foo", action.GetNamespace())
					getAction := action.(clienttesting.GetAction)
					assert.Equal(t, "bar", getAction.GetName())
					return true, nil, errSomethingWrong
				})
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.ExpOut.ResultsList = []Results{
					{ErrStr: "get endpoints; namespace=\"foo\" endpointsName=\"bar\": something wrong"},
				}
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.ResultsList[0].Err
				if assert.ErrorIs(t, *err, errSomethingWrong) {
					*err = nil
				}
			}),
		tc.Copy().
			Given("error while watching endpoints (1)").
			Then("should fail").
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Clientset.CoreV1().Endpoints("foo").Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
				}, metav1.CreateOptions{})
				w.Clientset.PrependWatchReactor("endpoints", func(action clienttesting.Action) (handled bool, ret watch.Interface, err error) {
					assert.Equal(t, "foo", action.GetNamespace())
					watchRestrictions := action.(clienttesting.WatchAction).GetWatchRestrictions()
					assert.Equal(t, "version1", watchRestrictions.ResourceVersion)
					assert.Equal(t, "metadata.name=bar", watchRestrictions.Fields.String())
					return true, nil, errSomethingWrong
				})
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.ExpOut.ResultsList = []Results{
					{ErrStr: "watch endpoints; namespace=\"foo\" endpointsName=\"bar\": something wrong"},
				}
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.ResultsList[0].Err
				if assert.ErrorIs(t, *err, errSomethingWrong) {
					*err = nil
				}
			}),
		tc.Copy().
			Given("error while watching endpoints (2)").
			Then("should fail").
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Clientset.CoreV1().Endpoints("foo").Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
				}, metav1.CreateOptions{})
				var n int
				w.Clientset.PrependWatchReactor("endpoints", func(action clienttesting.Action) (handled bool, ret watch.Interface, err error) {
					assert.Equal(t, "foo", action.GetNamespace())
					watchRestrictions := action.(clienttesting.WatchAction).GetWatchRestrictions()
					assert.Equal(t, "metadata.name=bar", watchRestrictions.Fields.String())
					i := n
					n++
					switch i {
					case 0:
						assert.Equal(t, "version1", watchRestrictions.ResourceVersion)
						return true, nil, &apierrors.StatusError{ErrStatus: metav1.Status{
							Reason:  metav1.StatusReasonGone,
							Message: "gone",
						}}
					case 1:
						assert.Equal(t, "", watchRestrictions.ResourceVersion)
						return true, nil, errSomethingWrong
					default:
						panic("unreachable code")
					}
				})
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.ExpOut.ResultsList = []Results{
					{ErrStr: "watch endpoints; namespace=\"foo\" endpointsName=\"bar\": something wrong"},
				}
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.ResultsList[0].Err
				if assert.ErrorIs(t, *err, errSomethingWrong) {
					*err = nil
				}
			}),
		tc.Copy().
			Given("result chan closed").
			Then("should fail").
			Step(0.5, func(t *testing.T, w *Workspace) {
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
				w.Clientset.PrependWatchReactor("endpoints", func(action clienttesting.Action) (handled bool, ret watch.Interface, err error) {
					go func() {
						time.Sleep(100 * time.Millisecond)
						w.IPAS.ClearWatch()
					}()
					return false, nil, nil
				})
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.ExpOut.ResultsList = []Results{
					{Values: []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"}},
					{ErrStr: "kubetransport: watch cleared; namespace=\"foo\" endpointsName=\"bar\""},
				}
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.ResultsList[1].Err
				if assert.ErrorIs(t, *err, ErrWatchCleared) {
					*err = nil
				}
			}),
		tc.Copy().
			Given("background context cancelled").
			Then("should fail").
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.Cancel()
				w.ExpOut.ResultsList = []Results{
					{},
					{Err: context.Canceled, ErrStr: context.Canceled.Error()},
				}
			}),
		tc.Copy().
			Given("result chan returning error").
			Then("should fail").
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Clientset.PrependWatchReactor("endpoints", func(action clienttesting.Action) (handled bool, ret watch.Interface, err error) {
					watch := watch.NewRaceFreeFake()
					go func() {
						time.Sleep(100 * time.Millisecond)
						watch.Error(&metav1.Status{
							Reason:  metav1.StatusReasonTooManyRequests,
							Message: "too many requests",
						})
					}()
					return true, watch, nil
				})
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.ExpOut.ResultsList = []Results{
					{},
					{ErrStr: "watch endpoints; namespace=\"foo\" endpointsName=\"bar\": too many requests"},
				}
			}).
			Step(2.5, func(t *testing.T, w *Workspace) {
				err := &w.ActOut.ResultsList[1].Err
				if assert.True(t, apierrors.IsTooManyRequests(*err)) {
					*err = nil
				}
			}),
		tc.Copy().
			Then("should succeed (1)").
			Step(0.5, func(t *testing.T, w *Workspace) {
				endpointsInterface := w.Clientset.CoreV1().Endpoints("foo")
				endpointsInterface.Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
					Subsets: []v1.EndpointSubset{
						{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}, {IP: "2.3.4.5"}}},
						{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}, {IP: "4.5.6.7"}}},
					},
				}, metav1.CreateOptions{})
				go func() {
					time.Sleep(100 * time.Millisecond)
					endpointsInterface.Update(context.Background(), &v1.Endpoints{
						ObjectMeta: metav1.ObjectMeta{
							Name:            "bar",
							ResourceVersion: "version1",
						},
						Subsets: []v1.EndpointSubset{
							{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}}},
							{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}}},
						},
					}, metav1.UpdateOptions{})
				}()
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.ExpOut.ResultsList = []Results{
					{Values: []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"}},
					{Values: []string{"1.2.3.4", "3.4.5.6"}},
				}
			}),
		tc.Copy().
			Then("should succeed (2)").
			Step(0.5, func(t *testing.T, w *Workspace) {
				endpointsInterface := w.Clientset.CoreV1().Endpoints("foo")
				endpointsInterface.Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
					Subsets: []v1.EndpointSubset{
						{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}, {IP: "2.3.4.5"}}},
						{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}, {IP: "4.5.6.7"}}},
					},
				}, metav1.CreateOptions{})
				go func() {
					time.Sleep(100 * time.Millisecond)
					endpointsInterface.Delete(context.Background(), "bar", metav1.DeleteOptions{})
				}()
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.ExpOut.ResultsList = []Results{
					{Values: []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"}},
					{Values: nil},
				}
			}),
		tc.Copy().
			Then("should succeed (3)").
			Step(0.5, func(t *testing.T, w *Workspace) {
				endpointsInterface := w.Clientset.CoreV1().Endpoints("foo")
				go func() {
					time.Sleep(100 * time.Millisecond)
					endpointsInterface.Create(context.Background(), &v1.Endpoints{
						ObjectMeta: metav1.ObjectMeta{
							Name:            "bar",
							ResourceVersion: "version1",
						},
						Subsets: []v1.EndpointSubset{
							{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}, {IP: "2.3.4.5"}}},
							{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}, {IP: "4.5.6.7"}}},
						},
					}, metav1.CreateOptions{})
				}()
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.ExpOut.ResultsList = []Results{
					{},
					{Values: []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"}},
				}
			}),
		tc.Copy().
			Then("should succeed (4)").
			Step(0.5, func(t *testing.T, w *Workspace) {
				endpointsInterface := w.Clientset.CoreV1().Endpoints("foo")
				endpointsInterface.Create(context.Background(), &v1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "bar",
						ResourceVersion: "version1",
					},
					Subsets: []v1.EndpointSubset{
						{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}, {IP: "2.3.4.5"}}},
						{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}, {IP: "4.5.6.7"}}},
					},
				}, metav1.CreateOptions{})
				var n int
				w.Clientset.PrependWatchReactor("endpoints", func(action clienttesting.Action) (handled bool, ret watch.Interface, err error) {
					assert.Equal(t, "foo", action.GetNamespace())
					watchRestrictions := action.(clienttesting.WatchAction).GetWatchRestrictions()
					assert.Equal(t, "metadata.name=bar", watchRestrictions.Fields.String())
					i := n
					n++
					switch i {
					case 0:
						assert.Equal(t, "version1", watchRestrictions.ResourceVersion)
						return true, nil, &apierrors.StatusError{ErrStatus: metav1.Status{
							Reason:  metav1.StatusReasonGone,
							Message: "gone",
						}}
					case 1:
						assert.Equal(t, "", watchRestrictions.ResourceVersion)
						watch := watch.NewRaceFreeFake()
						go func() {
							time.Sleep(100 * time.Millisecond)
							watch.Add(&v1.Endpoints{
								ObjectMeta: metav1.ObjectMeta{
									Name:            "bar",
									ResourceVersion: "version1",
								},
								Subsets: []v1.EndpointSubset{
									// {Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}, {IP: "2.3.4.5"}}},
									// {Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}, {IP: "4.5.6.7"}}},
								},
							})
							watch.Modify(&v1.Endpoints{
								ObjectMeta: metav1.ObjectMeta{
									Name:            "bar",
									ResourceVersion: "version2",
								},
								Subsets: []v1.EndpointSubset{
									{Addresses: []v1.EndpointAddress{{IP: "1.2.3.4"}}},
									{Addresses: []v1.EndpointAddress{{IP: "3.4.5.6"}}},
								},
							})
						}()
						return true, watch, nil
					default:
						panic("unreachable code")
					}
				})
				w.Init.Namespace = "foo"
				w.Init.EndpointsName = "bar"
				w.ExpOut.ResultsList = []Results{
					{Values: []string{"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"}},
					{Values: []string{"1.2.3.4", "3.4.5.6"}},
				}
			}),
	)
}
