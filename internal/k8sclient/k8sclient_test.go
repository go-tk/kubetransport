package k8sclient_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/adammck/venv"
	"github.com/benbjohnson/clock"
	. "github.com/go-tk/kubetransport/internal/k8sclient"
	"github.com/go-tk/vk8s"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func TestK8sClient(t *testing.T) {
	te := prepareTestingEnvironment(t)
	t.Run("GetEndpoints", func(t *testing.T) { testK8sClient_GetEndpoints(t, te) })
	t.Run("WatchEndpoints", func(t *testing.T) { testK8sClient_WatchEndpoints(t, te) })
}

func testK8sClient_GetEndpoints(t *testing.T, te *testingEnvironment) {
	kc, err := DoNew(te.Fs, te.Env, te.Clock)
	if err != nil {
		t.Fatal(err)
	}

	{
		endpoints, err := kc.GetEndpoints(context.Background(), "", "abc")
		if err != nil {
			t.Fatal(err)
		}
		if !assert.Nil(t, endpoints) {
			t.FailNow()
		}
		endpoints, err = kc.GetEndpoints(context.Background(), "default", "abc")
		if err != nil {
			t.Fatal(err)
		}
		if !assert.Nil(t, endpoints) {
			t.FailNow()
		}
	}

	{
		endpoints, err := te.Clientset.CoreV1().Endpoints("default").Create(
			context.Background(),
			&corev1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name: "abc",
				},
				Subsets: []corev1.EndpointSubset{
					{
						Addresses: []corev1.EndpointAddress{
							{IP: "1.2.3.4"},
							{IP: "3.4.5.6"},
						},
					},
					{
						Addresses: []corev1.EndpointAddress{
							{IP: "6.6.6.6"},
							{IP: "7.7.7.7"},
						},
					},
				},
			},
			metav1.CreateOptions{},
		)
		if err != nil {
			t.Fatal(err)
		}
		endpoints1, err := kc.GetEndpoints(context.Background(), "", "abc")
		if err != nil {
			t.Fatal(err)
		}
		endpoints2, err := kc.GetEndpoints(context.Background(), "default", "abc")
		if err != nil {
			t.Fatal(err)
		}
		if !assert.Equal(t, endpoints1, endpoints2) {
			t.FailNow()
		}
		if !assert.Equal(t, &Endpoints{
			Metadata: Metadata{
				ResourceVersion: endpoints.ResourceVersion,
			},
			Subsets: []EndpointSubset{
				{
					Addresses: []EndpointAddress{
						{IP: "1.2.3.4"},
						{IP: "3.4.5.6"},
					},
				},
				{
					Addresses: []EndpointAddress{
						{IP: "6.6.6.6"},
						{IP: "7.7.7.7"},
					},
				},
			},
		}, endpoints1) {
			t.FailNow()
		}
	}

	{
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := kc.GetEndpoints(ctx, "", "abcd")
		if !assert.ErrorIs(t, err, context.Canceled) {
			t.FailNow()
		}
	}
}

func testK8sClient_WatchEndpoints(t *testing.T, te *testingEnvironment) {
	kc, err := DoNew(te.Fs, te.Env, te.Clock)
	if err != nil {
		t.Fatal(err)
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = cancel
		var f bool
		err := kc.WatchEndpoints(ctx, "default", "xyz", "", func(EventType, *Endpoints) bool {
			f = true
			return true
		})
		if !assert.ErrorIs(t, err, context.DeadlineExceeded) {
			t.FailNow()
		}
		if !assert.False(t, f) {
			t.FailNow()
		}
	}

	{
		go func() {
			time.Sleep(time.Second)
			if _, err := te.Clientset.CoreV1().Endpoints("default").Create(
				context.Background(),
				&corev1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name: "xyz",
					},
					Subsets: []corev1.EndpointSubset{
						{
							Addresses: []corev1.EndpointAddress{
								{IP: "1.2.3.4"},
								{IP: "3.4.5.6"},
							},
						},
						{
							Addresses: []corev1.EndpointAddress{
								{IP: "6.6.6.6"},
								{IP: "7.7.7.7"},
							},
						},
					},
				},
				metav1.CreateOptions{},
			); err != nil {
				t.Error(err)
				return
			}
			if _, err := te.Clientset.CoreV1().Endpoints("default").Update(
				context.Background(),
				&corev1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name: "xyz",
					},
					Subsets: []corev1.EndpointSubset{
						{
							Addresses: []corev1.EndpointAddress{
								{IP: "1.1.1.1"},
							},
						},
						{
							Addresses: []corev1.EndpointAddress{
								{IP: "2.2.2.2"},
							},
						},
					},
				},
				metav1.UpdateOptions{},
			); err != nil {
				t.Error(err)
				return
			}
			if err := te.Clientset.CoreV1().Endpoints("default").Delete(
				context.Background(),
				"xyz",
				metav1.DeleteOptions{},
			); err != nil {
				t.Error(err)
				return
			}
			if _, err := te.Clientset.CoreV1().Endpoints("default").Create(
				context.Background(),
				&corev1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name: "xyz",
					},
					Subsets: []corev1.EndpointSubset{
						{
							Addresses: []corev1.EndpointAddress{
								{IP: "10.10.10.10"},
							},
						},
					},
				},
				metav1.CreateOptions{},
			); err != nil {
				t.Error(err)
				return
			}
		}()
		n := 0
		err := kc.WatchEndpoints(context.Background(), "default", "xyz", "", func(eventType EventType, endpoints *Endpoints) bool {
			n++
			var expectedEventType EventType
			var expectedEndpoints *Endpoints
			switch n {
			case 1:
				expectedEventType = EventAdded
				expectedEndpoints = &Endpoints{
					Metadata: Metadata{
						ResourceVersion: endpoints.Metadata.ResourceVersion,
					},
					Subsets: []EndpointSubset{
						{
							Addresses: []EndpointAddress{
								{IP: "1.2.3.4"},
								{IP: "3.4.5.6"},
							},
						},
						{
							Addresses: []EndpointAddress{
								{IP: "6.6.6.6"},
								{IP: "7.7.7.7"},
							},
						},
					},
				}
			case 2:
				expectedEventType = EventModified
				expectedEndpoints = &Endpoints{
					Metadata: Metadata{
						ResourceVersion: endpoints.Metadata.ResourceVersion,
					},
					Subsets: []EndpointSubset{
						{
							Addresses: []EndpointAddress{
								{IP: "1.1.1.1"},
							},
						},
						{
							Addresses: []EndpointAddress{
								{IP: "2.2.2.2"},
							},
						},
					},
				}
			case 3:
				expectedEventType = EventDeleted
				expectedEndpoints = &Endpoints{
					Metadata: Metadata{
						ResourceVersion: endpoints.Metadata.ResourceVersion,
					},
					Subsets: []EndpointSubset{
						{
							Addresses: []EndpointAddress{
								{IP: "1.1.1.1"},
							},
						},
						{
							Addresses: []EndpointAddress{
								{IP: "2.2.2.2"},
							},
						},
					},
				}
			case 4:
				expectedEventType = EventAdded
				expectedEndpoints = &Endpoints{
					Metadata: Metadata{
						ResourceVersion: endpoints.Metadata.ResourceVersion,
					},
					Subsets: []EndpointSubset{
						{
							Addresses: []EndpointAddress{
								{IP: "10.10.10.10"},
							},
						},
					},
				}
			}
			if !assert.Equal(t, expectedEventType, eventType) {
				t.FailNow()
			}
			if !assert.Equal(t, expectedEndpoints, endpoints) {
				t.FailNow()
			}
			return n < 4
		})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}

	{
		if _, err := te.Clientset.CoreV1().Endpoints("default").Create(
			context.Background(),
			&corev1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ijk",
				},
				Subsets: []corev1.EndpointSubset{
					{
						Addresses: []corev1.EndpointAddress{
							{IP: "10.10.10.10"},
						},
					},
				},
			},
			metav1.CreateOptions{},
		); err != nil {
			t.Error(err)
			return
		}
		endpoints, err := kc.GetEndpoints(context.Background(), "", "ijk")
		if err != nil {
			t.Fatal(err)
		}
		if !assert.NotNil(t, endpoints) {
			t.FailNow()
		}
		err = kc.WatchEndpoints(context.Background(), "default", "ijk", "", func(eventType EventType, endpoints *Endpoints) bool {
			if !assert.Equal(t, EventAdded, eventType) {
				t.FailNow()
			}
			if !assert.Equal(t, &Endpoints{
				Metadata: Metadata{
					ResourceVersion: endpoints.Metadata.ResourceVersion,
				},
				Subsets: []EndpointSubset{
					{
						Addresses: []EndpointAddress{
							{IP: "10.10.10.10"},
						},
					},
				},
			}, endpoints) {
				t.FailNow()
			}
			return false
		})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		go func() {
			time.Sleep(time.Second)
			if _, err := te.Clientset.CoreV1().Endpoints("default").Update(
				context.Background(),
				&corev1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Name: "ijk",
					},
					Subsets: []corev1.EndpointSubset{
						{
							Addresses: []corev1.EndpointAddress{
								{IP: "11.11.11.11"},
							},
						},
					},
				},
				metav1.UpdateOptions{},
			); err != nil {
				t.Error(err)
				return
			}
		}()
		err = kc.WatchEndpoints(context.Background(), "default", "ijk", endpoints.Metadata.ResourceVersion, func(eventType EventType, endpoints *Endpoints) bool {
			if !assert.Equal(t, EventModified, eventType) {
				t.FailNow()
			}
			if !assert.Equal(t, &Endpoints{
				Metadata: Metadata{
					ResourceVersion: endpoints.Metadata.ResourceVersion,
				},
				Subsets: []EndpointSubset{
					{
						Addresses: []EndpointAddress{
							{IP: "11.11.11.11"},
						},
					},
				},
			}, endpoints) {
				t.FailNow()
			}
			return false
		})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}
}

type testingEnvironment struct {
	Clientset *kubernetes.Clientset
	Fs        afero.Fs
	Env       venv.Env
	Clock     clock.Clock
}

func prepareTestingEnvironment(t *testing.T) *testingEnvironment {
	kubeConfigData := vk8s.SetUp(context.Background(), 5*time.Minute, t)
	config, err := clientcmd.RESTConfigFromKubeConfig(kubeConfigData)
	if err != nil {
		t.Fatal(err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clientset.RbacV1().ClusterRoles().Create(
		context.Background(),
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name: "endpoints-reader",
			},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{""},
				Resources: []string{"endpoints"},
				Verbs:     []string{"get", "watch"},
			}},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := clientset.RbacV1().ClusterRoleBindings().Create(
		context.Background(),
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "endpoints-reader",
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      "default",
					Namespace: metav1.NamespaceDefault,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "endpoints-reader",
			},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}
	serviceAccount, err := clientset.CoreV1().ServiceAccounts(metav1.NamespaceDefault).Get(context.Background(), "default", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	secretName := serviceAccount.Secrets[0].Name
	secret, err := clientset.CoreV1().Secrets(metav1.NamespaceDefault).Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fs := afero.NewMemMapFs()
	if err := fs.MkdirAll("/var/run/secrets/kubernetes.io/serviceaccount", 755); err != nil {
		t.Fatal(err)
	}
	for _, fileName := range [...]string{"token", "ca.crt", "namespace"} {
		data, ok := secret.Data[fileName]
		if !ok {
			t.Fatalf("can't find %s in secret.Data", fileName)
		}
		if err := afero.WriteFile(
			fs,
			"/var/run/secrets/kubernetes.io/serviceaccount/"+fileName,
			data,
			644,
		); err != nil {
			t.Fatal(err)
		}
	}
	url, err := url.Parse(config.Host)
	if err != nil {
		t.Fatal(err)
	}
	env := venv.Mock()
	env.Setenv("KUBERNETES_SERVICE_HOST", url.Hostname())
	env.Setenv("KUBERNETES_SERVICE_PORT", url.Port())
	clock := clock.NewMock()
	clock.Set(time.Now())
	return &testingEnvironment{
		Clientset: clientset,
		Fs:        fs,
		Env:       env,
		Clock:     clock,
	}
}
