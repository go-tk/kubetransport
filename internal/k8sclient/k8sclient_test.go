package k8sclient_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/adammck/venv"
	"github.com/benbjohnson/clock"
	. "github.com/go-tk/kubetransport/internal/k8sclient"
	"github.com/go-tk/testcase"
	"github.com/jarcoal/httpmock"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestDoNew(t *testing.T) {
	type Workspace struct {
		In struct {
			Fs        afero.Fs
			Env       venv.Env
			MockClock *clock.Mock
		}
		ExpOut, ActOut struct {
			Err    error
			ErrStr string
		}
		KC K8sClient
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			w.In.Fs = afero.NewMemMapFs()
			w.In.Env = venv.Mock()
			w.In.MockClock = clock.NewMock()
			w.In.MockClock.Set(time.Now())
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			w.KC, w.ActOut.Err = DoNew(w.In.Fs, DummyTransportReplacer, w.In.Env, w.In.MockClock)
			if w.ActOut.Err != nil {
				w.ActOut.ErrStr = w.ActOut.Err.Error()
			}
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			if w.ExpOut.Err == nil || errors.Is(w.ActOut.Err, w.ExpOut.Err) {
				w.ExpOut.Err = w.ActOut.Err
			}
			assert.Equal(t, w.ExpOut, w.ActOut)
		})
	testcase.RunListParallel(t,
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.ExpOut.Err = os.ErrNotExist
				w.ExpOut.ErrStr = "read ca certificate file; filePath=\"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt\": open /var/run/secrets/kubernetes.io/serviceaccount/ca.crt: file does not exist"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				err := afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
					[]byte("CCAA"),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.ExpOut.ErrStr = "can't add ca certificate"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				err := afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
					[]byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCAR2gAwIBAgIBADAKBggqhkjOPQQDAjAjMSEwHwYDVQQDDBhrM3Mtc2Vy
dmVyLWNhQDE2MjM1MDQ5MDYwHhcNMjEwNjEyMTMzNTA2WhcNMzEwNjEwMTMzNTA2
WjAjMSEwHwYDVQQDDBhrM3Mtc2VydmVyLWNhQDE2MjM1MDQ5MDYwWTATBgcqhkjO
PQIBBggqhkjOPQMBBwNCAAQ3qTr0SbaK0a7zf8LqavDZsV0dwTvXTnmkDa4DJ7XZ
/zU1E1rBuCeJ4hmqnLB97k5ePamOrFEcQljOI27+2/2Qo0IwQDAOBgNVHQ8BAf8E
BAMCAqQwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUmAS4j2mFkRsIbhk2FlrO
+9eFeKswCgYIKoZIzj0EAwIDSAAwRQIgH6sg05GpW0gOrVySsQgO5LD3ythEfJte
lO/HJTzVSS8CIQCySRrL0DQOyd2PYzqPvUq7XHuiIfRqLtLOP4+j7fDGDQ==
-----END CERTIFICATE-----
`),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.ExpOut.ErrStr = "can't find environment variable KUBERNETES_SERVICE_HOST"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				err := afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
					[]byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCAR2gAwIBAgIBADAKBggqhkjOPQQDAjAjMSEwHwYDVQQDDBhrM3Mtc2Vy
dmVyLWNhQDE2MjM1MDQ5MDYwHhcNMjEwNjEyMTMzNTA2WhcNMzEwNjEwMTMzNTA2
WjAjMSEwHwYDVQQDDBhrM3Mtc2VydmVyLWNhQDE2MjM1MDQ5MDYwWTATBgcqhkjO
PQIBBggqhkjOPQMBBwNCAAQ3qTr0SbaK0a7zf8LqavDZsV0dwTvXTnmkDa4DJ7XZ
/zU1E1rBuCeJ4hmqnLB97k5ePamOrFEcQljOI27+2/2Qo0IwQDAOBgNVHQ8BAf8E
BAMCAqQwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUmAS4j2mFkRsIbhk2FlrO
+9eFeKswCgYIKoZIzj0EAwIDSAAwRQIgH6sg05GpW0gOrVySsQgO5LD3ythEfJte
lO/HJTzVSS8CIQCySRrL0DQOyd2PYzqPvUq7XHuiIfRqLtLOP4+j7fDGDQ==
-----END CERTIFICATE-----
`),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Env.Setenv("KUBERNETES_SERVICE_HOST", "1.1.1.1")
				w.ExpOut.ErrStr = "can't find environment variable KUBERNETES_SERVICE_PORT"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				err := afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
					[]byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCAR2gAwIBAgIBADAKBggqhkjOPQQDAjAjMSEwHwYDVQQDDBhrM3Mtc2Vy
dmVyLWNhQDE2MjM1MDQ5MDYwHhcNMjEwNjEyMTMzNTA2WhcNMzEwNjEwMTMzNTA2
WjAjMSEwHwYDVQQDDBhrM3Mtc2VydmVyLWNhQDE2MjM1MDQ5MDYwWTATBgcqhkjO
PQIBBggqhkjOPQMBBwNCAAQ3qTr0SbaK0a7zf8LqavDZsV0dwTvXTnmkDa4DJ7XZ
/zU1E1rBuCeJ4hmqnLB97k5ePamOrFEcQljOI27+2/2Qo0IwQDAOBgNVHQ8BAf8E
BAMCAqQwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUmAS4j2mFkRsIbhk2FlrO
+9eFeKswCgYIKoZIzj0EAwIDSAAwRQIgH6sg05GpW0gOrVySsQgO5LD3ythEfJte
lO/HJTzVSS8CIQCySRrL0DQOyd2PYzqPvUq7XHuiIfRqLtLOP4+j7fDGDQ==
-----END CERTIFICATE-----
`),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Env.Setenv("KUBERNETES_SERVICE_HOST", "1.1.1.1")
				w.In.Env.Setenv("KUBERNETES_SERVICE_PORT", "443")
				w.ExpOut.Err = os.ErrNotExist
				w.ExpOut.ErrStr = "read namespace file; filePath=\"/var/run/secrets/kubernetes.io/serviceaccount/namespace\": open /var/run/secrets/kubernetes.io/serviceaccount/namespace: file does not exist"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				err := afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
					[]byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCAR2gAwIBAgIBADAKBggqhkjOPQQDAjAjMSEwHwYDVQQDDBhrM3Mtc2Vy
dmVyLWNhQDE2MjM1MDQ5MDYwHhcNMjEwNjEyMTMzNTA2WhcNMzEwNjEwMTMzNTA2
WjAjMSEwHwYDVQQDDBhrM3Mtc2VydmVyLWNhQDE2MjM1MDQ5MDYwWTATBgcqhkjO
PQIBBggqhkjOPQMBBwNCAAQ3qTr0SbaK0a7zf8LqavDZsV0dwTvXTnmkDa4DJ7XZ
/zU1E1rBuCeJ4hmqnLB97k5ePamOrFEcQljOI27+2/2Qo0IwQDAOBgNVHQ8BAf8E
BAMCAqQwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUmAS4j2mFkRsIbhk2FlrO
+9eFeKswCgYIKoZIzj0EAwIDSAAwRQIgH6sg05GpW0gOrVySsQgO5LD3ythEfJte
lO/HJTzVSS8CIQCySRrL0DQOyd2PYzqPvUq7XHuiIfRqLtLOP4+j7fDGDQ==
-----END CERTIFICATE-----
`),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Env.Setenv("KUBERNETES_SERVICE_HOST", "1.1.1.1")
				w.In.Env.Setenv("KUBERNETES_SERVICE_PORT", "443")
				err = afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/namespace",
					[]byte("foo"),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.ExpOut.Err = os.ErrNotExist
				w.ExpOut.ErrStr = "get token: read token file; filePath=\"/var/run/secrets/kubernetes.io/serviceaccount/token\": open /var/run/secrets/kubernetes.io/serviceaccount/token: file does not exist"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				err := afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
					[]byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCAR2gAwIBAgIBADAKBggqhkjOPQQDAjAjMSEwHwYDVQQDDBhrM3Mtc2Vy
dmVyLWNhQDE2MjM1MDQ5MDYwHhcNMjEwNjEyMTMzNTA2WhcNMzEwNjEwMTMzNTA2
WjAjMSEwHwYDVQQDDBhrM3Mtc2VydmVyLWNhQDE2MjM1MDQ5MDYwWTATBgcqhkjO
PQIBBggqhkjOPQMBBwNCAAQ3qTr0SbaK0a7zf8LqavDZsV0dwTvXTnmkDa4DJ7XZ
/zU1E1rBuCeJ4hmqnLB97k5ePamOrFEcQljOI27+2/2Qo0IwQDAOBgNVHQ8BAf8E
BAMCAqQwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUmAS4j2mFkRsIbhk2FlrO
+9eFeKswCgYIKoZIzj0EAwIDSAAwRQIgH6sg05GpW0gOrVySsQgO5LD3ythEfJte
lO/HJTzVSS8CIQCySRrL0DQOyd2PYzqPvUq7XHuiIfRqLtLOP4+j7fDGDQ==
-----END CERTIFICATE-----
`),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Env.Setenv("KUBERNETES_SERVICE_HOST", "1.1.1.1")
				w.In.Env.Setenv("KUBERNETES_SERVICE_PORT", "443")
				err = afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/namespace",
					[]byte("foo"),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				err = afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/token",
					[]byte("bar"),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			}),
	)
}

func TestK8sClient_GetEndpoints(t *testing.T) {
	type Workspace struct {
		Init struct {
			Fs            afero.Fs
			MockTransport *httpmock.MockTransport
			Env           venv.Env
			MockClock     *clock.Mock
		}
		In struct {
			Ctx           context.Context
			Namespace     string
			EndpointsName string
		}
		ExpOut, ActOut struct {
			Endpoints *Endpoints
			Err       error
			ErrStr    string
		}
		KC K8sClient
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			w.Init.Fs = afero.NewMemMapFs()
			w.Init.MockTransport = httpmock.NewMockTransport()
			w.Init.Env = venv.Mock()
			w.Init.MockClock = clock.NewMock()
			w.Init.MockClock.Set(time.Now())
			w.In.Ctx = context.Background()
			err := afero.WriteFile(
				w.Init.Fs,
				"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
				[]byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCAR2gAwIBAgIBADAKBggqhkjOPQQDAjAjMSEwHwYDVQQDDBhrM3Mtc2Vy
dmVyLWNhQDE2MjM1MDQ5MDYwHhcNMjEwNjEyMTMzNTA2WhcNMzEwNjEwMTMzNTA2
WjAjMSEwHwYDVQQDDBhrM3Mtc2VydmVyLWNhQDE2MjM1MDQ5MDYwWTATBgcqhkjO
PQIBBggqhkjOPQMBBwNCAAQ3qTr0SbaK0a7zf8LqavDZsV0dwTvXTnmkDa4DJ7XZ
/zU1E1rBuCeJ4hmqnLB97k5ePamOrFEcQljOI27+2/2Qo0IwQDAOBgNVHQ8BAf8E
BAMCAqQwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUmAS4j2mFkRsIbhk2FlrO
+9eFeKswCgYIKoZIzj0EAwIDSAAwRQIgH6sg05GpW0gOrVySsQgO5LD3ythEfJte
lO/HJTzVSS8CIQCySRrL0DQOyd2PYzqPvUq7XHuiIfRqLtLOP4+j7fDGDQ==
-----END CERTIFICATE-----
`),
				644,
			)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			w.Init.Env.Setenv("KUBERNETES_SERVICE_HOST", "1.2.3.4")
			w.Init.Env.Setenv("KUBERNETES_SERVICE_PORT", "6443")
			err = afero.WriteFile(
				w.Init.Fs,
				"/var/run/secrets/kubernetes.io/serviceaccount/namespace",
				[]byte("default"),
				644,
			)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			err = afero.WriteFile(
				w.Init.Fs,
				"/var/run/secrets/kubernetes.io/serviceaccount/token",
				[]byte("admin"),
				644,
			)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			var err error
			w.KC, err = DoNew(w.Init.Fs, func(http.RoundTripper) http.RoundTripper { return w.Init.MockTransport }, w.Init.Env, w.Init.MockClock)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			w.ActOut.Endpoints, w.ActOut.Err = w.KC.GetEndpoints(w.In.Ctx, w.In.Namespace, w.In.EndpointsName)
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
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/namespaces/foo/endpoints/bar",
					httpmock.NewBytesResponder(404, nil),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/namespaces/foo/endpoints/bar",
					httpmock.NewStringResponder(200, `{
	"metadata": {
		"resourceVersion": "8910"
	},
	"subsets": [
		{
			"addresses": [
				{"ip": "1.2.3.4"},
				{"ip": "2.3.4.5"}
			]
		},
		{
			"addresses": [
				{"ip": "7.7.7.7"},
				{"ip": "8.8.8.8"}
			]
		}
	]
}
`),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.Endpoints = &Endpoints{
					Metadata: Metadata{
						ResourceVersion: "8910",
					},
					Subsets: []EndpointSubset{
						{
							Addresses: []EndpointAddress{
								{IP: "1.2.3.4"},
								{IP: "2.3.4.5"},
							},
						},
						{
							Addresses: []EndpointAddress{
								{IP: "7.7.7.7"},
								{IP: "8.8.8.8"},
							},
						},
					},
				}
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/namespaces/foo/endpoints/bar",
					httpmock.NewBytesResponder(500, nil),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.ErrStr = "get \"https://1.2.3.4:6443/api/v1/namespaces/foo/endpoints/bar\"; statusCode=500"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/namespaces/foo/endpoints/bar",
					httpmock.NewStringResponder(200, "[]"),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.ErrStr = "decode endpoints json: json: cannot unmarshal array into Go value of type k8sclient.Endpoints"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/namespaces/foo/endpoints/bar",
					httpmock.NewBytesResponder(401, nil),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				_, err := w.KC.GetEndpoints(context.Background(), "foo", "bar")
				if !assert.Error(t, err) {
					t.FailNow()
				}
				err = w.Init.Fs.Remove("/var/run/secrets/kubernetes.io/serviceaccount/token")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.ErrStr = "get token: read token file; filePath=\"/var/run/secrets/kubernetes.io/serviceaccount/token\": open /var/run/secrets/kubernetes.io/serviceaccount/token: file does not exist"
			}),
	)
}

func TestK8sClient_WatchEndpoints(t *testing.T) {
	type CallbackArgs struct {
		EventType EventType
		Endpoints *Endpoints
	}
	type Workspace struct {
		Init struct {
			Fs            afero.Fs
			MockTransport *httpmock.MockTransport
			Env           venv.Env
			MockClock     *clock.Mock
		}
		In struct {
			Ctx             context.Context
			Namespace       string
			EndpointsName   string
			ResourceVersion string
		}
		ExpOut, ActOut struct {
			CAs    []CallbackArgs
			Err    error
			ErrStr string
		}
		KC K8sClient
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			w.Init.Fs = afero.NewMemMapFs()
			w.Init.MockTransport = httpmock.NewMockTransport()
			w.Init.Env = venv.Mock()
			w.Init.MockClock = clock.NewMock()
			w.Init.MockClock.Set(time.Now())
			w.In.Ctx = context.Background()
			err := afero.WriteFile(
				w.Init.Fs,
				"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
				[]byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCAR2gAwIBAgIBADAKBggqhkjOPQQDAjAjMSEwHwYDVQQDDBhrM3Mtc2Vy
dmVyLWNhQDE2MjM1MDQ5MDYwHhcNMjEwNjEyMTMzNTA2WhcNMzEwNjEwMTMzNTA2
WjAjMSEwHwYDVQQDDBhrM3Mtc2VydmVyLWNhQDE2MjM1MDQ5MDYwWTATBgcqhkjO
PQIBBggqhkjOPQMBBwNCAAQ3qTr0SbaK0a7zf8LqavDZsV0dwTvXTnmkDa4DJ7XZ
/zU1E1rBuCeJ4hmqnLB97k5ePamOrFEcQljOI27+2/2Qo0IwQDAOBgNVHQ8BAf8E
BAMCAqQwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUmAS4j2mFkRsIbhk2FlrO
+9eFeKswCgYIKoZIzj0EAwIDSAAwRQIgH6sg05GpW0gOrVySsQgO5LD3ythEfJte
lO/HJTzVSS8CIQCySRrL0DQOyd2PYzqPvUq7XHuiIfRqLtLOP4+j7fDGDQ==
-----END CERTIFICATE-----
`),
				644,
			)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			w.Init.Env.Setenv("KUBERNETES_SERVICE_HOST", "1.2.3.4")
			w.Init.Env.Setenv("KUBERNETES_SERVICE_PORT", "6443")
			err = afero.WriteFile(
				w.Init.Fs,
				"/var/run/secrets/kubernetes.io/serviceaccount/namespace",
				[]byte("default"),
				644,
			)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			err = afero.WriteFile(
				w.Init.Fs,
				"/var/run/secrets/kubernetes.io/serviceaccount/token",
				[]byte("admin"),
				644,
			)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			var err error
			w.KC, err = DoNew(w.Init.Fs, func(http.RoundTripper) http.RoundTripper { return w.Init.MockTransport }, w.Init.Env, w.Init.MockClock)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			w.ActOut.Err = w.KC.WatchEndpoints(w.In.Ctx, w.In.Namespace, w.In.EndpointsName, w.In.ResourceVersion, func(eventType EventType, endpoints *Endpoints) bool {
				w.ActOut.CAs = append(w.ActOut.CAs, CallbackArgs{EventType: eventType, Endpoints: endpoints})
				return true
			})
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
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar",
					httpmock.NewStringResponder(200, `{
	"type": "ADDED",
	"object": {
		"metadata": {
			"resourceVersion": "8910"
		},
		"subsets": [
			{
				"addresses": [
					{"ip": "1.2.3.4"},
					{"ip": "2.3.4.5"}
				]
			},
			{
				"addresses": [
					{"ip": "7.7.7.7"},
					{"ip": "8.8.8.8"}
				]
			}
		]
	}
}
{
	"type": "DELETED",
	"object": {
		"metadata": {
			"resourceVersion": "8920"
		},
		"subsets": [
			{
				"addresses": [
					{"ip": "9.9.9.9"}
				]
			}
		]
	}
}
`),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.CAs = []CallbackArgs{
					{
						EventType: EventAdded,
						Endpoints: &Endpoints{
							Metadata: Metadata{
								ResourceVersion: "8910",
							},
							Subsets: []EndpointSubset{
								{
									Addresses: []EndpointAddress{
										{IP: "1.2.3.4"},
										{IP: "2.3.4.5"},
									},
								},
								{
									Addresses: []EndpointAddress{
										{IP: "7.7.7.7"},
										{IP: "8.8.8.8"},
									},
								},
							},
						},
					},
					{
						EventType: EventDeleted,
						Endpoints: &Endpoints{
							Metadata: Metadata{
								ResourceVersion: "8920",
							},
							Subsets: []EndpointSubset{
								{
									Addresses: []EndpointAddress{
										{IP: "9.9.9.9"},
									},
								},
							},
						},
					},
				}
				w.ExpOut.Err = io.EOF
				w.ExpOut.ErrStr = "decode event json: EOF"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar",
					httpmock.NewBytesResponder(500, nil),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.ErrStr = "get \"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar\"; statusCode=500"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar",
					httpmock.NewStringResponder(200, `{
	"type": "ADDED",
	"object": {
		"metadata": {
			"resourceVersion": "8910"
		},
		"subsets": [{"addresses": [{"ip": "1.2.3.4"}]}]
	}
}
{
	"type": 100
}
`),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.CAs = []CallbackArgs{
					{
						EventType: EventAdded,
						Endpoints: &Endpoints{
							Metadata: Metadata{
								ResourceVersion: "8910",
							},
							Subsets: []EndpointSubset{
								{
									Addresses: []EndpointAddress{
										{IP: "1.2.3.4"},
									},
								},
							},
						},
					},
				}
				w.ExpOut.ErrStr = "decode event json: json: cannot unmarshal number into Go struct field .type of type k8sclient.EventType"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar",
					httpmock.NewStringResponder(200, `{
	"type": "ADDED",
	"object": {
		"metadata": {
			"resourceVersion": "8910"
		},
		"subsets": [{"addresses": [{"ip": "1.2.3.4"}]}]
	}
}
{
	"type": "ADDED",
	"object": {
		"metadata": {
			"resourceVersion": "8910"
		},
		"subsets": [{"addresses": "1.1.1.1,2.2.2.2,3.3.3.3"}]
	}
}
`),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.CAs = []CallbackArgs{
					{
						EventType: EventAdded,
						Endpoints: &Endpoints{
							Metadata: Metadata{
								ResourceVersion: "8910",
							},
							Subsets: []EndpointSubset{
								{
									Addresses: []EndpointAddress{
										{IP: "1.2.3.4"},
									},
								},
							},
						},
					},
				}
				w.ExpOut.ErrStr = "decode event json: json: cannot unmarshal string into Go struct field EndpointSubset.subsets.addresses of type []k8sclient.EndpointAddress"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar",
					httpmock.NewStringResponder(200, `{
	"type": "ADDED",
	"object": {
		"metadata": {
			"resourceVersion": "8910"
		},
		"subsets": [{"addresses": [{"ip": "1.2.3.4"}]}]
	}
}
{
	"type": "ERROR",
	"object": {
		"code": 410,
		"message": "gone"
	}
}
`),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.ExpOut.CAs = []CallbackArgs{
					{
						EventType: EventAdded,
						Endpoints: &Endpoints{
							Metadata: Metadata{
								ResourceVersion: "8910",
							},
							Subsets: []EndpointSubset{
								{
									Addresses: []EndpointAddress{
										{IP: "1.2.3.4"},
									},
								},
							},
						},
					},
				}
				w.ExpOut.ErrStr = "receive error event: gone"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar?resourceVersion=8910",
					httpmock.NewStringResponder(200, `{
	"type": "ERROR",
	"object": {
		"code": 410,
		"message": "gone"
	}
}
`),
				)
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar",
					httpmock.NewStringResponder(200, `{
	"type": "ADDED",
	"object": {
		"metadata": {
			"resourceVersion": "8910"
		},
		"subsets": [{"addresses": [{"ip": "1.2.3.4"}]}]
	}
}
`),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.In.ResourceVersion = "8910"
				w.ExpOut.Err = io.EOF
				w.ExpOut.ErrStr = "decode event json: EOF"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar?resourceVersion=8910",
					httpmock.NewStringResponder(200, `{
	"type": "ERROR",
	"object": {
		"code": 410,
		"message": "gone"
	}
}
`),
				)
				w.Init.MockTransport.RegisterResponder(
					"GET",
					"https://1.2.3.4:6443/api/v1/watch/namespaces/foo/endpoints/bar",
					httpmock.NewStringResponder(200, `{
	"type": "ADDED",
	"object": {
		"metadata": {
			"resourceVersion": "8999"
		},
		"subsets": [{"addresses": [{"ip": "1.2.3.4"}]}]
	}
}
`),
				)
			}).
			Step(1.5, func(t *testing.T, w *Workspace) {
				w.In.Namespace = "foo"
				w.In.EndpointsName = "bar"
				w.In.ResourceVersion = "8910"
				w.ExpOut.CAs = []CallbackArgs{
					{
						EventType: EventAdded,
						Endpoints: &Endpoints{
							Metadata: Metadata{
								ResourceVersion: "8999",
							},
							Subsets: []EndpointSubset{
								{
									Addresses: []EndpointAddress{
										{IP: "1.2.3.4"},
									},
								},
							},
						},
					},
				}
				w.ExpOut.Err = io.EOF
				w.ExpOut.ErrStr = "decode event json: EOF"
			}),
	)
}

func TestToken_Get(t *testing.T) {
	type Workspace struct {
		In struct {
			MockClock *clock.Mock
			Fs        afero.Fs
		}
		ExpOut, ActOut struct {
			Value  string
			Err    error
			ErrStr string
		}
		T Token
	}
	tc := testcase.New().
		Step(0, func(t *testing.T, w *Workspace) {
			w.In.MockClock = clock.NewMock()
			w.In.MockClock.Set(time.Now())
			w.In.Fs = afero.NewMemMapFs()
			err := w.In.Fs.MkdirAll("/var/run/secrets/kubernetes.io/serviceaccount", 755)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			err = afero.WriteFile(
				w.In.Fs,
				"/var/run/secrets/kubernetes.io/serviceaccount/token",
				[]byte("tOKen"),
				644,
			)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
		}).
		Step(1, func(t *testing.T, w *Workspace) {
			w.ActOut.Value, w.ActOut.Err = w.T.Get(w.In.MockClock, w.In.Fs)
			if w.ActOut.Err != nil {
				w.ActOut.ErrStr = w.ActOut.Err.Error()
			}
		}).
		Step(2, func(t *testing.T, w *Workspace) {
			if w.ExpOut.Err == nil || errors.Is(w.ActOut.Err, w.ExpOut.Err) {
				w.ExpOut.Err = w.ActOut.Err
			}
			assert.Equal(t, w.ExpOut, w.ActOut)
		})
	testcase.RunListParallel(t,
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				w.ExpOut.Value = "tOKen"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				v, err := w.T.Get(w.In.MockClock, w.In.Fs)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.Equal(t, "tOKen", v) {
					t.FailNow()
				}
				err = w.In.Fs.Remove("/var/run/secrets/kubernetes.io/serviceaccount/token")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.ExpOut.Value = "tOKen"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				v, err := w.T.Get(w.In.MockClock, w.In.Fs)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.Equal(t, "tOKen", v) {
					t.FailNow()
				}
				err = afero.WriteFile(
					w.In.Fs,
					"/var/run/secrets/kubernetes.io/serviceaccount/token",
					[]byte("New_tOKen"),
					644,
				)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.In.MockClock.Set(w.In.MockClock.Now().Add(24 * time.Hour))
				w.ExpOut.Value = "New_tOKen"
			}),
		tc.Copy().
			Step(0.5, func(t *testing.T, w *Workspace) {
				err := w.In.Fs.Remove("/var/run/secrets/kubernetes.io/serviceaccount/token")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w.ExpOut.Err = os.ErrNotExist
				w.ExpOut.ErrStr = "read token file; filePath=\"/var/run/secrets/kubernetes.io/serviceaccount/token\": open /var/run/secrets/kubernetes.io/serviceaccount/token: file does not exist"
			}),
	)
}

func TestToken_Reset(t *testing.T) {
	var tk Token
	mockClock := clock.NewMock()
	mockClock.Set(time.Now())
	fs := afero.NewMemMapFs()
	err := fs.MkdirAll("/var/run/secrets/kubernetes.io/serviceaccount", 755)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	err = afero.WriteFile(
		fs,
		"/var/run/secrets/kubernetes.io/serviceaccount/token",
		[]byte("tOKen"),
		644,
	)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	v, err := tk.Get(mockClock, fs)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.Equal(t, "tOKen", v) {
		t.FailNow()
	}
	err = afero.WriteFile(
		fs,
		"/var/run/secrets/kubernetes.io/serviceaccount/token",
		[]byte("New_tOKen"),
		644,
	)
	v, err = tk.Get(mockClock, fs)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.Equal(t, "tOKen", v) {
		t.FailNow()
	}
	tk.Reset()
	v, err = tk.Get(mockClock, fs)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.Equal(t, "New_tOKen", v) {
		t.FailNow()
	}
}
