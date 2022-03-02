# kubetransport

[![GoDev](https://pkg.go.dev/badge/golang.org/x/pkgsite.svg)](https://pkg.go.dev/github.com/go-tk/kubetransport)
[![Workflow Status](https://github.com/go-tk/kubetransport/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/go-tk/kubetransport/actions/workflows/ci.yaml?query=branch%3Amain)
[![Coverage Status](https://codecov.io/gh/go-tk/kubetransport/branch/main/graph/badge.svg)](https://codecov.io/gh/go-tk/kubetransport/branch/main)

HTTP/2 transport wrapper for client-side load balancing in Kubernetes

## Problem

When an HTTP/2 client sends a request to an HTTP/2 server, a persistent connection between the client
and the server is established, and thanks to the multiplexing feature of the HTTP/2 protocol, all the
subsequent requests from the client can go through this connection concurrently without establishing
additional connections, which significantly improves the use of network resources.

Unfortunately, the persistent connection also comes with a side effect that it breaks the load
balancing in Kubernetes. Let's say there are three server instances behind a Kubernetes Service
FQDN and our client send out requests with this FQDN, which will result in a persistent connection
established with only one of the server instances, and no traffic will reach the other server instances.

## Solution

Kubernetes provides Endpoints API to retrieve and watch the endpoints information of a Service which
contains the IP addresses of server instances. If we intercept every request sent out and replace the
hostname (FQDN) in its URL with one of the IP addresses randomly, the load balancing can work again.
For intercepting outgoing requests, we can simply wrap the HTTP transport.

That is all about what this package did.
