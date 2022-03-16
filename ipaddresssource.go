package kubetransport

import (
	"context"
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

type ipAddressSource struct {
	backgroundCtx   context.Context
	endpointsGetter typedcorev1.EndpointsGetter
	namespace       string
	endpointsName   string
	callback        func([]string, error)
	watchClearer    func()
}

func newIPAddressSource(backgroundCtx context.Context, endpointsGetter typedcorev1.EndpointsGetter, namespace string, endpointsName string) *ipAddressSource {
	var ipas ipAddressSource
	ipas.backgroundCtx = backgroundCtx
	ipas.endpointsGetter = endpointsGetter
	ipas.namespace = namespace
	ipas.endpointsName = endpointsName
	return &ipas
}

func (ipas *ipAddressSource) GetValuesAndSetWatch(callback func([]string, error)) {
	ipas.callback = callback
	go ipas.doGetValuesAndSetWatch()
}

func (ipas *ipAddressSource) doGetValuesAndSetWatch() {
	endpointsInterface := ipas.endpointsGetter.Endpoints(ipas.namespace)
	endpoints, err := endpointsInterface.Get(ipas.backgroundCtx, ipas.endpointsName, metav1.GetOptions{})
	if err != nil && apierrors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		ipas.callback(nil, fmt.Errorf("get endpoints; namespace=%q endpointsName=%q: %w",
			ipas.namespace, ipas.endpointsName, err))
		return
	}
	var firstResourceVersion string
	if endpoints != nil {
		firstResourceVersion = endpoints.ResourceVersion
	}
	listOptions := metav1.ListOptions{
		FieldSelector:   "metadata.name=" + ipas.endpointsName,
		ResourceVersion: firstResourceVersion,
	}
	watchInterface, err := endpointsInterface.Watch(ipas.backgroundCtx, listOptions)
	if err != nil && apierrors.IsGone(err) {
		listOptions.ResourceVersion = ""
		watchInterface, err = endpointsInterface.Watch(ipas.backgroundCtx, listOptions)
	}
	if err != nil {
		ipas.callback(nil, fmt.Errorf("watch endpoints; namespace=%q endpointsName=%q: %w",
			ipas.namespace, ipas.endpointsName, err))
		return
	}
	ipas.watchClearer = watchInterface.Stop
	var values []string
	if endpoints != nil {
		values = extractIPAddresses(endpoints)
	}
	ipas.callback(values, nil)
	for {
		select {
		case <-ipas.backgroundCtx.Done():
			err := ipas.backgroundCtx.Err()
			ipas.callback(nil, err)
			watchInterface.Stop()
			return
		case event, ok := <-watchInterface.ResultChan():
			if !ok {
				ipas.callback(nil, fmt.Errorf("%w; namespace=%q endpointsName=%q",
					errWatchCleared, ipas.namespace, ipas.endpointsName))
				return
			}
			switch event.Type {
			case watch.Added:
				endpoints = event.Object.(*v1.Endpoints)
				if endpoints.ResourceVersion == firstResourceVersion {
					continue
				}
				values := extractIPAddresses(endpoints)
				ipas.callback(values, nil)
			case watch.Modified:
				endpoints = event.Object.(*v1.Endpoints)
				values := extractIPAddresses(endpoints)
				ipas.callback(values, nil)
			case watch.Deleted:
				ipas.callback(nil, nil)
			case watch.Error:
				status := event.Object.(*metav1.Status)
				statusError := apierrors.StatusError{
					ErrStatus: *status,
				}
				ipas.callback(nil, fmt.Errorf("watch endpoints; namespace=%q endpointsName=%q: %w",
					ipas.namespace, ipas.endpointsName, &statusError))
				return
			}
		}
	}
}

func (ipas *ipAddressSource) ClearWatch() { ipas.watchClearer() }

func extractIPAddresses(endpoints *v1.Endpoints) []string {
	var i int
	for j := range endpoints.Subsets {
		endpointSubset := &endpoints.Subsets[j]
		i += len(endpointSubset.Addresses)
	}
	ipAddresses := make([]string, i)
	i = 0
	for j := range endpoints.Subsets {
		endpointSubset := &endpoints.Subsets[j]
		for k := range endpointSubset.Addresses {
			endpointAddress := &endpointSubset.Addresses[k]
			ipAddresses[i] = endpointAddress.IP
			i++
		}
	}
	return ipAddresses
}

var errWatchCleared = errors.New("kubetransport: watch cleared")
