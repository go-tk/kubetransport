package kubetransport

import (
	"context"
	"fmt"

	"github.com/go-tk/kubetransport/internal/k8sclient"
)

type ipAddressesSource struct {
	backgroundCtx context.Context
	stop          context.CancelFunc
	k8sClient     k8sclient.K8sClient
	namespace     string
	endpointsName string
	valueCallback ipAddressesCallback
}

type ipAddressesCallback func(ipAddressesSource *ipAddressesSource, ipAddresses []string, err error)

func newIPAddressesSource(
	backgroundCtx context.Context,
	k8sClient k8sclient.K8sClient,
	namespace string,
	endpointsName string,
	valueCallback ipAddressesCallback,
) *ipAddressesSource {
	var ipas ipAddressesSource
	ipas.backgroundCtx, ipas.stop = context.WithCancel(backgroundCtx)
	ipas.k8sClient = k8sClient
	ipas.namespace = namespace
	ipas.endpointsName = endpointsName
	ipas.valueCallback = valueCallback
	go ipas.getValuesAndSetWatch()
	return &ipas
}

func (ipas *ipAddressesSource) getValuesAndSetWatch() {
	endpoints, err := ipas.k8sClient.GetEndpoints(ipas.backgroundCtx, ipas.namespace, ipas.endpointsName)
	if err != nil {
		ipas.valueCallback(ipas, nil, fmt.Errorf("get endpoints; namespace=%q endpointsName=%q: %w", ipas.namespace, ipas.endpointsName, err))
		return
	}
	var value []string
	var resourceVersion string
	if endpoints != nil {
		value = extractIPAddresses(endpoints)
		resourceVersion = endpoints.Metadata.ResourceVersion
	}
	ipas.valueCallback(ipas, value, nil)
	err = ipas.k8sClient.WatchEndpoints(ipas.backgroundCtx, ipas.namespace, ipas.endpointsName, resourceVersion, func(eventType k8sclient.EventType, endpoints *k8sclient.Endpoints) bool {
		var value []string
		if eventType != k8sclient.EventDeleted {
			value = extractIPAddresses(endpoints)
		}
		ipas.valueCallback(ipas, value, nil)
		return true
	})
	ipas.valueCallback(ipas, nil, fmt.Errorf("watch endpoints; namespace=%q endpointsName=%q: %w", ipas.namespace, ipas.endpointsName, err))
}

func extractIPAddresses(endpoints *k8sclient.Endpoints) []string {
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

func (ipas *ipAddressesSource) Stop() { ipas.stop() }
