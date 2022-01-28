package main

import (
	"context"
	"flag"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
	"net/http"
	"net/url"
)

// The main idea of this PoC is to probe that dynamic client will send less request that regular client
// preventing throttling issues for example
// `kubectl get workloads.carto.run -v 10`
// shows a long list of discovery call but if you run this program with -v
// The list of call is reduced significantly

// Most of this example is taken from https://caiorcferreira.github.io/post/the-kubernetes-dynamic-client/

// By default, it will take configuration from  ~/.kube/config and the selected namespace
// use loadingRules.ExplicitPath = "/Users/luis/.kube/config" if you don't want that
// use CurrentContext: "stage" on ConfigOverrides to change current context
func kubeConfig() clientcmd.ClientConfig {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
}

// Creates a client with a low query per second variable and a low burst to make sure it won't throttle under bad
// conditions plus it output the request to the K8s cluster
func newClient() (dynamic.Interface, error) {
	kubeConfig := kubeConfig()
	config, err := kubeConfig.ClientConfig()
	// Reduce queries per second (qps) and burst
	config.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(1, 1)

	// Just print the request
	config.Proxy = func(request *http.Request) (*url.URL, error) {
		fmt.Println(request)
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return dynClient, nil
}

// List the Worloads on the cluster
func ListWorkloads(ctx context.Context, client dynamic.Interface, namespace string) ([]unstructured.Unstructured, error) {
	var resource = schema.GroupVersionResource{Group: "carto.run", Version: "v1alpha1", Resource: "workloads"}
	list, err := client.Resource(resource).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return list.Items, nil
}

func main() {
	ctx := context.Background()

	// Tell client-go to be as verbose as is set on -v attr
	klog.InitFlags(nil) // initializing the flags
	defer klog.Flush()  // flushes all pending log I/O
	flag.Parse()        // parses the command-line flags

	c, _ := newClient()
	l, _ := ListWorkloads(ctx, c, "default")
	fmt.Println(l)
}
