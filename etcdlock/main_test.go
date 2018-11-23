package etcdlock

import (
	"fmt"
	"os"
	"testing"
	"time"

	"go.etcd.io/etcd/integration"
	"go.etcd.io/etcd/pkg/testutil"
)

var endpoints []string

// TestMain sets up an etcd cluster for running the examples.
func TestMain(m *testing.M) {
	cfg := integration.ClusterConfig{Size: 1}
	clus := integration.NewClusterV3(nil, &cfg)
	endpoints = []string{clus.Client(0).Endpoints()[0]}
	v := m.Run()
	clus.Terminate(nil)
	if err := testutil.CheckAfterTest(time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(1)
	}
	if v == 0 && testutil.CheckLeakedGoroutine() {
		os.Exit(1)
	}
	os.Exit(v)
}
