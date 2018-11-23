package etcdlock

import (
	_ "fmt"
	_ "os"
	_ "testing"
	_ "time"

	_ "github.com/etcd-io/etcd/contrib/recipes"
	_ "github.com/flaviostutz/etcd-lock/etcdlock"
	_ "go.etcd.io/etcd/clientv3"
	_ "go.etcd.io/etcd/clientv3/concurrency"
	_ "go.etcd.io/etcd/integration"
	_ "go.etcd.io/etcd/mvcc/mvccpb"
	_ "go.etcd.io/etcd/pkg/testutil"
)
