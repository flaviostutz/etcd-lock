package etcdlock

import (
	_ "fmt"
	_ "os"
	_ "testing"
	_ "time"

	_ "github.com/coreos/etcd/contrib/recipes"
	_ "github.com/coreos/etcd/clientv3"
	_ "github.com/coreos/etcd/clientv3/concurrency"
	_ "github.com/coreos/etcd/integration"
	_ "github.com/coreos/etcd/mvcc/mvccpb"
	_ "github.com/coreos/etcd/pkg/testutil"
)
