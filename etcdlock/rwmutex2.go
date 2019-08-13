//Almost the same as official ETCD recipes rwmutex, but with explicit Context pass on Lock() operations

package etcdlock

import (
	"context"
	"errors"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	recipes "github.com/coreos/etcd/contrib/recipes"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

//RWMutex struct for RWLock mutext
type RWMutex struct {
	s     *concurrency.Session
	pfx   string
	myKey *EphemeralKV
}

//NewRWMutex for RWLock management. Session will define a refreshing update of a lock on server. If session is Orphaned or closed, the remote lock will be lost.
func NewRWMutex(s *concurrency.Session, prefix string) *RWMutex {
	return &RWMutex{s, prefix + "/", nil}
}

//RLock Read Lock. Will obey context for the deadline or canceling of lock acquirement
func (rwm *RWMutex) RLock(ctx context.Context) error {
	rk, err := newUniqueEphemeralKey(rwm.s, rwm.pfx+"read")
	if err != nil {
		return err
	}
	rwm.myKey = rk
	// wait until nodes with "/write" and a lower revision number than myKey are gone
	_, err = rwm.waitOnLastRev(ctx, rwm.pfx+"write")
	if err != nil {
		_ = rwm.myKey.Delete()
		return err
	}
	return nil
}

//RWLock Read Write Lock. Will obey context for the deadline or canceling of lock acquirement
func (rwm *RWMutex) RWLock(ctx context.Context) error {
	rk, err := newUniqueEphemeralKey(rwm.s, rwm.pfx+"write")
	if err != nil {
		return err
	}
	rwm.myKey = rk
	// wait until all keys of lower revision than myKey are gone
	_, err = rwm.waitOnLastRev(ctx, rwm.pfx)
	if err != nil {
		_ = rwm.myKey.Delete()
		return err
	}
	return nil
}

// waitOnLowest will wait on the last key with a revision < rwm.myKey.Revision with a
// given prefix. If there are no keys left to wait on, return true.
func (rwm *RWMutex) waitOnLastRev(ctx context.Context, pfx string) (bool, error) {
	client := rwm.s.Client()
	// get key that's blocking myKey
	opts := append(v3.WithLastRev(), v3.WithMaxModRev(rwm.myKey.Revision()-1))
	lastKey, err := client.Get(ctx, pfx, opts...)
	if err != nil {
		return false, err
	}
	if len(lastKey.Kvs) == 0 {
		return true, nil
	}
	// wait for release on blocking key
	_, err = WaitEvent(
		ctx,
		client,
		string(lastKey.Kvs[0].Key),
		rwm.myKey.Revision(),
		mvccpb.DELETE)
	return false, err
}

// WaitEvent waits on a key until it observes the given event and returns the matched one or returns error if the channel closes.
func WaitEvent(ctx context.Context, c *v3.Client, key string, rev int64, event mvccpb.Event_EventType) (*v3.Event, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := c.Watch(ctx, key, v3.WithRev(rev))
	if wc == nil {
		return nil, recipes.ErrNoWatcher
	}
	for {
		select {
		case keyChannel := <-wc:
			if err := keyChannel.Err(); err != nil {
				return nil, err
			}
			for _, ev := range keyChannel.Events {
				if ev.Type == event {
					return ev, nil
				}
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

//Unlock a previously acquired lock
func (rwm *RWMutex) Unlock() error {
	if rwm.myKey != nil {
		return rwm.myKey.Delete()
	}
	return errors.New("Lock cannot be released because it was not acquired yet")
}
