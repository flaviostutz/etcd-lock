//Almost the same as official ETCD recipes rwmutex, but with explicit Context pass on Lock() operations

package etcdlock

import (
	"context"
	"errors"

	v3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	recipe "go.etcd.io/etcd/contrib/recipes"
	"go.etcd.io/etcd/mvcc/mvccpb"
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
	// wait until nodes with "write-" and a lower revision number than myKey are gone
	for {
		if done, werr := rwm.waitOnLastRev(ctx, rwm.pfx+"write"); done || werr != nil {
			return werr
		}
	}
}

//RWLock Read Write Lock. Will obey context for the deadline or canceling of lock acquirement
func (rwm *RWMutex) RWLock(ctx context.Context) error {
	rk, err := newUniqueEphemeralKey(rwm.s, rwm.pfx+"write")
	if err != nil {
		return err
	}
	rwm.myKey = rk
	// wait until all keys of lower revision than myKey are gone
	for {
		if done, werr := rwm.waitOnLastRev(ctx, rwm.pfx); done || werr != nil {
			return werr
		}
		//  get the new lowest key until this is the only one left
	}
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
	_, err = WaitEvents(
		ctx,
		client,
		string(lastKey.Kvs[0].Key),
		rwm.myKey.Revision(),
		[]mvccpb.Event_EventType{mvccpb.DELETE})
	return false, err
}

// WaitEvents waits on a key until it observes the given events and returns the final one.
func WaitEvents(ctx context.Context, c *v3.Client, key string, rev int64, evs []mvccpb.Event_EventType) (*v3.Event, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := c.Watch(ctx, key, v3.WithRev(rev))
	if wc == nil {
		return nil, recipe.ErrNoWatcher
	}
	return waitEvents(wc, evs), nil
}

func waitEvents(wc v3.WatchChan, evs []mvccpb.Event_EventType) *v3.Event {
	i := 0
	for wresp := range wc {
		for _, ev := range wresp.Events {
			if ev.Type == evs[i] {
				i++
				if i == len(evs) {
					return ev
				}
			}
		}
	}
	return nil
}

//Unlock a previously acquired lock
func (rwm *RWMutex) Unlock() error {
	if rwm.myKey != nil {
		return rwm.myKey.Delete()
	} else {
		return errors.New("Lock cannot be released because it was not acquired yet")
	}
}
