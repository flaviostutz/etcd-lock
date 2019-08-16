//Almost the same as official ETCD recipes rwmutex, but with explicit Context pass on Lock() operations

package etcdlock

import (
	"context"
	"errors"
	"fmt"
	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	recipes "github.com/coreos/etcd/contrib/recipes"
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
	if err = rwm.waitOnLastRev(ctx, rwm.pfx+"write"); err != nil {
		if dErr := rwm.myKey.Delete(); dErr != nil {
			return errors.New(fmt.Sprintf("error getting lock: %s; error deleting key %s from etcd: %s.", err.Error(), rwm.myKey.key, dErr.Error()))
		}
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
	if err = rwm.waitOnLastRev(ctx, rwm.pfx); err != nil {
		if dErr := rwm.myKey.Delete(); dErr != nil {
			return errors.New(fmt.Sprintf("error getting lock: %s; error deleting key %s from etcd: %s.", err.Error(), rwm.myKey.key, dErr.Error()))
		}
		return err
	}
	return nil
}

// waitOnLowest will wait on the last key with a revision < rwm.myKey.Revision with a
// given prefix. If there are no keys left to wait on, return true.
func (rwm *RWMutex) waitOnLastRev(ctx context.Context, pfx string) error {
	client := rwm.s.Client()
	// get keys that's blocking myKey
	lastKeys, err := client.Get(ctx, pfx, v3.WithPrefix(), v3.WithMaxModRev(rwm.myKey.Revision()-1))
	if err != nil {
		return err
	}
	if len(lastKeys.Kvs) == 0 {
		return nil
	}
	// wait for release on blocking keys
	err = WaitKeysDelete(
		ctx,
		client,
		pfx,
		lastKeys)
	return err
}

// WaitKeysDelete waits on a keys until it observes the given delete event or returns error if the channel closes.
func WaitKeysDelete(ctx context.Context, c *v3.Client, prefix string, response *v3.GetResponse) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	//WithFilterPut discards PUT events from the watcher, so just DELETE events are being observed
	wc := c.Watch(ctx, prefix, v3.WithPrefix(), v3.WithFilterPut())
	if wc == nil {
		return recipes.ErrNoWatcher
	}

	keys := getKeysFromResponse(response)

	// loop until channel error, channel closes or all keys are removed from map
	for {
		select {
		case keyChannel := <-wc:
			if err := keyChannel.Err(); err != nil {
				return err
			}
			for _, ev := range keyChannel.Events {
				delete(keys, string(ev.Kv.Key))
				if len(keys) == 0 {
					return nil
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func getKeysFromResponse(response *v3.GetResponse) map[string]string {
	keys := make(map[string]string, len(response.Kvs))
	for _, value := range response.Kvs {
		key := string(value.Key)
		keys[key] = key
	}
	return keys
}

//Unlock a previously acquired lock
func (rwm *RWMutex) Unlock() error {
	if rwm.myKey != nil {
		return rwm.myKey.Delete()
	}
	return errors.New("lock cannot be released because it was not acquired yet")
}
