package etcdlock

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"log"
	"strings"
	"testing"
	"time"
)

func TestLockMustBeGrantedToTheNextOnQueue(t *testing.T) {
	log.Println("--------> LockMustBeGrantedToTheNextOnQueue started")
	log.Println("Lock must be granted to the next register on queue")

	const key = "/key/to/lock"
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer etcdClient.Close()
	sessions := createEtcdSessions(etcdClient, 4, 1)
	defer sessions[0].Close()
	defer sessions[1].Close()

	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	mutex := NewRWMutex(sessions[0], key)
	if err := mutex.RWLock(ctx); err != nil {
		t.Fatal(fmt.Sprintf("key %s write lock error %s\n", key, err.Error()))
	}
	log.Printf("key %s locked successful for write\n", key)

	go func() {
		log.Println("lock will be release in 2 seconds")
		time.Sleep(2 * time.Second)
		if err := mutex.Unlock(); err != nil {
			t.Fatal(fmt.Sprintf("key %s write unlock error %s\n", key, err.Error()))
		}
		log.Printf("key %s unlocked successful for write\n", key)
	}()

	go func() {
		log.Println("putting first on queue")
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		mutex := NewRWMutex(sessions[1], key)
		if err := mutex.RWLock(ctx); err != nil {
			t.Fatal(fmt.Sprintf("the first to be registered must get the lock. key %s write lock error %s\n", key, err.Error()))
		}
		log.Println("the first register got the lock as expected")
	}()

	go func() {
		time.Sleep(time.Second) //wait the first one
		log.Println("putting second on queue")
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		mutex := NewRWMutex(sessions[2], key)
		if err := mutex.RWLock(ctx); err != nil {
			log.Println("got error as expected for the second")
		} else {
			t.Fatal("the first to be registered must get the lock but the second one got it.")
		}
	}()

	go func() {
		time.Sleep(1500 * time.Millisecond) //wait the second one
		log.Println("putting third on queue")
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		mutex := NewRWMutex(sessions[3], key)
		if err := mutex.RWLock(ctx); err != nil {
			log.Println("got error as expected for the third")
		} else {
			t.Fatal("the first to be registered must get the lock but the third one got it.")
		}
	}()

	time.Sleep(5 * time.Second)

	log.Println("--------> LockMustBeGrantedToTheNextOnQueue finished successfully")
}

func TestOnlyOneWriteLockForKeyIsAllowedAtaTime(t *testing.T) {
	log.Println("--------> OnlyOneWriteLockForKeyIsAllowedAtaTime started")
	log.Println("Must not be possible to get write lock until unlock all read locks")

	const key = "/key/to/lock"
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer etcdClient.Close()
	sessions := createEtcdSessions(etcdClient, 2, 1)

	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	mutex := NewRWMutex(sessions[0], key)
	if err := mutex.RWLock(ctx); err != nil {
		t.Fatal(fmt.Sprintf("key %s write lock error %s\n", key, err.Error()))
	}
	log.Printf("key %s locked successful for write\n", key)
	go func() {
		log.Printf("waiting 3 seconds to unlock write lock for key %s\n", key)
		time.Sleep(3 * time.Second)
		if err := mutex.Unlock(); err != nil {
			t.Fatal(fmt.Sprintf("key %s write unlock error %s\n", key, err.Error()))
		}
		log.Printf("key %s unlocked successful for write\n", key)
	}()

	log.Println("Trying to get lock before previous unlock. Error is expected")
	ctx2, _ := context.WithTimeout(context.Background(), time.Second)
	mutex2 := NewRWMutex(sessions[1], key)
	if err := mutex2.RWLock(ctx2); err != nil {
		log.Println("got error as expected")
	} else {
		t.Fatal(fmt.Sprintf("Must not be possible to get write lock for key %s when alredy exists locks for this key", key))
	}

	log.Println("wait 3 seconds for the unlock and retry to get new lock")
	time.Sleep(3 * time.Second)
	ctx3, _ := context.WithTimeout(context.Background(), time.Second)
	if err := mutex2.RWLock(ctx3); err != nil {
		t.Fatal(fmt.Sprintf("key %s write unlock error %s\n", key, err.Error()))
	}
	_ = mutex2.Unlock()
	log.Println("--------> OnlyOneWriteLockForKeyIsAllowedAtaTime finished successfully")
}

func TestGetWriteLockAfterAllReadLockRelease(t *testing.T) {
	log.Println("--------> GetWriteLockAfterAllReadLockRelease started")
	log.Println("Must not be possible to get write lock until unlock all read locks")

	const key = "/key/to/lock"
	const sessionTTLSeconds = 1
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer etcdClient.Close()
	etcdReadSessions := createEtcdSessions(etcdClient, 4, sessionTTLSeconds)
	etcdWriteSessions := createEtcdSessions(etcdClient, 1, sessionTTLSeconds)

	//Get lock for other keys to make sure that it will not interfere
	session, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(sessionTTLSeconds))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	ctxAux, _ := context.WithTimeout(context.Background(), time.Second)
	readAuxMutex := NewRWMutex(session, "key/a")
	if err := readAuxMutex.RLock(ctxAux); err != nil {
		t.Fatal(fmt.Sprintf("read lock error %s\n", err.Error()))
	}
	ctxAux, _ = context.WithTimeout(context.Background(), time.Second)
	writeAuxMutex := NewRWMutex(session, "key/b")
	if err := writeAuxMutex.RWLock(ctxAux); err != nil {
		t.Fatal(fmt.Sprintf("write lock error %s\n", err.Error()))
	}
	//aux locks end

	log.Printf("Getting 4 read locks for key %s with different etcd session\n", key)
	var mutexesRead []*RWMutex
	for _, s := range etcdReadSessions {
		mutexesRead = append(mutexesRead, createMutex(s, key, 1)...)
	}
	for i, mutex := range mutexesRead {
		ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := mutex.RLock(ctx); err != nil {
			t.Fatal(fmt.Sprintf("mutex %d rlock error %s\n", i, err.Error()))
		}
		log.Printf("read lock %d successful\n", i)
	}

	writeMutex := createMutex(etcdWriteSessions[0], key, 1)[0]

	log.Println("Unlock one read lock at a time and try to get write lock")
	for i := 0; i < len(mutexesRead); i++ {
		if err := mutexesRead[i].Unlock(); err != nil {
			t.Fatal(fmt.Sprintf("mutex %d rUnlock error %s\n", i, err.Error()))
		}
		log.Printf("unlock read lock %d successful\n", i)

		log.Println("Trying write lock for 1 second")
		ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
		if i+1 == len(mutexesRead) {
			log.Println("All read locks are gone, write lock must not get error")
			if err := writeMutex.RWLock(ctx); err != nil {
				log.Println("Error: " + err.Error())
				t.Fatal(fmt.Sprintf("Must be possible to get write lock for key %s when there's no others locks for this key", key))
			} else {
				log.Printf("got write lock for key %s as expected", key)
				_ = writeMutex.Unlock()
			}
		} else {
			log.Println("Error is expected")
			if err := writeMutex.RWLock(ctx); err != nil {
				log.Println("got error as expected")
			} else {
				t.Fatal(fmt.Sprintf("Must not be possible to get write lock for key %s when alredy exists locks for this key", key))
			}
		}
	}

	log.Print()
	log.Print("Same test but unlocking in reverse order")
	for i, mutex := range mutexesRead {
		ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := mutex.RLock(ctx); err != nil {
			t.Fatal(fmt.Sprintf("mutex %d read lock error %s\n", i, err.Error()))
		}
		log.Printf("read lock %d successful\n", i)
	}

	log.Println("Unlock one read lock at a time in reverse order and try to get write lock")
	for i := len(mutexesRead) - 1; i >= 0; i-- {
		if err := mutexesRead[i].Unlock(); err != nil {
			t.Fatal(fmt.Sprintf("mutex %d rUnlock error %s\n", i, err.Error()))
		}
		log.Printf("unlock read lock %d successful\n", i)

		log.Println("Trying write lock for 1 second")
		ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
		if i == 0 {
			log.Println("All read locks are gone, write lock must not get error")
			if err := writeMutex.RWLock(ctx); err != nil {
				log.Println("Error: " + err.Error())
				t.Fatal(fmt.Sprintf("Must be possible to get write lock for key %s when there's no others locks for this key", key))
			} else {
				log.Printf("got write lock for key %s as expected", key)
				_ = writeMutex.Unlock()
			}
		} else {
			log.Println("Error is expected")
			if err := writeMutex.RWLock(ctx); err != nil {
				log.Println("got error as expected")
			} else {
				t.Fatal(fmt.Sprintf("Must not be possible to get write lock for key %s when alredy exists locks for this key", key))
			}
		}
	}
	log.Println("--------> GetWriteLockAfterAllReadLockRelease finished successfully")
}

func TestDeadLineExceededError(t *testing.T) {
	log.Println("--------> DeadLineExceededError started")
	log.Println("Must get 'context deadline exceeded' error when the context parameter passed to RLock or RWLock have deadline or timeout and the time is reached")
	const key = "/key/to/lock"
	const sessionTTLSeconds = 1
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer etcdClient.Close()
	session, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(sessionTTLSeconds))
	if err != nil {
		t.Fatal(err)
	}

	ctxSuccess, _ := context.WithTimeout(context.Background(), time.Second)
	readLockSuccess := NewRWMutex(session, key)
	if err := readLockSuccess.RLock(ctxSuccess); err != nil {
		t.Fatal(fmt.Sprintf("lockSuccess read lock error %s\n", err.Error()))
	}
	log.Printf("key %s locked successful for read\n", key)

	log.Printf("try write lock for key %s for 1 seconds\n", key)
	ctxError, _ := context.WithTimeout(context.Background(), time.Second)
	writeLockError := NewRWMutex(session, key)
	if err := writeLockError.RWLock(ctxError); err != nil {
		if strings.Contains(err.Error(), "deadline exceeded") {
			log.Printf("error occured as expected for key %s", key)
		} else {
			t.Fatal(fmt.Sprintf("expected error 'context deadline exceeded' but got %s", err.Error()))
		}
	} else {
		t.Fatal("expected error 'context deadline exceeded' but got nil")
	}

	log.Println("--------> DeadLineExceededError finished successfully")
}

func TestSyncMultiplesReadLock(t *testing.T) {
	log.Println("--------> SyncMultiplesReadLock started")
	log.Println("Must be able to do multiples read lock to the same key")
	const key = "/key/to/lock"
	const quantity = 10
	const sessionTTLSeconds = 1
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer etcdClient.Close()
	etcdSessions := createEtcdSessions(etcdClient, quantity, sessionTTLSeconds)

	log.Printf("getting syncronous %d locks for the key %s with same etcd session\n", quantity, key)
	mutexesSameSession := createMutex(etcdSessions[0], key, 10)
	log.Println("locking")
	for i, mutex := range mutexesSameSession {
		ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := mutex.RLock(ctx); err != nil {
			t.Fatal(fmt.Sprintf("mutex %d rlock error %s\n", i, err.Error()))
		}
		log.Printf("lock %d successful\n", i)
	}
	log.Println("unlocking")
	for i, mutex := range mutexesSameSession {
		if err := mutex.Unlock(); err != nil {
			t.Fatal(fmt.Sprintf("mutex %d rUnlock error %s\n", i, err.Error()))
		}
		log.Printf("unlock %d successful\n", i)
	}
	log.Println()

	log.Printf("getting syncronous %d locks for the key %s with different etcd session\n", quantity, key)
	var mutexesDifferentSession []*RWMutex
	for _, s := range etcdSessions {
		mutexesDifferentSession = append(mutexesDifferentSession, createMutex(s, key, 1)...)
	}

	log.Println("locking")
	for i, mutex := range mutexesDifferentSession {
		ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := mutex.RLock(ctx); err != nil {
			t.Fatal(fmt.Sprintf("mutex %d rlock error %s\n", i, err.Error()))
		}
		log.Printf("lock %d successful\n", i)
	}
	log.Println("unlocking")
	for i, mutex := range mutexesDifferentSession {
		if err := mutex.Unlock(); err != nil {
			t.Fatal(fmt.Sprintf("mutex %d rUnlock error %s\n", i, err.Error()))
		}
		log.Printf("unlock %d successful\n", i)
	}
	log.Println()

	log.Println("--------> SyncMultiplesReadLock finished successfully")
}

func createEtcdSessions(etcdClient *clientv3.Client, quantity, ttl int) []*concurrency.Session {
	var sessions []*concurrency.Session
	for i := 0; i < quantity; i++ {
		session, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(ttl))
		if err != nil {
			panic(err)
		}
		sessions = append(sessions, session)
	}
	return sessions
}

func createMutex(session *concurrency.Session, key string, quantity int) []*RWMutex {
	var mutexes []*RWMutex
	for i := 0; i < quantity; i++ {
		mutexes = append(mutexes, NewRWMutex(session, key))
	}
	return mutexes
}
