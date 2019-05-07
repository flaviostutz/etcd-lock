package etcdlock

import (
	"fmt"
	"log"
	"time"

	recipes "github.com/coreos/etcd/contrib/recipes"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

func exampleRWMutex_Lock() {
	log.Println("CREATE CLIENT")
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	log.Println("CREATE SESSIONS")

	// create separate sessions for lock competition
	s1, err := concurrency.NewSession(cli, concurrency.WithTTL(1))
	if err != nil {
		log.Fatal(err)
	}
	defer s1.Close()

	s2, err := concurrency.NewSession(cli, concurrency.WithTTL(1))
	if err != nil {
		log.Fatal(err)
	}
	defer s2.Close()

	s3, err := concurrency.NewSession(cli, concurrency.WithTTL(1))
	if err != nil {
		log.Fatal(err)
	}
	defer s3.Close()

	s4, err := concurrency.NewSession(cli, concurrency.WithTTL(1))
	if err != nil {
		log.Fatal(err)
	}
	defer s4.Close()

	s5, err := concurrency.NewSession(cli, concurrency.WithTTL(1))
	if err != nil {
		log.Fatal(err)
	}
	defer s5.Close()

	log.Println("PREPARE MUTEXES")

	//prepare
	m1 := recipes.NewRWMutex(s1, "/my-lock/a")
	m2 := recipes.NewRWMutex(s2, "/my-lock/a")
	m3 := recipes.NewRWMutex(s3, "/my-lock/a")
	m4 := recipes.NewRWMutex(s4, "/my-lock/a")
	m5 := recipes.NewRWMutex(s5, "/my-lock/a")

	log.Println("TRY LOCKS")
	//try locks

	log.Println("LOCK1")
	go func() {
		fmt.Println("waiting lock r1")
		if err := m1.RLock(); err != nil {
			log.Fatal("r1 " + err.Error())
		}
		fmt.Println("got lock r1")
		fmt.Println("orphaning s1 (will not renew TTL keepalive and will release lock by being absent)")
		s1.Orphan()
		// time.Sleep(time.Duration(2000) * time.Millisecond)
		// log.Println("runlock r1")
		// if err := m1.RUnlock(); err != nil {
		// 	log.Fatal("unlock r1 " + err.Error())
		// }
		// log.Println("released rlock for r1")
	}()

	log.Println("LOCK2")
	go func() {
		time.Sleep(time.Duration(100) * time.Millisecond)
		// wait until s1 is locks /my-lock/
		fmt.Println("waiting lock rw2")
		if err := m2.Lock(); err != nil {
			log.Fatal("rw2 " + err.Error())
		}
		fmt.Println("got lock rw2")
		time.Sleep(time.Duration(500) * time.Millisecond)
		fmt.Println("rwunlock rw2")
		if err := m2.Unlock(); err != nil {
			log.Fatal("unlock rw2 " + err.Error())
		}
		fmt.Println("released rwlock for rw2")
	}()

	log.Println("LOCK3")
	go func() {
		time.Sleep(time.Duration(200) * time.Millisecond)
		// wait until s1 is locks /my-lock/
		fmt.Println("waiting lock r3")
		if err := m3.RLock(); err != nil {
			log.Fatal("r3 " + err.Error())
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
		fmt.Println("got lock r3")
		time.Sleep(time.Duration(500) * time.Millisecond)
		fmt.Println("runlock r3")
		if err := m3.RUnlock(); err != nil {
			log.Fatal("unlock r3 " + err.Error())
		}
		fmt.Println("released rlock for r3")
	}()

	log.Println("LOCK4")
	go func() {
		time.Sleep(time.Duration(400) * time.Millisecond)
		// wait until s1 is locks /my-lock/
		fmt.Println("waiting lock r4")
		if err := m4.RLock(); err != nil {
			log.Fatal("r4 " + err.Error())
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
		fmt.Println("got lock r4")
		time.Sleep(time.Duration(700) * time.Millisecond)
		fmt.Println("closing r4 session (will release lock because lease will be removed immediatelly)")
		s4.Close()
		// log.Println("runlock r4")
		// if err := m4.RUnlock(); err != nil {
		// 	log.Fatal("unlock r4 " + err.Error())
		// }
		// log.Println("released rlock for r4")
	}()

	log.Println("LOCK5")
	go func() {
		time.Sleep(time.Duration(500) * time.Millisecond)
		// wait until s1 is locks /my-lock/
		fmt.Println("waiting lock rw5")
		if err := m5.Lock(); err != nil {
			log.Fatal("rw5 " + err.Error())
		}
		fmt.Println("got lock rw5")
		time.Sleep(time.Duration(500) * time.Millisecond)
		fmt.Println("runlock rw5")
		if err := m5.Unlock(); err != nil {
			log.Fatal("unlock rw5 " + err.Error())
		}
		fmt.Println("released rlock for rw5")
	}()

	time.Sleep(time.Duration(5000) * time.Millisecond)

	// Output:
	// waiting lock r1
	// got lock r1
	// orphaning s1 (will not renew TTL keepalive and will release lock by being absent)
	// waiting lock rw2
	// waiting lock r3
	// waiting lock r4
	// waiting lock rw5
	// got lock rw2
	// rwunlock rw2
	// released rwlock for rw2
	// got lock r3
	// got lock r4
	// runlock r3
	// released rlock for r3
	// closing r4 session (will release lock because lease will be removed immediatelly)
	// got lock rw5
	// runlock rw5
	// released rlock for rw5
}
