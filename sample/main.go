package main

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"log"
	"time"

	"github.com/flaviostutz/etcd-lock/etcdlock"
)

func main() {
	log.Println("CREATE CLIENT")
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"etcd0:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	log.Println("CREATE SESSION")
	//send TTL updates to server each 1s. If failed to send (client is down or without communications), lock will be released
	s1, err := concurrency.NewSession(cli, concurrency.WithTTL(1))
	if err != nil {
		log.Fatal(err)
	}
	defer s1.Close()
	// s2, err := concurrency.NewSession(cli, concurrency.WithTTL(1))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer s2.Close()
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

	log.Println("PREPARE MUTEXES")
	m1 := etcdlock.NewRWMutex(s1, "/mylocks/a")
	m2 := etcdlock.NewRWMutex(s1, "/mylocks/a")
	m3 := etcdlock.NewRWMutex(s3, "/mylocks/a")
	m4 := etcdlock.NewRWMutex(s4, "/mylocks/a")

	log.Println("LOCK1")
	go func() {
		log.Println("waiting lock r1")
		d := time.Now().Add(2000 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), d)
		defer cancel()
		if err := m1.RLock(ctx); err != nil {
			log.Fatal("r1 " + err.Error())
		}
		log.Println("got lock r1")
		time.Sleep(time.Duration(500) * time.Millisecond)
		if err := m1.Unlock(); err != nil {
			log.Fatal("unlock r1 " + err.Error())
		}
		log.Println("released rlock for r1")
	}()

	log.Println("LOCK2")
	go func() {
		time.Sleep(time.Duration(100) * time.Millisecond)
		log.Println("waiting lock r2")
		d := time.Now().Add(2000 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), d)
		defer cancel()
		if err := m2.RLock(ctx); err != nil {
			log.Fatal("r2 " + err.Error())
		}
		log.Println("got lock r2")
		time.Sleep(time.Duration(500) * time.Millisecond)
		if err := m2.Unlock(); err != nil {
			log.Fatal("unlock r2 " + err.Error())
		}
		log.Println("released rlock for r2")
	}()

	log.Println("LOCK3")
	go func() {
		time.Sleep(time.Duration(200) * time.Millisecond)
		log.Println("waiting lock rw3")
		d := time.Now().Add(2000 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), d)
		defer cancel()
		if err := m3.RWLock(ctx); err != nil {
			log.Fatal("rw3 " + err.Error())
		}
		log.Println("got lock rw3")
		time.Sleep(time.Duration(500) * time.Millisecond)
		if err := m3.Unlock(); err != nil {
			log.Fatal("unlock rw3 " + err.Error())
		}
		log.Println("released rlock for rw3")
	}()

	log.Println("LOCK4")
	go func() {
		time.Sleep(time.Duration(300) * time.Millisecond)
		log.Println("waiting lock r4")
		d := time.Now().Add(2000 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), d)
		defer cancel()
		if err := m4.RLock(ctx); err != nil {
			log.Fatal("r4 " + err.Error())
		}
		log.Println("got lock r4")
		time.Sleep(time.Duration(500) * time.Millisecond)
		if err := m4.Unlock(); err != nil {
			log.Fatal("unlock r4 " + err.Error())
		}
		log.Println("released rlock for r4")
	}()

	time.Sleep(time.Duration(10000) * time.Millisecond)
}
