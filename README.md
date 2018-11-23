# etcd-lock
Distributed locks using ETCD server. Currently have support for Read Write locks with Lock TTL and acquire Lock deadline

## Lock features
  * Lock resources are identified by a name
  * Distributed Read Write Locks works the same way as any other read write locks, but it uses an ETCD server for making it distributed
  * A session will identify the client. If it gets disconnected (process crash, for example), the lock will be released on the server and another client may get its lock.
  * During lock acquirement, you can pass a Context with deadline or invoke a "cancel()" to skip long running lock acquirements

## Example

```
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

log.Println("PREPARE MUTEXES")
m1 := etcdlock.NewRWMutex(s1, "/mylocks/a")
m2 := etcdlock.NewRWMutex(s2, "/mylocks/a")
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
```

Output:
```
PREPARE MUTEXES
LOCK1
LOCK2
LOCK3
LOCK4
waiting lock r1
got lock r1
waiting lock r2
got lock r2
waiting lock rw3
waiting lock r4
released rlock for r1
released rlock for r2
got lock rw3
released rlock for rw3
got lock r4
released rlock for r4
```
  * enter "sample" dir
  * run ```docker-compose up --build```
