package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const (
	LOCKED   = "LOCKED"
	RELEASED = "RELEASED"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		err := lk.ck.Put(lk.key, LOCKED, 0)
		if err == rpc.OK {
			// acquire lock
			return
		}
	}
}

func (lk *Lock) Release() {
	// Your code here

	for {
		_, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			return
		}
		if err != rpc.OK {
			continue
		}
		putErr := lk.ck.Put(lk.key, RELEASED, version)
		if putErr == rpc.OK {
			return
		}
	}

}
