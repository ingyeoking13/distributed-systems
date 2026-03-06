package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	id       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.id = kvtest.RandValue(8)
	lk.lockname = lockname
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, _, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			err := lk.ck.Put(lk.lockname, lk.id, 0)
			if err == rpc.OK {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if rpc.OK == err && val == lk.id {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			break
		}
		if err != rpc.OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if val != lk.id {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err = lk.ck.Put(lk.lockname, "del", version)
		if err == rpc.OK {
			break
		}

		time.Sleep(100 * time.Millisecond)
		println("sllepp")
	}
}
