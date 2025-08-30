package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

// 使用k/v来存储锁的归属权，当使用get来查询当前锁属于谁，
// 如果为空或者是锁处于空闲态，就可以尝试put，将自己的ID写入kv，表示获取锁
// 释放锁就直接进行释放
type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	l    string
	wait string
	myID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	lk.l = l
	lk.wait = "00000000"
	lk.myID = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.l)
		if err == rpc.ErrNoKey {
			put_err := lk.ck.Put(lk.l, lk.myID, 0)
			if put_err == rpc.OK {
				return
			}
		} else if value == lk.wait {
			put_err := lk.ck.Put(lk.l, lk.myID, version)
			if put_err == rpc.OK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	_, version, _ := lk.ck.Get(lk.l)
	lk.ck.Put(lk.l, lk.wait, version)
}
