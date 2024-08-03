package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

public class LockManager {

    private final Map<TransactionId, Set<PageId>> transactionIdPageMap = new HashMap<>();
    private final Map<PageId, Deque<TxLock>> lockMap = new HashMap<>();

    public void acquire(PageId id,
                        TransactionId transactionId,
                        boolean isShared,
                        long timeout) throws TimeoutException {
        long deadline = System.currentTimeMillis() + timeout;
        while (!tryAcquire(id, transactionId, isShared)) {
            if (timeout > 0 && deadline < System.currentTimeMillis()) {
                throw new TimeoutException();
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
        }
    }

    public synchronized boolean tryAcquire(PageId id, TransactionId transactionId, boolean isShared) {
        Deque<TxLock> txLocks = lockMap.get(id);
        // 1. 该 page 还没有锁
        if (txLocks == null || txLocks.isEmpty()) {
            Deque<TxLock> deque = new ArrayDeque<>();
            deque.addLast(new TxLock(transactionId, id, isShared));
            transactionIdPageMap.compute(transactionId, (k, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(id);
                return v;
            });
            lockMap.put(id, deque);
            return true;
        }
        // 2. 该 page 有锁，而且属于是重入
        for (TxLock txLock : txLocks) {
            if (Objects.equals(txLock.getTransactionId(), transactionId)) {
                // 2.1 同样的锁类型获取已经获取独占锁，直接可重入
                if (txLock.isShared() == isShared ||
                        !txLock.isShared()) {
                    return true;
                }
                // 2.2 获取了共享锁，但是队列里面就只有他一个锁，锁升级
                if (txLocks.size() == 1) {
                    txLock.setShared(false);
                    return true;
                }
                return false;
            }
        }
        // 3. 该 page 有锁而且冲突了
        for (TxLock txLock : txLocks) {
            if (!txLock.isShared() || !isShared) {
                return false;
            }
        }
        txLocks.addLast(new TxLock(transactionId, id, isShared));
        transactionIdPageMap.compute(transactionId, (k, v) -> {
            if (v == null) {
                v = new HashSet<>();
            }
            v.add(id);
            return v;
        });
        return true;
    }

    public synchronized boolean releaseAll(TransactionId transactionId) {
        Set<PageId> pageIds = transactionIdPageMap.get(transactionId);
        if (pageIds == null || pageIds.isEmpty()) {
            return true;
        }
        for (PageId pageId : pageIds) {
            Deque<TxLock> txLocks = lockMap.get(pageId);
            if (txLocks == null) {
                continue;
            }
            Iterator<TxLock> iterator = txLocks.iterator();
            while (iterator.hasNext()) {
                TxLock next = iterator.next();
                if (!next.getTransactionId().equals(transactionId)) {
                    continue;
                }
                iterator.remove();
                break;
            }
            if (txLocks.isEmpty()) {
                lockMap.remove(pageId);
            }
        }
        return true;
    }

    public synchronized boolean release(PageId id, TransactionId transactionId) {
        Deque<TxLock> txLocks = lockMap.get(id);
        if (txLocks.isEmpty()) {
            return true;
        }
        Iterator<TxLock> iterator = txLocks.iterator();
        while (iterator.hasNext()) {
            TxLock next = iterator.next();
            if (!next.getTransactionId().equals(transactionId)) {
                continue;
            }
            Set<PageId> pageIds = transactionIdPageMap.get(transactionId);
            pageIds.remove(next.getPageId());
            iterator.remove();
            if (pageIds.isEmpty()) {
                transactionIdPageMap.remove(transactionId);
            }
            break;
        }
        if (txLocks.isEmpty()) {
            lockMap.remove(id);
        }
        return true;
    }

    public synchronized boolean isHoldLock(PageId id, TransactionId transactionId) {
        Deque<TxLock> txLocks = lockMap.get(id);
        if (txLocks == null || txLocks.isEmpty()) {
            return false;
        }
        for (TxLock txLock : txLocks) {
            if (txLock.getTransactionId().equals(transactionId)) {
                return true;
            }
        }
        return false;
    }

    public synchronized void reset() {
        transactionIdPageMap.clear();
        lockMap.clear();
    }

    public synchronized Set<PageId> getPagesByTxid(TransactionId transactionId) {
        return transactionIdPageMap.get(transactionId);
    }
}
