package simpledb.transaction;

import simpledb.storage.PageId;

public class TxLock {
    private TransactionId transactionId;
    private PageId pageId;
    private boolean isShared;

    public TxLock(TransactionId transactionId, PageId pageId, boolean isShared) {
        this.transactionId = transactionId;
        this.pageId = pageId;
        this.isShared = isShared;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public PageId getPageId() {
        return pageId;
    }

    public boolean isShared() {
        return isShared;
    }

    public void setShared(boolean shared) {
        isShared = shared;
    }
}
