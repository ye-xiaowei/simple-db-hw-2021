package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private final File file;

    /**
     * 类似与 schema
     */
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (!Objects.equals(getId(), pid.getTableId())) {
            throw new IllegalArgumentException("Page does not exist in this file.");
        }
        try (RandomAccessFile read = new RandomAccessFile(file, "r")) {
            int pageSize = BufferPool.getPageSize();
            byte[] data = new byte[pageSize];
            int off = pid.getPageNumber() * pageSize;
            read.seek(off);
            read.read(data);
            return new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (!Objects.equals(getId(), page.getId().getTableId())) {
            throw new IllegalArgumentException("Page does not exist in this file.");
        }
        try (RandomAccessFile read = new RandomAccessFile(file, "rw")) {
            int pageSize = BufferPool.getPageSize();
            int off = page.getId().getPageNumber() * pageSize;
            read.seek(off);
            read.write(page.getPageData());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // 找现在的page看看有没有空位
        int numPages = numPages();
        for (int i = 0; i < numPages; i++) {
            HeapPage page = getPageFromPool(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                return List.of(page);
            }
        }
        // 没有空位只能，重新写一页了
        HeapPage page = getPageFromPool(tid, new HeapPageId(getId(), numPages), Permissions.READ_WRITE);
        page.insertTuple(t);
        page.markDirty(true, tid);
        writePage(page);
        return List.of(page);
    }

    private HeapPage getPageFromPool(TransactionId tid, PageId pageId, Permissions permissions)
            throws TransactionAbortedException, DbException {
        return (HeapPage) Database.getBufferPool().getPage(tid, pageId, permissions);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = getPageFromPool(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        return List.of(page);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapIterator(tid);
    }

    private class HeapIterator implements DbFileIterator {
        private final TransactionId tid;
        private Iterator<Tuple> tupleIterator;
        private int pageNo = -1;
        private boolean isOpen;

        public HeapIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                return false;
            }
            if (pageNo == -1) {
                pageNo = 0;
                tupleIterator = ((HeapPage) Database.getBufferPool().getPage(
                        tid,
                        new HeapPageId(getId(), pageNo),
                        Permissions.READ_ONLY)).iterator();
            }
            if (tupleIterator.hasNext()) {
                return true;
            }
            pageNo++;
            if (numPages() <= pageNo) {
                return false;
            }
            tupleIterator = ((HeapPage) Database.getBufferPool().getPage(
                    tid,
                    new HeapPageId(getId(), pageNo),
                    Permissions.READ_ONLY)).iterator();
            return tupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                throw new NoSuchElementException();
            }
            if (hasNext()) {
                return tupleIterator.next();
            }
            return null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageNo = -1;
        }

        @Override
        public void close() {
            isOpen = false;
        }
    }
}

