package simpledb;

import java.io.*;
import java.util.*;

import static java.lang.String.format;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        numPages = (int) (f.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile file = new RandomAccessFile(f, "r");
            file.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte[] bytes = new byte[BufferPool.getPageSize()];
            file.read(bytes);
            file.close();
            return new HeapPage((HeapPageId) pid, bytes);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("File not found");
        } catch (IOException e) {
            throw new IllegalArgumentException("Page not found");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            HeapPage page;
            int pgNo = 0;
            Iterator<Tuple> pageIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                try {
                    page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo), Permissions.READ_ONLY);
                    pageIterator = page.iterator();
                } catch (TransactionAbortedException e) {
                    throw e;
                } catch (Exception e) {
                    throw new DbException(String.format("Failed to open database: %s", f.getAbsoluteFile()));
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (page == null) {
                    return false;
                }
                if (!pageIterator.hasNext()) {
                    if (pgNo < numPages - 1) {
                        pgNo += 1;
                        page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo), Permissions.READ_ONLY);
                        pageIterator = page.iterator();
                    } else {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (hasNext()) {
                    return pageIterator.next();
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                try {
                    pgNo = 0;
                    page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo), Permissions.READ_ONLY);
                    pageIterator = page.iterator();
                } catch (TransactionAbortedException e) {
                    throw e;
                } catch (Exception e) {
                    throw new DbException(format("Failed to open database: %s", f.getAbsoluteFile()));
                }
            }

            @Override
            public void close() {
                page = null;
                pgNo = 0;
            }
        };
    }

}

