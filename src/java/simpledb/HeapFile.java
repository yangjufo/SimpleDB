package simpledb;

import java.io.*;
import java.util.*;

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

    private final File file;
    private final TupleDesc tupleDesc;
    private final int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(final File f, final TupleDesc td) {
        file = f;
        tupleDesc = td;
        numPages = (int) (file.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(final PageId pid) {
        try {
            final RandomAccessFile file = new RandomAccessFile(this.file, "r");
            file.seek(pid.getPageNumber() * BufferPool.getPageSize());
            final byte[] bytes = new byte[BufferPool.getPageSize()];
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
    public void writePage(final Page page) throws IOException {
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
    public ArrayList<Page> insertTuple(final TransactionId tid, final Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(final TransactionId tid, final Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(final TransactionId tid) {
        return new DbFileIterator() {
            HeapPage heapPage;
            int pageNumber = 0;
            Iterator<Tuple> pageIterator;
            boolean open = false;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                heapPage = null;
                pageNumber = -1;
                open = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!open) {
                    return false;
                }
                if (heapPage == null || !pageIterator.hasNext()) {
                    if (pageNumber < numPages - 1) {
                        pageNumber += 1;
                        heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNumber), Permissions.READ_ONLY);
                        pageIterator = heapPage.iterator();
                        return pageIterator.hasNext();
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
                close();
                open();
            }

            @Override
            public void close() {
                open = false;
                heapPage = null;
                pageIterator = null;
                pageNumber = -1;
            }
        };
    }

}

