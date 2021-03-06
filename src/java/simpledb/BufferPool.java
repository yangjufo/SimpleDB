package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final int TIMEOUT = 100; //seconds

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private final int numPages;
    private final Queue<Integer> countQueue = new LinkedList<>();
    private final Map<Integer, Integer> countIdMap = new HashMap<>();
    private final Map<Integer, Integer> idCountMap = new HashMap<>();
    private final Map<Integer, Page> idPageMap = new HashMap<>();
    private final Map<Integer, Page> discardedIdPageMap = new HashMap<>();
    private final Map<Integer, TransactionId> pageTransacationELockMap = new HashMap<>();
    private final Map<Integer, Set<TransactionId>> pageTransactionSLockMap = new HashMap<>();
    private final Map<TransactionId, Set<PageId>> transactionPageLockMap = new HashMap<>();
    private final Map<TransactionId, Long> transactionLockTimeMap = new HashMap<>();
    private int pageCount = 0;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(final int numPages) {
        this.numPages = numPages;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(final int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(final TransactionId tid, final PageId pid, final Permissions perm) throws TransactionAbortedException, DbException {
        pageCount += 1;
        final int pidKey = pid.hashCode();
        if (!idPageMap.containsKey(pidKey)) {
            while (idPageMap.size() == numPages) {
                evictPage();
            }
            idPageMap.put(pidKey, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
        }
        if (idCountMap.containsKey(pidKey)) {
            final int oldcount = idCountMap.get(pidKey);
            countQueue.remove(oldcount);
            countIdMap.remove(oldcount);
        }
        idCountMap.put(pidKey, pageCount);
        countIdMap.put(pageCount, pidKey);
        countQueue.add(pageCount);
        if (perm == Permissions.READ_ONLY) {
            synchronized (pageTransactionSLockMap) {
                synchronized (pageTransacationELockMap) {
                    synchronized (transactionPageLockMap) {
                        synchronized (transactionLockTimeMap) {
                            if (!transactionLockTimeMap.containsKey(tid)) {
                                transactionLockTimeMap.put(tid, new Date().getTime());
                            }
                            while (pageTransacationELockMap.containsKey(pidKey) && !pageTransacationELockMap.get(pidKey).equals(tid)) {
                                if (TimeUnit.SECONDS.toSeconds((new Date().getTime()) - transactionLockTimeMap.get(tid)) > TIMEOUT) {
                                    throw new TransactionAbortedException();
                                }
                            }
                            final Set<TransactionId> transactions = pageTransactionSLockMap.getOrDefault(pidKey, new HashSet<>());
                            if (!transactions.contains(tid)) {
                                transactions.add(tid);
                                pageTransactionSLockMap.put(pidKey, transactions);
                                if (!transactionPageLockMap.containsKey(tid)) {
                                    transactionPageLockMap.put(tid, new HashSet<>());
                                }
                                transactionPageLockMap.get(tid).add(pid);
                            }
                        }
                    }
                }
            }
        } else if (perm == Permissions.READ_WRITE) {
            synchronized (pageTransactionSLockMap) {
                synchronized (pageTransacationELockMap) {
                    synchronized (transactionPageLockMap) {
                        synchronized (transactionLockTimeMap) {
                            if (!transactionLockTimeMap.containsKey(tid)) {
                                transactionLockTimeMap.put(tid, new Date().getTime());
                            }
                            while ((pageTransacationELockMap.containsKey(pidKey) && !pageTransacationELockMap.get(pidKey).equals(tid))
                                    || (pageTransactionSLockMap.containsKey(pidKey)
                                    && (pageTransactionSLockMap.get(pidKey).size() > 1
                                    || (pageTransactionSLockMap.get(pidKey).size() == 1 && !pageTransactionSLockMap.get(pidKey).contains(tid))))) {
                                if (TimeUnit.SECONDS.toSeconds((new Date().getTime()) - transactionLockTimeMap.get(tid)) > TIMEOUT) {
                                    throw new TransactionAbortedException();
                                }
                            }
                            pageTransacationELockMap.put(pidKey, tid);
                            if (!transactionPageLockMap.containsKey(tid)) {
                                transactionPageLockMap.put(tid, new HashSet<>());
                            }
                            transactionPageLockMap.get(tid).add(pid);
                        }
                    }
                }
            }
        }
        return idPageMap.get(pidKey);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(final TransactionId tid, final PageId pid) {
        synchronized (pageTransactionSLockMap) {
            synchronized (pageTransacationELockMap) {
                synchronized (transactionPageLockMap) {
                    final int pidKey = pid.hashCode();
                    pageTransacationELockMap.remove(pidKey);
                    if (pageTransactionSLockMap.containsKey(pidKey)) {
                        pageTransactionSLockMap.get(pidKey).remove(tid);
                        if (pageTransactionSLockMap.get(pidKey).isEmpty()) {
                            pageTransactionSLockMap.remove(pidKey);
                        }
                    }
                    if (transactionPageLockMap.containsKey(tid)) {
                        transactionPageLockMap.get(tid).remove(pid);
                    }
                }
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(final TransactionId tid) throws IOException {
        synchronized (pageTransactionSLockMap) {
            synchronized (pageTransacationELockMap) {
                synchronized (transactionPageLockMap) {
                    if (transactionPageLockMap.containsKey(tid)) {
                        final Set<PageId> pages = new HashSet<>(transactionPageLockMap.get(tid));
                        pages.forEach(pid -> releasePage(tid, pid));
                        transactionPageLockMap.remove(tid);
                    }
                }
            }
        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(final TransactionId tid, final PageId p) {
        synchronized (transactionPageLockMap) {
            return transactionPageLockMap.containsKey(tid) && transactionPageLockMap.get(tid).contains(p);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(final TransactionId tid, final boolean commit)
            throws IOException {
        if (commit) {
            flushPages(tid);
        } else {
            synchronized (transactionPageLockMap) {
                synchronized (pageTransacationELockMap) {
                    if (transactionPageLockMap.containsKey(tid)) {
                        for (PageId pid : transactionPageLockMap.get(tid)) {
                            if (pageTransacationELockMap.containsKey(pid.hashCode())) {
                                discardPage(pid);
                            }
                        }
                    }
                }
            }
        }
        transactionComplete(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(final TransactionId tid, final int tableId, final Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        final ArrayList<Page> modifiedPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            idPageMap.put(page.getId().hashCode(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(final TransactionId tid, final Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        final ArrayList<Page> modifiedPages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        modifiedPages.forEach(page -> page.markDirty(true, tid));
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            idPageMap.put(page.getId().hashCode(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : idPageMap.values()) {
            flushPage(page.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(final PageId pid) {
        final int pidKey = pid.hashCode();
        if (idCountMap.containsKey(pidKey)) {
            final int count = idCountMap.remove(pidKey);
            countIdMap.remove(count);
            countQueue.remove(count);
        }
        discardedIdPageMap.put(pidKey, idPageMap.remove(pidKey));
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(final PageId pid) throws IOException {
        final int pidKey = pid.hashCode();
        if (idPageMap.containsKey(pidKey)) {
            final Page page = idPageMap.get(pidKey);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(final TransactionId tid) throws IOException {
        synchronized (transactionPageLockMap) {
            for (PageId pid : transactionPageLockMap.get(tid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        if (countQueue.isEmpty()) {
            throw new DbException("No page to evict!");
        }
        int count = countQueue.poll();
        int pid = countIdMap.get(count);
        Page page = idPageMap.get(pid);
        int dirtyCount = 0;
        while (page.isDirty() != null && dirtyCount != countIdMap.size()) {
            dirtyCount += 1;
            countQueue.add(count);
            count = countQueue.poll();
            pid = countIdMap.get(count);
            page = idPageMap.get(pid);
        }
        if (dirtyCount == countIdMap.size()) {
            countQueue.add(count);
            throw new DbException("No non-dirty pages!");
        }
        idCountMap.remove(pid);
        idPageMap.remove(pid);
        countIdMap.remove(count);
    }

}
