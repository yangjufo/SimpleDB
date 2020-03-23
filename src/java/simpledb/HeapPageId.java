package simpledb;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {

    private final int tableId;
    private final int pageNumber;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(final int tableId, final int pgNo) {
        this.tableId = tableId;
        pageNumber = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     * this PageId
     */
    public int getPageNumber() {
        return pageNumber;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     * the table number and the page number (needed if a PageId is used as a
     * key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        return (String.valueOf(tableId) + pageNumber).hashCode();
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    public boolean equals(final Object o) {
        return o instanceof PageId && tableId == ((PageId) o).getTableId() && pageNumber == ((PageId) o).getPageNumber();
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        final int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
