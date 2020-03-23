package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;
    private final OpIterator opIterator;
    private final int tableId;
    private Tuple insertCountTuple = null;
    private List<OpIterator> children = new ArrayList<>();
    private boolean first = true;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("The TupleDesc of child differ from the table to be inserted!");
        }
        transactionId = t;
        opIterator = child;
        this.tableId = tableId;
        children.add(child);
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        if (insertCountTuple == null) {
            int insertCount = 0;
            insertCountTuple = new Tuple(getTupleDesc());
            opIterator.open();
            while (opIterator.hasNext()) {
                try {
                    Database.getBufferPool().insertTuple(transactionId, tableId, opIterator.next());
                    insertCount += 1;
                } catch (IOException e) {
                    throw new DbException("Failed to insert tuple!");
                }
            }
            opIterator.close();
            insertCountTuple.setField(0, new IntField(insertCount));
        }
        first = true;
    }

    public void close() {
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (first) {
            first = false;
            return insertCountTuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return children.toArray(OpIterator[]::new);
    }

    @Override
    public void setChildren(final OpIterator[] children) {
        this.children = Arrays.asList(children);
    }
}
