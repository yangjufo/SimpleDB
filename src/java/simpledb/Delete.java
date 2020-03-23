package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;
    private final OpIterator opIterator;
    private List<OpIterator> children = new ArrayList<>();
    private Tuple deleteCountTuple = null;
    private boolean first = true;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(final TransactionId t, final OpIterator child) {
        transactionId = t;
        opIterator = child;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        if (deleteCountTuple == null) {
            int deleteCount = 0;
            deleteCountTuple = new Tuple(getTupleDesc());
            opIterator.open();
            while (opIterator.hasNext()) {
                try {
                    Database.getBufferPool().deleteTuple(transactionId, opIterator.next());
                    deleteCount += 1;
                } catch (IOException e) {
                    throw new DbException("Failed to delete tuple!");
                }
            }
            opIterator.close();
            deleteCountTuple.setField(0, new IntField(deleteCount));
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (first) {
            first = false;
            return deleteCountTuple;
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
