package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate predicate;
    private final OpIterator opIterator;
    private ArrayList<OpIterator> children = new ArrayList<>();

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(final Predicate p, final OpIterator child) {
        predicate = p;
        opIterator = child;
        children.add(child);
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        return opIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        opIterator.open();
    }

    public void close() {
        super.close();
        opIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        opIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (opIterator.hasNext()) {
            final Tuple tuple = opIterator.next();
            if (predicate.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return children.toArray(OpIterator[]::new);
    }

    @Override
    public void setChildren(final OpIterator[] children) {
        this.children = (ArrayList<OpIterator>) Arrays.asList(children);
    }

}
