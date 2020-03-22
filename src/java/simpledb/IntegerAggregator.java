package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByField;
    private final int aggregateField;
    private final Op aggregateOperator;
    private int aggregatedCount;
    private final TupleDesc aggregatedTupleDesc;
    private final Map<Field, Tuple> aggregatedTuples = new HashMap<>();

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(final int gbfield, final Type gbfieldtype, final int afield, final Op what) {
        groupByField = gbfield;
        aggregateField = afield;
        aggregateOperator = what;
        aggregatedCount = 0;
        if (groupByField == NO_GROUPING) {
            aggregatedTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            aggregatedTupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(final Tuple tup) {
        final Tuple mergedTuple = aggregatedTuples.getOrDefault(groupByField == NO_GROUPING ? null : tup.getField(groupByField),
                new Tuple(aggregatedTupleDesc));
        IntField aggregatedValue = (IntField) (groupByField == NO_GROUPING ? mergedTuple.getField(0) : mergedTuple.getField(1));
        final IntField newValue = (IntField) tup.getField(aggregateField);
        switch (aggregateOperator) {
            case MIN:
                aggregatedValue = aggregatedValue == null ? newValue : aggregatedValue.compare(Predicate.Op.LESS_THAN, newValue) ? aggregatedValue : newValue;
                break;
            case MAX:
                aggregatedValue = aggregatedValue == null ? newValue : aggregatedValue.compare(Predicate.Op.GREATER_THAN,
                        newValue) ? aggregatedValue : newValue;
                break;
            case AVG:
                aggregatedValue = aggregatedValue == null ? newValue : new IntField(
                        (aggregatedValue.getValue() * aggregatedCount + newValue.getValue()) / (aggregatedCount + 1));
                break;
            case SUM:
                aggregatedValue = aggregatedValue == null ? newValue : new IntField(aggregatedValue.getValue() + newValue.getValue());
                break;
            case COUNT:
                aggregatedValue = aggregatedValue == null ? new IntField(1) : new IntField(aggregatedCount + 1);
                break;
            case SC_AVG:
            case SUM_COUNT:
                break;
        }
        mergedTuple.setField(groupByField == NO_GROUPING ? 0 : 1, aggregatedValue);
        if (groupByField != NO_GROUPING) {
            mergedTuple.setField(0, tup.getField(groupByField));
        }
        aggregatedTuples.put(groupByField == NO_GROUPING ? null : tup.getField(groupByField), mergedTuple);
        aggregatedCount += 1;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            Iterator<Tuple> aggregatedIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                aggregatedIterator = aggregatedTuples.values().iterator();

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return aggregatedIterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return aggregatedIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return aggregatedTupleDesc;
            }

            @Override
            public void close() {
                aggregatedIterator = null;
            }
        };
    }

}
