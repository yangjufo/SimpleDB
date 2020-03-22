package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByField;
    private final TupleDesc aggregatedTupleDesc;
    private final Map<Field, Integer> aggregatedCounts = new HashMap<>();
    private final Map<Field, Tuple> aggregatedTuples = new HashMap<>();

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(final int gbfield, final Type gbfieldtype, final int afield, final Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Unsupported operator");
        }
        groupByField = gbfield;
        if (groupByField == NO_GROUPING) {
            aggregatedTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            aggregatedTupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @SuppressWarnings("DuplicatedCode")
    public void mergeTupleIntoGroup(final Tuple tup) {
        final Tuple mergedTuple = aggregatedTuples.getOrDefault(groupByField == NO_GROUPING ? null : tup.getField(groupByField),
                new Tuple(aggregatedTupleDesc));
        final int currCount = aggregatedCounts.getOrDefault(groupByField == NO_GROUPING ? null : tup.getField(groupByField), 0);
        mergedTuple.setField(groupByField == NO_GROUPING ? 0 : 1, new IntField(currCount + 1));
        if (groupByField != NO_GROUPING) {
            mergedTuple.setField(0, tup.getField(groupByField));
        }
        aggregatedTuples.put(groupByField == NO_GROUPING ? null : tup.getField(groupByField), mergedTuple);
        aggregatedCounts.put(groupByField == NO_GROUPING ? null : tup.getField(groupByField), currCount + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new TupleIterator(aggregatedTupleDesc, aggregatedTuples.values());
    }

}
