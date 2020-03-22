package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByField;
    private final int aggregateField;
    private final Op aggregateOperator;
    private final TupleDesc aggregatedTupleDesc;
    private final Map<Field, Tuple> aggregatedTuples = new HashMap<>();
    private final Map<Field, Integer> aggregatedCounts = new HashMap<>();

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
    @SuppressWarnings("DuplicatedCode")
    public void mergeTupleIntoGroup(final Tuple tup) {
        final Tuple mergedTuple = aggregatedTuples.getOrDefault(groupByField == NO_GROUPING ? null : tup.getField(groupByField),
                new Tuple(aggregatedTupleDesc));
        IntField aggregatedValue = (IntField) (groupByField == NO_GROUPING ? mergedTuple.getField(0) : mergedTuple.getField(1));
        final IntField newValue = (IntField) tup.getField(aggregateField);
        final int currCount = aggregatedCounts.getOrDefault(groupByField == NO_GROUPING ? null : tup.getField(groupByField), 0);
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
                        (aggregatedValue.getValue() * currCount + newValue.getValue()) / (currCount + 1));
                break;
            case SUM:
                aggregatedValue = aggregatedValue == null ? newValue : new IntField(aggregatedValue.getValue() + newValue.getValue());
                break;
            case COUNT:
                aggregatedValue = new IntField(currCount + 1);
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
        aggregatedCounts.put(groupByField == NO_GROUPING ? null : tup.getField(groupByField), currCount + 1);
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
        return new TupleIterator(aggregatedTupleDesc, aggregatedTuples.values());
    }

}
