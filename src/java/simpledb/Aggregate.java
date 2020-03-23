package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final OpIterator opIterator;
    private final int groupByField;
    private final int aggregateField;
    private final Aggregator.Op aggregateOperator;
    private Aggregator aggregator;
    private OpIterator aggregatorIterator;
    private List<OpIterator> children = new ArrayList<>();

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(final OpIterator child, final int afield, final int gfield, final Aggregator.Op aop) {
        opIterator = child;
        aggregateField = afield;
        groupByField = gfield;
        aggregateOperator = aop;
        aggregator = null;
        children.add(child);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return groupByField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return groupByField == Aggregator.NO_GROUPING ? null : opIterator.getTupleDesc().getFieldName(groupByField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return opIterator.getTupleDesc().getFieldName(aggregateField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aggregateOperator;
    }

    public static String nameOfAggregatorOp(final Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        if (aggregator == null) {
            opIterator.open();
            final Type groupType = groupByField == Aggregator.NO_GROUPING ? null : opIterator.getTupleDesc().getFieldType(groupByField);
            aggregator = opIterator.getTupleDesc().getFieldType(aggregateField) == Type.INT_TYPE ? new IntegerAggregator(groupByField, groupType,
                    aggregateField,
                    aggregateOperator) : new StringAggregator(groupByField, groupType, aggregateField, aggregateOperator);
            while (opIterator.hasNext()) {
                aggregator.mergeTupleIntoGroup(opIterator.next());
            }
            opIterator.close();
            aggregatorIterator = aggregator.iterator();
        }
        aggregatorIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggregatorIterator.hasNext()) {
            return aggregatorIterator.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        final Type[] typeAr = groupByField == Aggregator.NO_GROUPING ? new Type[]{opIterator.getTupleDesc().getFieldType(
                aggregateField)} : new Type[]{opIterator.getTupleDesc().getFieldType(groupByField), opIterator.getTupleDesc().getFieldType(aggregateField)};
        final String[] fieldAr = groupByField == Aggregator.NO_GROUPING ? new String[]{opIterator.getTupleDesc().getFieldName(
                aggregateField)} : new String[]{opIterator.getTupleDesc().getFieldName(groupByField), opIterator.getTupleDesc().getFieldName(aggregateField)};
        return new TupleDesc(typeAr, fieldAr);
    }

    public void close() {
        super.close();
        aggregatorIterator.close();
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
