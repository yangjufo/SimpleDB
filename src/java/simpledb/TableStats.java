package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(final String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(final String tablename, final TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(final HashMap<String, TableStats> s) {
        try {
            final java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (final NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        final Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            final int tableid = tableIt.next();
            final TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final int tableid;
    private final int ioCostPerPage;
    private final TupleDesc tupleDesc;
    private int numTuples;
    private final Map<Integer, IntHistogram> intHistograms = new HashMap<>();
    private final Map<Integer, StringHistogram> stringHistograms = new HashMap<>();

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(final int tableid, final int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;

        final DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        tupleDesc = file.getTupleDesc();
        final int numFields = tupleDesc.numFields();
        final Field[] mins = new Field[numFields];
        final Field[] maxs = new Field[numFields];

        final DbFileIterator fileIterator = file.iterator(null);
        try {
            fileIterator.open();
            while (fileIterator.hasNext()) {
                final Tuple tuple = fileIterator.next();
                for (int i = 0; i < numFields; i++) {
                    if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        if (mins[i] == null || tuple.getField(i).compare(Predicate.Op.LESS_THAN, mins[i])) {
                            mins[i] = tuple.getField(i);
                        }
                        if (maxs[i] == null || tuple.getField(i).compare(Predicate.Op.GREATER_THAN, maxs[i])) {
                            maxs[i] = tuple.getField(i);
                        }
                    }
                }
                numTuples += 1;
            }
            for (int i = 0; i < numFields; i++) {
                if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                    intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, ((IntField) mins[i]).getValue(), ((IntField) maxs[i]).getValue()));
                } else {
                    stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
                }

            }
            fileIterator.rewind();
            while (fileIterator.hasNext()) {
                final Tuple tuple = fileIterator.next();
                for (int i = 0; i < numFields; i++) {
                    if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        intHistograms.get(i).addValue(((IntField) tuple.getField(i)).getValue());
                    } else {
                        stringHistograms.get(i).addValue(((StringField) tuple.getField(i)).getValue());
                    }
                }
            }
            fileIterator.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return ((HeapFile) Database.getCatalog().getDatabaseFile(tableid)).numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(final double selectivityFactor) {
        return (int) (numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(final int field, final Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(final int field, final Predicate.Op op, final Field constant) {
        if (tupleDesc.getFieldType(field).equals(Type.INT_TYPE)) {
            return intHistograms.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            return stringHistograms.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return numTuples;
    }

}
