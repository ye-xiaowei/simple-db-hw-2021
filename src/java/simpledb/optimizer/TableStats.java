package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
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

    private final int ioCostPerPage;
    private final int pageNum;
    private int tupleNum = 0;
    private final FiledInfo[] filedInfos;

    private static class FiledInfo {
        TupleDesc.TDItem filed;
        IntHistogram histogram;
        int max;
        int min;
    }

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        this.filedInfos = new FiledInfo[tupleDesc.numFields()];
        Iterator<TupleDesc.TDItem> itemIterator = tupleDesc.iterator();
        int i = 0;
        while (itemIterator.hasNext()) {
            TupleDesc.TDItem next = itemIterator.next();
            filedInfos[i] = new FiledInfo();
            filedInfos[i].filed = next;
            i++;
        }
        HeapFile dbFile = (HeapFile)Database.getCatalog().getDatabaseFile(tableid);
        this.pageNum = dbFile.numPages();
        try(DbFileIterator fileIterator = dbFile.iterator(new TransactionId())) {
            fileIterator.open();
            while (fileIterator.hasNext()) {
                tupleNum++;
                Tuple next = fileIterator.next();
                Iterator<Field> fields = next.fields();
                int j = 0;
                while (fields.hasNext()) {
                    Field field = fields.next();
                    if (!(field instanceof StringField)) {
                        filedInfos[j].max = Math.max(filedInfos[j].max, (Integer) field.getV());
                        filedInfos[j].min = Math.min(filedInfos[j].min, (Integer) field.getV());;
                    }
                    j++;
                }
            }
            fileIterator.rewind();
            for (FiledInfo info : filedInfos) {
                info.histogram = new IntHistogram(NUM_HIST_BINS, info.min, info.max);
            }
            while (fileIterator.hasNext()) {
                Tuple next = fileIterator.next();
                int j = 0;
                Iterator<Field> fields = next.fields();
                while (fields.hasNext()) {
                    Field field = fields.next();
                    if (!(field instanceof StringField)) {
                        Integer v = (Integer) field.getV();
                        filedInfos[j].histogram.addValue(v);
                    }
                    j++;
                }
            }
        } catch (Exception e) {
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
        // some code goes here
        return ioCostPerPage * pageNum;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        // 估算某个tuple数量
        return (int) (tupleNum * selectivityFactor);
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
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return filedInfos[field].histogram.avgSelectivity();
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
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        FiledInfo info = filedInfos[field];
        switch (info.filed.fieldType) {
            case INT_TYPE: {
                return info.histogram.estimateSelectivity(op, (Integer) constant.getV());
            }
            case STRING_TYPE: {

            }
        }
        return -1.0;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return tupleNum;
    }
}
