package simpledb;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.File;


public class MyTest {

    public static void main(String[] argv) {
        testJoin();
    }

    /**
     * java -jar dist/simpledb.jar convert ./data/data.txt 3
     * <pre>
     *  1,1,1
     *  2,2,2
     *  3,4,4
     * </pre>
     */
    public static void testSeqScan() {
        // construct a 3-column table schema
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] names = new String[]{"field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with data.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("./data/data.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 下面类似是这样的查询
     * <pre>
     * SELECT *
     * FROM data1, data2
     * WHERE data1.field1 = data2.field1
     *  AND data1.id > 1
     * </pre>
     * 我们使用 jar 包搞 2 个dat文件，比如:  java -jar ./dist/simpledb.jar convert ./data/data1.txt 3
     */
    public static void testJoin() {
        // construct a 3-column table schema
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] names = new String[]{"field0", "field1", "field2"};

        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("./data/data1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("./data/data2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                new Predicate(0,
                        Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
