package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private Map<String, List<Integer>> strMap = new HashMap<>();
    private Map<Integer, List<Integer>> intMap = new HashMap<>();
    private List<Integer> nonGb;

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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        if (gbfield == NO_GROUPING) {
            nonGb = new ArrayList<>();
        } else if (gbfieldtype == Type.STRING_TYPE) {
            strMap = new HashMap<>();
        } else {
            intMap = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField field = (IntField) tup.getField(afield);
        if (gbfield == -1){
            nonGb.add(field.getValue());
            return;
        }
        switch (gbfieldtype) {
            case STRING_TYPE:{
                StringField key = (StringField)tup.getField(gbfield);
                strMap.compute(key.getValue(), (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(field.getValue());
                    return v;
                });
                break;
            }
            case INT_TYPE:{
                IntField key = (IntField)tup.getField(gbfield);
                intMap.compute(key.getValue(), (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(field.getValue());
                    return v;
                });
                break;
            }
        }
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
        // some code goes here
        return new GpOpIterator();
    }

    private class GpOpIterator extends Operator {
        private TupleDesc tupleDesc;
        private Iterator<Map.Entry<Integer, List<Integer>>> intIt;
        private Iterator<Map.Entry<String, List<Integer>>> strIt;
        private boolean flag;

        public GpOpIterator() {
            init();
        }

        private void init() {
            if (gbfield == -1) {
                tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
                flag = false;
                return;
            }

            switch (gbfieldtype) {
                case INT_TYPE: {
                    tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
                    intIt = intMap.entrySet().iterator();
                    break;
                }
                case STRING_TYPE: {
                    tupleDesc = new TupleDesc(new Type[]{Type.STRING_TYPE, Type.INT_TYPE});
                    strIt = strMap.entrySet().iterator();
                    break;
                }
            }
        }

        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            if (!super.open) {
                return null;
            }
            if (gbfield == -1) {
                if (!flag) {
                    flag = true;
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, new IntField(group(nonGb)));
                    return tuple;
                } else {
                    return null;
                }
            }
            switch (gbfieldtype) {
                case INT_TYPE:{
                    if (!intIt.hasNext()) {
                        return null;
                    }
                    Map.Entry<Integer, List<Integer>> next = intIt.next();
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, new IntField(next.getKey()));
                    tuple.setField(1, new IntField(group(next.getValue())));
                    return tuple;
                }
                case STRING_TYPE: {
                    if (!strIt.hasNext()) {
                        return null;
                    }
                    Map.Entry<String, List<Integer>> next = strIt.next();
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, new StringField(next.getKey(), Type.STRING_LEN));
                    tuple.setField(1, new IntField(group(next.getValue())));
                    return tuple;
                }
            }
            return null;
        }

        private Integer group(List<Integer> list) {
            switch (what) {
                case MIN:
                    return list.stream().min(Comparator.naturalOrder()).get();
                case MAX:
                    return list.stream().max(Comparator.naturalOrder()).get();
                case COUNT:
                    return list.size();
                case AVG: {
                    int sum = 0;
                    for (Integer num : list) {
                        sum += num;
                    }
                    return sum / list.size();
                }
                 case SUM:
                    return list.stream().mapToInt(Integer::intValue).sum();
                default:
                    // TODO 其他操作符
                    throw new UnsupportedOperationException("unknown op：" + what);
            }
        }

        @Override
        public OpIterator[] getChildren() {
            return new OpIterator[0];
        }

        @Override
        public void setChildren(OpIterator[] children) {
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            init();
        }
    }
}
