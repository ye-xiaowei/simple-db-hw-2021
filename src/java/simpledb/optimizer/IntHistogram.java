package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private final int buckets;
    private final int min;
    private final int max;
    private final double width;
    private final int[] bucketsArr;
    private final AtomicLong count = new AtomicLong();

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.min = min;
        this.max = max;
        double w = (double) (max - min + 1) / buckets;
        while (w < 1) {
            buckets--;
            w = (double) (max - min + 1) / buckets;
        }
        this.width = w;
        this.buckets = buckets;
        this.bucketsArr = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v > max || v < min) {
            throw new IllegalArgumentException("value should [" + min + ", " + max + "]");
        }
        int i = (int) ((v - min) / width);
        if (v == max) {
            // 恰好为最大值
            i--;
        }
        bucketsArr[i]++;
        count.incrementAndGet();
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int baseV = v - min;
        int bucketIndex;
        int height;

        if (v == max) {
            bucketIndex = buckets - 1;
        } else {
            bucketIndex = (int) (baseV / width);
        }
        if (bucketIndex < bucketsArr.length && bucketIndex >= 0) {
            height = bucketsArr[bucketIndex];
        } else {
            height = -1;
        }

        switch (op) {
            case EQUALS: {
                if (v > max || v < min || count.get() == 0) {
                    return 0;
                }
                return height / width / count.get();
            }
            case NOT_EQUALS: {
                if (v > max || v < min || count.get() == 0) {
                    return 1.0;
                }
                return 1 - height / width / count.get();
            }
            case LESS_THAN_OR_EQ: {
                if (v < min || count.get() == 0) {
                    return 0;
                }
                if (v >= max) {
                    return 1.0;
                }
                double ans = (baseV - bucketIndex * width) / width * height;
                for (int i = 0; i < bucketIndex; i++) {
                    ans += bucketsArr[i];
                }
                return ans / count.get() + height / width / count.get();
            }
            case LESS_THAN: {
                if (v <= min || count.get() == 0) {
                    return 0;
                }
                if (v > max) {
                    return 1.0;
                }
                double ans = (baseV - bucketIndex * width) / width * height;
                for (int i = 0; i < bucketIndex; i++) {
                    ans += bucketsArr[i];
                }
                return ans / count.get();
            }
            case GREATER_THAN_OR_EQ: {
                if (v > max || count.get() == 0) {
                    return 0;
                }
                if (v <= min) {
                    return 1.0;
                }
                double ans = ((bucketIndex + 1) * width - baseV) / width * height;
                for (int i = bucketIndex + 1; i < bucketsArr.length; i++) {
                    ans += bucketsArr[i];
                }
                return ans / count.get() + height / width / count.get();
            }
            case GREATER_THAN: {
                if (v >= max || count.get() == 0) {
                    return 0;
                }
                if (v < min) {
                    return 1.0;
                }
                double ans = ((bucketIndex + 1) * width - baseV) / width * height;
                for (int i = bucketIndex + 1; i < bucketsArr.length; i++) {
                    ans += bucketsArr[i];
                }
                return ans / count.get();
            }
        }
        return -1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return Arrays.stream(bucketsArr).average().getAsDouble() / width / count.get();
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
