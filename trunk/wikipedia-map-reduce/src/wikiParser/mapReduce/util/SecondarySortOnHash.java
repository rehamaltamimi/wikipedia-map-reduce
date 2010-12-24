/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.mapReduce.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Utilities that setup secondary sorting on hashes in keys.
 * Keys with hash marks are sent to the same reducer and same mapper, but
 * are sorted first in order.
 * 
 * @author shilad
 */
public class SecondarySortOnHash {

    /**
     * Partitions pairs to mappers, but ignores things follow hash marks.
     * @author Shilad
     */
    public static final class HashIgnoringPartitioner implements Partitioner {

        private JobConf jobConf;

        public int getPartition(Object key, Object value, int numPartitions) {
            if (!(key instanceof Text)) {
                throw new UnsupportedOperationException("key " + key + " is not text");
            }
            Text textKey = (Text) key;
            String stringKey = textKey.toString();
            int hashIndex = stringKey.indexOf("#");
            if (hashIndex >= 0) {
                stringKey = stringKey.substring(0, hashIndex);
            }
            return Math.abs(stringKey.hashCode()) % numPartitions;
        }

        public void configure(JobConf jc) {
            this.jobConf = jc;
        }
    }

    /**
     * Compares keys alphabetically, but the '#' character functionally serves
     * as the end of string marker if it appears.
     */
    public static final class HashEndingRawComparator extends HashRawComparator {
        public HashEndingRawComparator() {
            super(true);
        }
    }

    /**
     * Compares keys alphabetically, but the '#' character is ordered before
     * other characters.
     */
    public static final class HashFavoringRawComparator extends HashRawComparator{
        public HashFavoringRawComparator() {
            super(false);
        }
    }

    public static class HashRawComparator extends Text.Comparator {
        private boolean stopAtHash;
        
        public HashRawComparator(boolean stopAtHash) {
            this.stopAtHash = stopAtHash;
        }


        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int n1 = WritableUtils.decodeVIntSize(b1[s1]);
            int n2 = WritableUtils.decodeVIntSize(b2[s2]);
            s1 += n1;
            l1 -= n1;
            s2 += n2;
            l2 -= n2;

            int end1 = s1 + l1;
            int end2 = s2 + l2;

            for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
                int a = (b1[i] & 0xff);
                int b = (b2[j] & 0xff);
                if (a == '#' && b == '#') {
                    if (stopAtHash) {
                        return 0;
                    }
                    // skip both characters...
                } else if (a == '#') {
                    // a comes first
                    return -1;
                } else if (b == '#') {
                    // b comes first
                    return +1;
                } else if (a != b) {
                    return a - b;
                }
            }
            if (l1 > l2) {
                if ((b1[l2+s1] & 0xff) == '#') {
                    return -1;
                } else {
                    return +1;
                }
            } else if (l2 > l1) {
                if ((b2[l1+s2] & 0xff) == '#') {
                    return +1;
                } else {
                    return -1;
                }
            } else {
                return l1 - l2;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text ta = (Text)a;
            Text tb = (Text)b;
            if (ta == tb)
              return 0;
            else
              return compareBytes(ta.getBytes(), 0, ta.getLength(),
                                  tb.getBytes(), 0, tb.getLength());
        }

        @Override
        public int compare(Object a, Object b) {
            return compare((WritableComparable) a, (WritableComparable) b);
        }
    }

    public static void setupSecondarySortOnHash(JobConf conf) {
        conf.setPartitionerClass(HashIgnoringPartitioner.class);
        conf.setOutputValueGroupingComparator(HashEndingRawComparator.class);
        conf.setOutputKeyComparatorClass(HashFavoringRawComparator.class);
    }
}
