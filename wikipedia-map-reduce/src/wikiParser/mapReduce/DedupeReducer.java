/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wikiParser.mapReduce;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Outputs the "first" value for each key (first is arbitrary).
 * @author shilad
 */
public class DedupeReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    @Override
    public void reduce(KEYIN key, Iterable<VALUEIN> values, Context context)
            throws IOException, InterruptedException {
        for (VALUEIN value : values) {
            context.write((KEYOUT)key, (VALUEOUT)value);
            break;  // only print the first
        }
    }
}
