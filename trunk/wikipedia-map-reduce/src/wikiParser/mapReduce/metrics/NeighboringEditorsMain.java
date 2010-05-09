package wikiParser.mapReduce.metrics;

import java.io.IOException;

import wikiParser.mapReduce.metrics.NeighboringEditorsMapReduce;
import wikiParser.mapReduce.util.UniqueConcatenateReduce;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.mapReduce.util.SimpleJobConf;

public class NeighboringEditorsMain {

	public static void main (String [] args) throws IOException {
            SimpleJobConf conf = new SimpleJobConf(
                    NeighboringEditorsMapReduce.Map.class,
                    NeighboringEditorsMapReduce.Reduce.class,
                    MapReduceUtils.MIST_INPUTS,
                    "/user/cwelch/neigh_edit_mr_out",
                    "neighboringeditorsmapreduce");
            conf.getConf().setNumMapTasks(300);
            conf.getConf().setNumReduceTasks(40);
            conf.run();
	}
}
