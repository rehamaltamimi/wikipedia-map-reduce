package wikiParser.mapReduce;

import java.io.IOException;

import wikiParser.mapReduce.graphs.CommutativeLinkMapReduce;
import wikiParser.mapReduce.util.UniqueConcatenateReduce;
import wikiParser.mapReduce.util.SimpleJobConf;

public class LinkMapReduceMain {

	public static void main (String [] args) throws IOException {
/*            SimpleJobConf conf = new SimpleJobConf(
                    InitialLinkMapReduce.Map.class,
                    UniqueConcatenateReduce.class,
                    MapReduceUtils.S3_INPUTS,
                    MapReduceUtils.S3_OUTPUT + "/links",
                    "initiallinkmapreduce");*/
            SimpleJobConf conf = new SimpleJobConf(
                    CommutativeLinkMapReduce.Map.class,
                    UniqueConcatenateReduce.class,
                    new String[] { "/user/cwelch/sub_links_alt.txt" },
                    "/user/cwelch/comm_link_out",
                    "commutativelinkmapreduce");
            conf.getConf().setNumMapTasks(300);
            conf.getConf().setNumReduceTasks(40);
            conf.run();
	}
}
