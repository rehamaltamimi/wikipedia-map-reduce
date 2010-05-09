package wikiParser.mapReduce.statistics;

import java.io.IOException;


import wikiParser.mapReduce.util.*;

public class RevisionCountHistogram {

	public static void runMe(String [] inputs, String outputDir, String jobName) throws IOException{
            SimpleJobConf conf = new SimpleJobConf(IdentityMap.class, HistogramReduce.class, inputs, outputDir, jobName);
            conf.run();
	}

}
