package wikiParser.mapReduce;

import java.io.IOException;

import wikiParser.mapReduce.statistics.RevisionCountData;
import wikiParser.mapReduce.statistics.RevisionCountHistogram;
import wikiParser.mapReduce.statistics.RevisionLengthHistogram;
import wikiParser.mapReduce.util.IdentityReduce;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.mapReduce.util.SimpleJobConf;

public class RevisionHistogramsMain {

	public static void main (String [] args) throws IOException {
//		mistMain(args);
		s3Main(args);
	}

	private static void mistMain (String [] args) throws IOException {
		//		RevisionCountData.runMe("/user/shilad/wikipedia.txt", "rcd", "rcdmr");
		//		RevisionLengthData.runMe("/user/shilad/wikipedia.txt", "rld", "rldmr");
		RevisionCountHistogram.runMe(new String[] {"./rcd/"}, "rch", "rchmr");
		RevisionLengthHistogram.runMe(new String[] {"./rld/"}, "rlh", "rlhmr");
	}

	private static void s3Main (String [] args) throws IOException {
                SimpleJobConf conf = new SimpleJobConf(
                        RevisionCountData.Map.class,
                        IdentityReduce.class,
                        MapReduceUtils.S3_TEST,
                        "s3n://macalester/rcdmr/",
                        "rcd");
                conf.runTest();
	}

}
