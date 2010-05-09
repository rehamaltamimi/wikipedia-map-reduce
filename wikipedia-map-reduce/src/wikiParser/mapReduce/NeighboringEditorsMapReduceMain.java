package wikiParser.mapReduce;

import java.io.IOException;

import wikiParser.mapReduce.metrics.NeighboringEditorsMapReduce;

public class NeighboringEditorsMapReduceMain {
	public static void main (String [] args) throws IOException {
		NeighboringEditorsMapReduce.runMe("/user/shilad/wikipedia.txt", "/user/cwelch/nemr", "nemr");
	}
}
