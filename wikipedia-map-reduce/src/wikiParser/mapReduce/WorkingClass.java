package wikiParser.mapReduce;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import wmr.core.PageParser;
import wmr.core.Edge;
import wmr.core.EdgeParser;

/**
 * Suggestions for refactoring:
 * 
 * Create a Revision class with user, article, timestamp, version, log message, etc.
 * Create a ArticleReader class that takes a file and returns a list of revisions.
 * Move all parsing to ArticleReader, and add instance variables if necessary.
 * 
 * @author shilad
 *
 */
public class WorkingClass {

	static final String outputEncoding = "UTF-8";
	public static final String filename = "userioerror.xml";

	public static void main(String[] args) throws Exception {
		System.out.println("parsing " + filename);
		InputStream stream = new FileInputStream(new File(filename));
		PageParser parser = new PageParser(stream);
		EdgeParser lp = new EdgeParser();
		for (Edge link : lp.findEdges(parser)) {
			System.out.println("link is " + link);
		}
	}
}