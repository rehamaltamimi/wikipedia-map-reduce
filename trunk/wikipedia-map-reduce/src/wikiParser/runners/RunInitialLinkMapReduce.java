/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.runners;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import wmr.core.Page;
import wmr.core.PageParser;
import wmr.core.RevisionFingerprinter;
import wmr.core.Edge;
import wmr.core.EdgeParser;
import wmr.core.Revision;

public class RunInitialLinkMapReduce extends WikiLineReader {

	/**
	 * @uml.property  name="sIZE1"
	 */
	public int SIZE1 = 100000;
	/**
	 * @uml.property  name="sIZE2"
	 */
	public int SIZE2 = 10000000;
	private BufferedWriter out;


	public RunInitialLinkMapReduce(File inFile, File outFile) throws IOException {
		super(inFile);
		this.out = new BufferedWriter(new FileWriter(outFile));
	}

	@Override
	public void processArticle(PageParser parser) throws XMLStreamException, IOException {
		EdgeParser lp = new EdgeParser();
		Page article = parser.getArticle();
		RevisionFingerprinter fparser = new RevisionFingerprinter();
		if (article.isUserTalk() || article.isUser()) { 
			this.out.write("u" + article.getUser().getId() + "\t");
		} else {
			this.out.write("a" + article.getId() + "\t");
		}
		while (true) {
			Revision rev = parser.getNextRevision();
			if (rev == null) {
				break;
			}
			for (Edge link : lp.findEdges(fparser, article, rev)) {
				this.out.write(link.toOutputString() + " ");
			}
			fparser.update(rev);
		}
		this.out.write("\n");
		this.out.flush();
	}

	@Override
	public void cleanup() throws IOException {
		this.out.close();
	}

	public static void run() throws IOException {
		RunInitialLinkMapReduce rILMR = new RunInitialLinkMapReduce(new File("runners_data/big.txt"),
				new File("runners_data/init_link_mr_out.txt"));
		rILMR.readLines();
	}

	public static void main(String args[]) throws IOException {
		run();
	}
}


