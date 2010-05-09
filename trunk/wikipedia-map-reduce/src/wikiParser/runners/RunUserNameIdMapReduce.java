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

import org.apache.hadoop.io.Text;

import wikiParser.Page;
import wikiParser.PageParser;
import wikiParser.RevisionFingerprinter;
import wikiParser.Edge;
import wikiParser.Revision;

public class RunUserNameIdMapReduce extends WikiLineReader {

	/**
	 * @uml.property  name="sIZE1"
	 */
	public int SIZE1 = 100000;
	/**
	 * @uml.property  name="sIZE2"
	 */
	public int SIZE2 = 10000000;
	private BufferedWriter out;


	public RunUserNameIdMapReduce(File inFile, File outFile) throws IOException {
		super(inFile);
		this.out = new BufferedWriter(new FileWriter(outFile));
	}

	@Override
	public void processArticle(PageParser parser) throws XMLStreamException, IOException {
		Page article = parser.getArticle();
		while (true) {
			Revision rev = parser.getNextRevision();
			if (rev == null) {
				break;
			}
			this.out.write(rev.getContributor().toUnderscoredString() +"\t"+ "u"+rev.getContributor().getId() +"\n");
		}
		this.out.flush();
	}

	@Override
	public void cleanup() throws IOException {
		this.out.close();
	}

	public static void run() throws IOException {
		RunUserNameIdMapReduce rILMR = new RunUserNameIdMapReduce(new File("runners_data/big.txt"),
				new File("runners_data/user_name_id_mr_out.txt"));
		rILMR.readLines();
	}

	public static void main(String args[]) throws IOException {
		run();
	}
}


