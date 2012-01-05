/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.runners;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import wmr.core.PageParser;
import wikiParser.mapReduce.graphs.NameIdSubstitution;

public class RunNameIdSubstitution extends WikiLineReader {

	/**
	 * @uml.property  name="sIZE1"
	 */
	public int SIZE1 = 100000;
	/**
	 * @uml.property  name="sIZE2"
	 */
	public int SIZE2 = 10000000;
	private File articleIdFile, userIdFile, linkInFile, linkOutFile;


	public RunNameIdSubstitution(File articleIdFile, File userIdFile, File linkInFile, File linkOutFile) throws IOException {
		super(linkInFile);
		this.articleIdFile = articleIdFile;
		this.userIdFile = userIdFile;
		this.linkInFile = linkInFile;
		this.linkOutFile = linkOutFile;
	}

	@Override
	public void processArticle(PageParser parser) throws XMLStreamException {
		//not needed for testing NameIdSubstitution
	}

	public void substitute() throws FileNotFoundException, IOException {
		NameIdSubstitution.substitute(this.articleIdFile, this.userIdFile, this.linkInFile, this.linkOutFile);
	}

	public static void run() throws IOException, FileNotFoundException {
		RunNameIdSubstitution rILMR = new RunNameIdSubstitution(new File("runners_data/art_name_id_mr_out.txt"),
				new File("runners_data/user_name_id_mr_out.txt"), new File("runners_data/init_link_mr_out.txt"),
				new File("runners_data/name_id_sub_out.txt"));
		rILMR.substitute();
	}

	public static void main(String args[]) throws IOException {
		run();
	}
}


