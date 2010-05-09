/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.runners;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import wikiParser.PageParser;

public class RunCommutativeLinkMapReduce extends WikiLineReader {

	/**
	 * @uml.property  name="sIZE1"
	 */
	public int SIZE1 = 100000;
	/**
	 * @uml.property  name="sIZE2"
	 */
	public int SIZE2 = 10000000;
	private BufferedReader in;
	private BufferedWriter out;


	public RunCommutativeLinkMapReduce(File inFile, File outFile) throws IOException {
		super(inFile);
		this.in = new BufferedReader(new FileReader(inFile));
		this.out = new BufferedWriter(new FileWriter(outFile));
	}

	@Override
	public void processArticle(PageParser parser) throws XMLStreamException {
		//not needed for testing CommutativeLinkMapReduce
	}
	
	@Override
	public void cleanup() throws IOException {
		this.out.close();
	}

	public void parseLinks() throws IOException {
		String line = this.in.readLine();
		int lineNum = 1;
		while (line != null) {
			if (line.split("\t").length == 2) {		//this is a key:value pair
			String key = line.split("\t")[0];
			String value = line.split("\t")[1];
			String valueType = null, linkType = null, valueId = null, keyType = null, keyId = null;
			for (String link : value.toString().split(" ")) {
				valueType = link.substring(0, 1);
				linkType = link.substring(1, 3);
				valueId = link.substring(3);
				keyType = key.toString().substring(0,1);
				keyId = key.toString().substring(1);
				this.out.write(key +"\t"+ link +"\n");
				this.out.write((valueType+valueId) +"\t"+ (keyType+linkType+keyId) +"\n");
			}
			this.out.flush();
			} else {	//this is not a key:value pair
				System.out.println(line.split("\t").length);
				System.out.println("error at line"+lineNum+": "+line);
			}
			line = this.in.readLine();
			lineNum++;
		}
		cleanup();
	}
	
	public static void run() throws IOException {
		RunCommutativeLinkMapReduce rILMR = new RunCommutativeLinkMapReduce(new File("runners_data/name_id_sub_out.txt"),
				new File("runners_data/comm_link_out.txt"));
		rILMR.parseLinks();
	}

	public static void main(String args[]) throws IOException {
		run();
	}
}


