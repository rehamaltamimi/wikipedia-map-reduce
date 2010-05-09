package wikiParser.mapReduce.metrics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class NeighboringEditorsFinalStep {

	public static void main (String [] args) throws IOException {
		calculateProbabilities(new File(""), new File(""));
	}

	private static void calculateProbabilities(File inFile, File outFile) throws FileNotFoundException,
			IOException {
		/*Final movement (non-M/R):
		 * 
		 * Step 1: read in file
		 * Step 2: Count things, do some probabilities.
		 * P(ca&talk): #ca&talk/#ca
		 * P(ca&!talk): #ca&!talk/#ca
		 * What effect does co-authoring have on talking?
		 *
		 * P(talk|ca) = P(talk^ca)/P(ca), or
		 * P(ca|talk) = P(talk^ca)/P(talk)
		 * P(ca) = #ofPeopleWhoHaveCoAuthored/#ofPeople
		 * P(talk) = #ofPeopleWhoHaveTalked/#ofPeople
		 * P(talk^ca) = #ofPeopleWhoHaveBothTalkedAndCoAuthored/#ofPeople
		 */
		BufferedReader in  = new BufferedReader(new FileReader(inFile));
		BufferedWriter out = new BufferedWriter(new FileWriter(outFile));
		int userCount = 0, coAuthorCount = 0, talkedCount = 0, cAndTCount = 0, n = 0;
		String line = new String("");
		while (line != null) {
			line = in.readLine();
			try {
				String key = line.split("\t")[0];
				if (key.equals("count")) {
					userCount = Integer.parseInt(line.split("\t")[1]);
				}
				else {
					String values = line.split("\t")[1];
					String[] splitValues = values.split(" ");
					for (String value : splitValues) {
						boolean cA = false;
						boolean t = false;
						//TODO: ask danny about this again.... coauthor with a particular
						//		user given that you've talked to that same user
						if (value.startsWith("c")) {
							coAuthorCount++;
							cA = true;
						}
						else if (value.startsWith("t")) {
							talkedCount++;
							t = true;
						}
						if (cA && t) {
							cAndTCount++;
						}
					}
				}
			}
			catch (Exception ex) {
				out.write("error at line "+n+"\n");
			}
			n++;
			if (n%5000 == 0) {
				out.write("-->read "+n+" lines\n");
			}
		}
		float pCA = ((float)coAuthorCount)/((float)userCount);
		float pT = ((float)talkedCount)/((float)userCount);
		float pCAAndT = ((float)cAndTCount)/((float)userCount);
		float pTGivenCA = pCAAndT/pCA;
		float pCAGivenT = pCAAndT/pT;
		out.write("===Results===\n");
		out.write("P(T|CA) = "+pTGivenCA+"\n");
		out.write("P(CA|T) = "+pCAGivenT+"\n");
		System.out.println("P(T|CA) = "+pTGivenCA);
		System.out.println("P(CA|T) = "+pCAGivenT);
		out.flush();
		out.close();
		in.close();
	}
}
