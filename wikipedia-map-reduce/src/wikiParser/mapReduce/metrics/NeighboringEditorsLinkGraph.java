package wikiParser.mapReduce.metrics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeSet;

public class NeighboringEditorsLinkGraph {

	private static final int TAKE_VALUE = 100;

	public static void main (String [] args) throws IOException {
		calculateSimpleProbs(new File("/data/colin/comm_links.txt"), new File("/data/colin/results/neighboring_editors.txt"));
	}

	private static void calculateSimpleProbs(File inFile, File outFile) throws FileNotFoundException, IOException {
		BufferedReader in  = new BufferedReader(new FileReader(inFile));
		BufferedWriter out = new BufferedWriter(new FileWriter(outFile));
		HashMap<String,TreeSet<String>> caMap = new HashMap<String,TreeSet<String>>();
		HashMap<String,TreeSet<String>> talkMap = new HashMap<String,TreeSet<String>>();
		TreeSet<String> userSet = new TreeSet<String>();
		long userCount = 0, coAuthorCount = 0, talkedCount = 0, cAndTCount = 0, n = 0; //increment every time a user is seen
		String line = in.readLine();
		while (line != null) {
			n += 1;
			if (n % 5000 == 0) {
				out.write("-->read "+n+" lines\n");
			}
			String key = line.split("\t")[0];
			String values = line.split("\t")[1];
			String[] splitValues = values.split(" ");
			if (key.startsWith("a")) { //key is article
				TreeSet<String> userList = new TreeSet<String>();
				for (String value : splitValues) {
					if (value.startsWith("u")) { //value is user
						userSet.add(value.substring(3));
						if (value.startsWith("u03") || value.startsWith("u06")) { //value is article-editing link
							if (value.substring(3).hashCode() % TAKE_VALUE == 0) { //take value
								userList.add(value.substring(3));
							}
						}
					}
				}
				for (String u : userList) {
					for (String v : userList) {
						if (!u.equals(v)) { //add to caMap
							if (caMap.get(u) == null) { //list null check
								caMap.put(u, new TreeSet<String>());
							}
							if (caMap.get(v) == null) { //list null check
								caMap.put(v, new TreeSet<String>());
							}
							caMap.get(u).add(v);
							caMap.get(v).add(u);
						}
					}
				}

			} else if (key.startsWith("u")) { //key is user
				try {
					if (key.substring(3).hashCode() % TAKE_VALUE == 0) { //take key
						for (String value : splitValues) {
							if (value.startsWith("u")) { //value is user
								userSet.add(value.substring(3));
								if (value.startsWith("u13")) { //value is talking link
									if (value.substring(3).hashCode() % TAKE_VALUE == 0) { //take value, add to talkMap
										if (talkMap.get(key.substring(3)) == null) { //list null check
											talkMap.put(key.substring(3), new TreeSet<String>());
										}
										if (talkMap.get(value.substring(3)) == null) { //list null check
											talkMap.put(value.substring(3), new TreeSet<String>());
										}
										talkMap.get(key.substring(3)).add(value.substring(3));
										talkMap.get(value.substring(3)).add(key.substring(3));
									}
								}
							}
						}
					} else { //reject key
						userSet.add(key.substring(3));
					}
				} catch (StringIndexOutOfBoundsException sioobe) {
					out.write("StringIndexOutOfBoundsException at line "+n+": "+key+"\n");
				}
			}
			line = in.readLine();
		}
		//count counts
		for (String u : caMap.keySet()) {
			for (String v : caMap.get(u)) {
				coAuthorCount += 1;
				if (talkMap.get(u) != null) {
					if (talkMap.get(u).contains(v)) {
						cAndTCount += 1;
					}
				}
			}
		}
		for (String u : talkMap.keySet()) {
			for (String v : talkMap.get(u)) {
				talkedCount += 1;
				if (caMap.get(u) != null) {
					if (caMap.get(u).contains(v)) {
						cAndTCount += 1;
					}
				}
			}
		}
		out.write("===Results===\n");
		out.write("userCount: "+userSet.size()+"\n");
		userCount = userSet.size()*(userSet.size()-1); //sample this down as well?  If so, have to hash every u## value
		//commutative .... I may need to divide by 2.
		//calucluate probs
		double pCA = ((double)coAuthorCount)/((double)userCount);
		double pT = ((double)talkedCount)/((double)userCount);
		double pCAAndT = ((double)cAndTCount)/((double)userCount);
		double pTGivenCA = pCAAndT/pCA;
		double pCAGivenT = pCAAndT/pT;
		out.write("coAuthorCount: "+coAuthorCount+"\n");
		out.write("talkedCount: "+talkedCount+"\n");
		out.write("pCA: "+pCA+"\n");
		out.write("pT: "+pT+"\n");
		out.write("pCAAndT: "+pCAAndT+"\n");
		out.write("P(T|CA) = "+pTGivenCA+"\n");
		out.write("P(CA|T) = "+pCAGivenT+"\n");
		out.flush();
		out.close();
		in.close();

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
		long userCount = 0, coAuthorCount = 0, talkedCount = 0, cAndTCount = 0, n = 0;
		HashMap<String,HashMap<String,Integer>> users = new HashMap<String,HashMap<String,Integer>>();
		String line = in.readLine();
		while (line != null) {
			try {
				String key = line.split("\t")[0];
				String values = line.split("\t")[1];
				String[] splitValues = values.split(" ");
				String keyLink = key.substring(3);
				if (key.startsWith("u")) {
					if (users.get(keyLink) == null) {
						users.put(keyLink, new HashMap<String,Integer>());
					}
				}
				boolean ca = false;
				String firstUser = "";
				for (String value : splitValues) {
					if (value.startsWith("u")) {
						String link = value.substring(3);
						if (users.get(keyLink).get(link) == null) {
							users.get(keyLink).put(link, 0);
						}
						if (value.startsWith("u03") || value.startsWith("u06")) {
							TreeSet<String> userList = new TreeSet<String>();
							if (userList.size() > 0) { //there has been an author already
								for (String author : userList) {
									if (users.get(author) == null) {
										users.put(author, new HashMap<String,Integer>());
									} //do i need to do this for link as well?
									if (users.get(author).get(link) == null) {
										users.get(author).put(link, 0);
									}
									if (users.get(author).get(link) == 0 || users.get(author).get(link) == 2) {
										users.get(author).put(link,users.get(author).get(link)+1);
									}
								}
								userList.add(link);
							} else { //first author
								userList.add(link);
								firstUser = link;
							}
						}
						else if (value.startsWith("u13")) {
							if (users.get(keyLink).get(link) == 0 || users.get(keyLink).get(link) == 1) {
								users.get(keyLink).put(link,users.get(keyLink).get(link)+2);
							}
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
			line = in.readLine();
		}
		int m = 0;
		for (String u : users.keySet()) {
			for (String v : users.get(u).keySet()) {
				if (m%100 == 0) {
					if (users.get(u).get(v) == 0) { userCount++; }
					if (users.get(u).get(v) == 1) { userCount++; coAuthorCount++; }
					if (users.get(u).get(v) == 2) { userCount++; talkedCount++; }
					if (users.get(u).get(v) == 3) { userCount++; coAuthorCount++; talkedCount++; cAndTCount++; }
				}
				m++;
			}
		}
		double pCA = ((double)coAuthorCount)/((double)userCount);
		double pT = ((double)talkedCount)/((double)userCount);
		double pCAAndT = ((double)cAndTCount)/((double)userCount);
		double pTGivenCA = pCAAndT/pCA;
		double pCAGivenT = pCAAndT/pT;
		out.write("===Results===\n");
		out.write("coAuthorCount: "+coAuthorCount+"\n");
		out.write("talkedCount: "+talkedCount+"\n");
		out.write("userCount: "+userCount+"\n");
		out.write("pCA: "+pCA+"\n");
		out.write("pT: "+pT+"\n");
		out.write("pCAAndT: "+pCAAndT+"\n");
		out.write("P(T|CA) = "+pTGivenCA+"\n");
		out.write("P(CA|T) = "+pCAGivenT+"\n");
		out.flush();
		out.close();
		in.close();
	}
}
