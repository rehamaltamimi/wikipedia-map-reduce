/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.runners;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import javax.xml.stream.XMLStreamException;
import wikiParser.PageParser;
import wikiParser.RevisionFingerprinter;
import wikiParser.Revision;
import wikiParser.User;

/**
 *
 * @author shilad
 */
public class RunFingerprinter extends WikiLineReader {
    /**
	 * @uml.property  name="sIZE1"
	 */
    public int SIZE1 = 100000;
    /**
	 * @uml.property  name="sIZE2"
	 */
    public int SIZE2 = 10000000;

    
    public RunFingerprinter(File file) throws IOException {
        super(file);
    }

    @Override
    public void processArticle(PageParser parser) throws XMLStreamException {
        int totalLength = 0;
        int newLength1 = 0;
        int newLength2 = 0;
        int numTotalNeighbors = 0;
        int numSameNeighbors = 0;
        int numRevisions = 0;

        RevisionFingerprinter fp1 = new RevisionFingerprinter(SIZE1);
        RevisionFingerprinter fp2 = new RevisionFingerprinter(SIZE2);

        while (true) {
            Revision rev = parser.getNextRevision();
            if (rev == null) {
                break;
            }
//            System.out.println("data for " + rev.getId() + " is " + rev.getText() + " user is " + rev.getContributor().getId());
//            System.out.println("==================");
            numRevisions++;
            if (fp1.getCacheSize() != fp2.getCacheSize()) {
                totalLength += rev.getText().length();
                ArrayList<String> t1 = fp1.findUniqueText(rev);
                ArrayList<String> t2 = fp2.findUniqueText(rev);
                for (String s : t1) {
                    newLength1 += s.length();
                }
                for (String s : t2) {
                    newLength2 += s.length();
                }
                Set<User> u1 = new HashSet<User>(fp1.neighboringContributors(rev, t1));
                Set<User> u2= new HashSet<User>(fp1.neighboringContributors(rev, t2));
                numTotalNeighbors += Math.max(u1.size() ,u2.size());
                u1.retainAll(u2);
                numSameNeighbors += u1.size();
                System.out.println("revision " + rev.getId() + " fraction new " + (1.0 * newLength1 / totalLength)
                        + " vs " + (1.0 * newLength2 / totalLength)
                        + " size of parser is " + fp1.getCacheSize()
                        + " vs " + fp2.getCacheSize()
                        + " jacard is " + (1.0 * numSameNeighbors / numTotalNeighbors)
                        + " (" + numTotalNeighbors + ", " + u1.size() + ")");
            }
            fp1.update(rev);
            fp2.update(rev);

        }
    }

    public static void main(String args[]) throws IOException {
        RunFingerprinter fp = new RunFingerprinter(new File("big.txt"));
        fp.readLines();
    }
}
