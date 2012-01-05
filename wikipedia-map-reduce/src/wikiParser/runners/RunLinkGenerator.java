/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.runners;

import java.io.File;
import java.io.IOException;
import javax.xml.stream.XMLStreamException;
import wmr.core.Page;
import wmr.core.PageParser;
import wmr.core.RevisionFingerprinter;
import wmr.core.Edge;
import wmr.core.EdgeParser;
import wmr.core.Revision;

/**
 *
 * @author shilad
 */
public class RunLinkGenerator extends WikiLineReader {

    
    public RunLinkGenerator(File file) throws IOException {
        super(file);
    }

    @Override
    public void processArticle(PageParser parser) throws XMLStreamException {
        RevisionFingerprinter fparser = new RevisionFingerprinter();
        EdgeParser lparser = new EdgeParser();
        Page article = parser.getArticle();

        while (true) {
            Revision rev = parser.getNextRevision();
            if (rev == null) {
                break;
            }
            for (Edge link : lparser.findEdges(fparser, article, rev)) {
//                System.out.println(link.toOutputString());
            }
            fparser.update(rev);
        }
    }

    public static void main(String args[]) throws IOException {
        RunLinkGenerator fp = new RunLinkGenerator(new File("runners_data/big.txt"));
        fp.readLines();
    }
}
