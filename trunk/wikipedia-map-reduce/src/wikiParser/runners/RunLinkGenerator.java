/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.runners;

import java.io.File;
import java.io.IOException;
import javax.xml.stream.XMLStreamException;
import wikiParser.Page;
import wikiParser.PageParser;
import wikiParser.RevisionFingerprinter;
import wikiParser.Edge;
import wikiParser.EdgeParser;
import wikiParser.Revision;

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
            for (Edge link : lparser.findLinks(fparser, article, rev)) {
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
