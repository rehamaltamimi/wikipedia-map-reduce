package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.RevisionFingerprinter;
import wikiParser.Edge;
import wikiParser.Revision;

public class ContributorMentionGenerator implements FingerprintingLinkGenerator {
    /**
	 * @uml.property  name="parser"
	 * @uml.associationEnd  
	 */
    private RevisionFingerprinter parser;
    
    public ContributorMentionGenerator() {
    }

    public void setFingerprintingParser(RevisionFingerprinter parser) {
        this.parser = parser;
    }

    public List<Edge> generate(Page article, Revision revision) {
        List<Edge> links = null;
        if (article.isUserTalk()) {
            links = new ArrayList<Edge>();
            for (String text : parser.findUniqueText(revision)) {
                for (String ref : Revision.getAnchorLinks(text)) {
                    links.add(
                            new Edge(revision.getContributor(),
                                     new Page(ref), Edge.USER_MENTION_ART));
                }
            }
        }
        return links;
    }
}
