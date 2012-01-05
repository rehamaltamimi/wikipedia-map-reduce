package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wmr.core.Page;
import wmr.core.RevisionFingerprinter;
import wmr.core.Edge;
import wmr.core.Revision;
import wmr.core.User;

public class ConversationGenerator implements FingerprintingEdgeGenerator {
    /**
	 * @uml.property  name="parser"
	 * @uml.associationEnd  
	 */
    private RevisionFingerprinter parser;

    public ConversationGenerator() {
    }

    public void setFingerprintingParser(RevisionFingerprinter parser) {
        this.parser = parser;
    }
    
    public List<Edge> generate(Page article, Revision revision) {
        if (!article.isTalk() && !article.isUserTalk()) {
            return null;
        }
        List<Edge> edges = new ArrayList<Edge>();
        for (User neighbor : parser.neighboringContributors(revision)) {
            edges.add(new Edge(neighbor, revision.getContributor(), Edge.USER_TALKSWITH_USER));
        }
        return edges;
    }
}
