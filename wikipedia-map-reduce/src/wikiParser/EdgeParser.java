package wikiParser;

import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import wikiParser.edges.ArticleArticleGenerator;
import wikiParser.edges.ContributorEditGenerator;
import wikiParser.edges.ContributorMentionGenerator;
import wikiParser.edges.ConversationGenerator;
import wikiParser.edges.FingerprintingLinkGenerator;
import wikiParser.edges.LinkGenerator;
import wikiParser.edges.UserArticleLinkGenerator;
import wikiParser.edges.UserUserRefGenerator;
import wikiParser.edges.UserUserTalkGenerator;

public class EdgeParser {

	List<LinkGenerator> generators = new ArrayList<LinkGenerator>();

	public EdgeParser() {
		generators.add(new ArticleArticleGenerator());
		generators.add(new ContributorEditGenerator());
		generators.add(new ContributorMentionGenerator());
		generators.add(new UserArticleLinkGenerator());
		generators.add(new UserUserRefGenerator());
		generators.add(new UserUserTalkGenerator());
		generators.add(new ConversationGenerator());
	}

	/**
	 * Useful for testing, but uses too much memory for production.
	 * (Some articles have a ridiculous number of revisions).
	 * @param parser
	 * @return
	 */
	public ArrayList<Edge> findEdges(PageParser parser) throws XMLStreamException {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		Page article = parser.getArticle();
		RevisionFingerprinter fparser = new RevisionFingerprinter();
		while (true) {
			Revision rev = parser.getNextRevision();
			if (rev == null) {
				break;
			}
			edges.addAll(findEdges(fparser, article, rev));
			fparser.update(rev);
		}
		return edges;
	}

	/**
	 * Finds and returns a list of Edges pertaining to a particular Page.
	 * Instantiates all Articles and Users that it is linked with.
	 * @param fparser The fingerprinting parser that should be used.
	 * @param article the article for which to find edges
	 * @param rev the revision for which to find edges
	 * @return a list of all Edges
	 * @throws XMLStreamException
	 */
	public ArrayList<Edge> findEdges(RevisionFingerprinter fparser, Page article, Revision rev) throws XMLStreamException {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		for (LinkGenerator generator : generators) {
			if (generator instanceof FingerprintingLinkGenerator) {
				((FingerprintingLinkGenerator)generator).setFingerprintingParser(fparser);
			}
			List<Edge> l = generator.generate(article, rev);
			if (l != null) {
				edges.addAll(l);
			}
		}
		return edges;
	}
}
