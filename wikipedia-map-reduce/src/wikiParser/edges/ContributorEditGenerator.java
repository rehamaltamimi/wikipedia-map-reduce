package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;

public class ContributorEditGenerator implements LinkGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> links = null;
		if (article.isNormalPage() || article.isTalk()) {
			links = new ArrayList<Edge>();
			links.add(new Edge(article, revision.getContributor(), Edge.ART_EDITEDBY_USER));
		}
		return links;
	}

}
