package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wmr.core.Page;
import wmr.core.Edge;
import wmr.core.Revision;

public class ContributorEditGenerator implements EdgeGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> edges = null;
		if (article.isNormalPage() || article.isTalk()) {
			edges = new ArrayList<Edge>();
			edges.add(new Edge(article, revision.getContributor(), Edge.ART_EDITEDBY_USER));
		}
		return edges;
	}

}
