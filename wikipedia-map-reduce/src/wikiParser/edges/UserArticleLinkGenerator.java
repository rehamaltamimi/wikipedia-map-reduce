package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;

public class UserArticleLinkGenerator implements EdgeGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> edges = null;
		if (article.isUser()) {
			edges = new ArrayList<Edge>();
			for (String ref : revision.getAnchorLinks()) {
				if (!ref.contains("User:") && !ref.contains("User talk:")) {
					edges.add(new Edge(article, new Page(ref), Edge.USER_LINK_ART));
				}
			}
		}
		return edges;
	}

}
