package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;

public class UserArticleLinkGenerator implements LinkGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> links = null;
		if (article.isUser()) {
			links = new ArrayList<Edge>();
			for (String ref : revision.getAnchorLinks()) {
				if (!ref.contains("User:") && !ref.contains("User talk:")) {
					links.add(new Edge(article, new Page(ref), Edge.USER_LINK_ART));
				}
			}
		}
		return links;
	}

}
