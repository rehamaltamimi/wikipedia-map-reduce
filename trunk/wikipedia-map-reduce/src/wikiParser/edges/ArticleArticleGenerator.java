package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;

public class ArticleArticleGenerator implements LinkGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> links = null;
		if (article.isNormalPage()) {
			links = new ArrayList<Edge>();
			for (String ref : revision.getAnchorLinks()) {
				links.add(new Edge(article, new Page(ref), Edge.ART_LINK_ART));
			}
		}
		return links;
	}

}
