package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;

public class ArticleArticleGenerator implements EdgeGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> edges = null;
		if (article.isNormalPage()) {
			edges = new ArrayList<Edge>();
			for (String ref : revision.getAnchorLinks()) {
				edges.add(new Edge(article, new Page(ref), Edge.ART_LINK_ART));
			}
		}
		return edges;
	}

}
