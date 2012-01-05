package wikiParser.edges;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import wmr.core.Page;
import wmr.core.Edge;
import wmr.core.Revision;

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
        
        public List<Edge> generateWeighted(Page article, Revision revision) {
            List<Edge> edges = null;
            if (article.isNormalPage()) {
                    edges = new ArrayList<Edge>();
                    Set<Page> links = new HashSet<Page>();
                    for (String ref : revision.getAnchorLinks()) {
                        Page p = new Page(ref);
                        if (links.contains(p)) {
                            for (Edge e : edges) {
                                if (e.getTwo() == p) {
                                    e.incrementWeight();
                                    break;
                                }
                            }
                        } else {
                            links.add(p);
                            edges.add(new Edge(article, new Page(ref), Edge.ART_LINK_ART));
                        }
                    }
            }
            return edges;
	}

}
