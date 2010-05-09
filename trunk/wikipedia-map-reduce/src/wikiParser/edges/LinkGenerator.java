package wikiParser.edges;

import java.util.List;
import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;

public interface LinkGenerator {	

    /**
     * Generates links for a particular revision of an article.
     * Returns null if the link generator does not apply to this article.
     * @param article
     * @param revision
     * @return
     */
    public List<Edge> generate(Page article, Revision revision);

}
