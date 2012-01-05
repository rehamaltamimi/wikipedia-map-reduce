package wikiParser.edges;

import java.util.List;
import wmr.core.Page;
import wmr.core.Edge;
import wmr.core.Revision;

public interface EdgeGenerator {	

    /**
     * Generates edges for a particular revision of an article.
     * Returns null if the edge generator does not apply to this article.
     * @param article
     * @param revision
     * @return
     */
    public List<Edge> generate(Page article, Revision revision);

}
