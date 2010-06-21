package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;

public class UserUserTalkGenerator implements EdgeGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> edges = null;
		if (article.isUserTalk()) {
			edges = new ArrayList<Edge>();
			edges.add(new Edge(revision.getContributor(), article.getUser(), Edge.USER_EDITSTALKOF_USER));
		}
		return edges;
	}

}
