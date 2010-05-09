package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;

public class UserUserTalkGenerator implements LinkGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> links = null;
		if (article.isUserTalk()) {
			links = new ArrayList<Edge>();
			links.add(new Edge(revision.getContributor(), article.getUser(), Edge.USER_EDITSTALKOF_USER));
		}
		return links;
	}

}
