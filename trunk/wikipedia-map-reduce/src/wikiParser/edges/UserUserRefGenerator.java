package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;
import wikiParser.User;

public class UserUserRefGenerator implements EdgeGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> edges = null;
		if (article.isUserTalk()) {
			edges = new ArrayList<Edge>();
			for (String ref : revision.getAnchorLinks()) {
				if (ref.contains("User:") || ref.contains("User talk:")) {
					edges.add(new Edge(article.getUser(), new User(ref), Edge.USER_LINKSTO_USER));
				}
			}
		}
		return edges;
	}

}
