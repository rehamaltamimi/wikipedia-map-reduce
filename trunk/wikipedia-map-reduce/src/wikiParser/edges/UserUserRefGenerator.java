package wikiParser.edges;

import java.util.ArrayList;
import java.util.List;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;
import wikiParser.User;

public class UserUserRefGenerator implements LinkGenerator {

	public List<Edge> generate(Page article, Revision revision) {
		List<Edge> links = null;
		if (article.isUserTalk()) {
			links = new ArrayList<Edge>();
			for (String ref : revision.getAnchorLinks()) {
				if (ref.contains("User:") || ref.contains("User talk:")) {
					links.add(new Edge(article.getUser(), new User(ref), Edge.USER_LINKSTO_USER));
				}
			}
		}
		return links;
	}

}
