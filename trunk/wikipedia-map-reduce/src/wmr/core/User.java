package wmr.core;

import java.util.ArrayList;

public class User extends Vertex {
	
	public User() {
		super();
	}

	public User(String id) {
		//FIXME: correctly handle single-input creations: should they be names or ids?
		super(id);
	}

	public User(String name, String id) {
		super(name, id);
	}

        public boolean isAnonymous() {
            return (getName() == null || getName().equals("null"));
        }

	public ArrayList<Revision> getRevisionsByArticle(Page article) {
		ArrayList<Revision> revList = new ArrayList<Revision>();
		for (Revision r : article.getRevisions()) {
			if (r.getContributor().equals(this)) {
				revList.add(r);
			}
		}
		return revList;
	}

	public boolean isCoAuthor (Page article, User user) {
		boolean coAuthor = false;
		if (article.getUsers().contains(this) && article.getUsers().contains(user)) {
			coAuthor = true;
		}
		return coAuthor;
	}

        public boolean isBot() {
            return (getName() != null) && (getName().toLowerCase().indexOf("bot") >= 0);
        }

}
