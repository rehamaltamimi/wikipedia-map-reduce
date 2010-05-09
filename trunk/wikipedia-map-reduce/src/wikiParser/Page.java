package wikiParser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Page extends Vertex {

    public Page(String id) {
        super(id);
    }

    public Page(String name, String id) {
        super(name, id);
    }
    private static Pattern userPagePattern = Pattern.compile("User:(.+)");
    private static Pattern userTalkPagePattern = Pattern.compile("User talk:(.+)");

    /**
     * Generates the user who owns the article in the case of User: and
     * User talk: namespace articles.  Returns a User object with just a username
     * unless the user has also edited the article at some point, in which case
     * it returns a User object with both a username and an UID.
     * @param article the article to determine the owner of
     * @return the owner of the article
     */
    public User getUser() {
        //FIXME: This isn't finding user ids.
        User user = null;
        Matcher nameMatcher = userPagePattern.matcher(getName());
        if (nameMatcher.find()) {
            user = new User(nameMatcher.group(1));
        }
        nameMatcher = userTalkPagePattern.matcher(getName());
        if (nameMatcher.find()) {
            user = new User(nameMatcher.group(1));
        }
        // Looks for a revision by them if there are any revisions, for the ID.
        if (!this.getRevisions().isEmpty()) {
            for (Revision rev : this.getRevisions()) {
                if (rev.getContributor().getName().equals(user.getId())) {
                    user = new User(nameMatcher.group(1), rev.getContributor().getId());
                }
            }
        }
        return user;
    }

    public ArrayList<User> getUsers() {
        ArrayList<User> users = new ArrayList<User>();
        for (Revision r : this.getRevisions()) {
            users.add(r.getContributor());
        }
        return users;
    }

    /**
     * Retrieves a set of all unique anchor tag targets originating from this article.
     * @return a set of unique hyperlink targets
     */
    public Set<String> getAnchorLinks() {
        Set<String> articleLinks = new HashSet<String>();
        for (Revision r : this.getRevisions()) {
            articleLinks.addAll(r.getAnchorLinks());
        }
        return articleLinks;
    }

    /**
     * Retrieves a set of all unique contributors to this article.
     * @return a set of unique contributors
     */
    // FIXME: This isn't adding all, even when they are unique.
    public Set<User> getContributors() {
        Set<User> contributors = new HashSet<User>();
        for (User u : this.getUsers()) {
            contributors.add(u);
        }
        return contributors;
    }

    public boolean isTalk() {
        return getName().startsWith("Talk:");
    }

    public boolean isUser() {
        return getName().startsWith("User:");
    }

    public boolean isUserTalk() {
        return getName().startsWith("User talk:");
    }

    public boolean isProject() {
        return getName().startsWith("Wikipedia:")
                || getName().startsWith("WP:")
                || getName().startsWith("Project:");
    }

    public boolean isProjectTalk() {
        return getName().startsWith("Wikipedia talk:")
                || getName().startsWith("WT:")
                || getName().startsWith("Project talk:");
    }

    public boolean isPortal() {
        return getName().startsWith("Portal:");
    }

    public boolean isPortalTalk() {
        return getName().startsWith("Portal talk:");
    }

    public boolean isFile() {
        return getName().startsWith("File:")
                || getName().startsWith("Image:");
    }

    public boolean isFileTalk() {
        return getName().startsWith("File talk:")
                || getName().startsWith("Image talk:");
    }

    public boolean isMediaWiki() {
        return getName().startsWith("MediaWiki:");
    }

    public boolean isMediaWikiTalk() {
        return getName().startsWith("MediaWiki talk:");
    }

    public boolean isTemplate() {
        return getName().startsWith("Template:");
    }

    public boolean isTemplateTalk() {
        return getName().startsWith("Template talk:");
    }

    public boolean isCategory() {
        return getName().startsWith("Category:");
    }

    public boolean isCategoryTalk() {
        return getName().startsWith("Category talk:");
    }

    public boolean isBook() {
        return getName().startsWith("Book:");
    }

    public boolean isBookTalk() {
        return getName().startsWith("Book talk:");
    }

    public boolean isHelp() {
        return getName().startsWith("Help:");
    }

    public boolean isHelpTalk() {
        return getName().startsWith("Help talk:");
    }

    public boolean isNormalPage() {
        return (getName().indexOf(':') < 0);
    }

    public boolean isAnyTalk() {
        return getName().contains(" talk:");
    }
}
