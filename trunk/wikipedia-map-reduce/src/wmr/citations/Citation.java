/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.citations;

import wmr.core.Page;
import wmr.core.Revision;
import wmr.templates.Template;

/**
 *
 * @author shilad
 */
public class Citation {
    Page page;
    Revision revision;
    String url;
    Template template;
    int location;

    public Citation(Page page, Revision revision, String url, int location) {
        this.page = page;
        this.revision = revision;
        this.url = url;
        this.location = location;
    }

    public Citation(Page page, Revision revision, Template template, int location) {
        this(page, revision, getCitationUrl(template), location);
        this.template = template;
    }

    public Page getPage() {
        return page;
    }

    public Revision getRevision() {
        return revision;
    }

    public String getUrl() {
        return url;
    }

    public String getUrlDomain() {
        if (url == null) {
            return null;
        } else {
            String urlSansSpace =  url.replaceAll("[\\s]+", " ");
            String[] split = urlSansSpace.split("/");    // http://boo.com to { "http", "", "boo.com"}
            if (split.length > 2) {
                return split[2];
            } else if (urlSansSpace.startsWith("wiki:")) {
                return urlSansSpace;
            } else {
                return urlSansSpace.toLowerCase();
            }
        }
    }

    public Template getTemplate() {
        return template;
    }

    public void setTemplate(Template template) {
        this.template = template;
    }

    public int getLocation() {
        return location;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Citation other = (Citation) obj;
        if (this.page != other.page && (this.page == null || !this.page.equals(other.page))) {
            return false;
        }
        if (this.revision != other.revision && (this.revision == null || !this.revision.equals(other.revision))) {
            return false;
        }
        if ((this.url == null) ? (other.url != null) : !this.url.equals(other.url)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 17 * hash + (this.page != null ? this.page.hashCode() : 0);
        hash = 17 * hash + (this.revision != null ? this.revision.hashCode() : 0);
        hash = 17 * hash + (this.url != null ? this.url.hashCode() : 0);
        return hash;
    }

    public String toString() {
        String contents = "<citation " + url + " in " + " " + page.getName() + ", rev " + revision.getId();
        if (template != null) {
            contents += " from template " + template;
        }
        return contents + " at " + location + ">";
    }

    public static String getCitationUrl(Template t) {
        if (t.hasParam("url")) {
            return t.getParam("url");
        } else if (t.getName() != null) {
            return t.getName();
        } else {
            return "noURL";
        }
    }

}
