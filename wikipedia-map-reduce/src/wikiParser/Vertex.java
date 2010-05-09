package wikiParser;

import java.util.ArrayList;
import java.util.List;

/**
 * An entity is a node in the wikipedia graph (either a user or an article).
 * Both User and Article extend this class.
 * 
 * @author Colin and Shilad
 */
public class Vertex {

    /**
     * @uml.property  name="name"
     */
    private String name;
    /**
     * @uml.property  name="id"
     */
    private String id;
    /**
     * @uml.property  name="revisions"
     * @uml.associationEnd  multiplicity="(0 -1)" elementType="wikiParser.Revision"
     */
    private List<Revision> revisions = new ArrayList<Revision>();
    /**
     * @uml.property  name="links"
     * @uml.associationEnd  multiplicity="(0 -1)" inverse="one:wikiParser.Edge"
     */
    private List<Edge> links = new ArrayList<Edge>();

    public Vertex(String id) {
        this.id = id;
    }

    public Vertex(String name, String id) {
        this.name = name;
        this.id = id;
    }

    /**
     * @return
     * @uml.property  name="id"
     */
    public String getId() {
        return id;
    }

    public List<Edge> getLinks() {
        return links;
    }

    /**
     * @return
     * @uml.property  name="name"
     */
    public String getName() {
        return name;
    }

    public List<Revision> getRevisions() {
        return revisions;
    }

    public ArrayList<Revision> findCommonRevisions(Vertex entity) {
        ArrayList<Revision> commonRevisions = new ArrayList<Revision>();
        for (int i = 0; i < this.getRevisions().size(); i++) {
            for (int j = 0; j < entity.getRevisions().size(); j++) {
                if (this.getRevisions().get(i).equals(entity.getRevisions().get(j))) {
                    commonRevisions.add(this.getRevisions().get(i));
                    break;
                }
            }
        }
        return commonRevisions;
    }

    public void addToRevisions(Revision newRevision) {
        this.revisions.add(newRevision);
    }

    public void addToLinks(Edge newLink) {
        this.links.add(newLink);
    }

    public String toUnderscoredString() {
        String underscoredName = null;
        if (this.getName() != null) {
            underscoredName = new String(this.getName());
            underscoredName = underscoredName.replaceAll(" ", "_");
        } else {
            underscoredName = new String(this.getId());
            underscoredName = underscoredName.replace(" ", "_");
        }
        return underscoredName;
    }

    public int hashCode() {
        return id.hashCode();
    }

    public boolean equals(Object o) {
        if (o instanceof Vertex) {
            return ((Vertex) o).id.equals(id);
        } else {
            return false;
        }
    }

    public String toString() {
        return "<wpentity id=" + getId() + " name=" + getName() + ">";
    }
}
