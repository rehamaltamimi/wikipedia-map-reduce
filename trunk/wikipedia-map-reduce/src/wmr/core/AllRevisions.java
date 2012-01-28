/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.core;

/**
 *
 * @author shilad
 */
public class AllRevisions {
    private Page page;
    private Iterable<Revision> revisions;

    AllRevisions(Page page, Iterable<Revision> revisions) {
        this.page = page;
        this.revisions = revisions;
    }

    public Page getPage() {
        return page;
    }

    public Iterable<Revision> getRevisions() {
        return revisions;
    }
}
