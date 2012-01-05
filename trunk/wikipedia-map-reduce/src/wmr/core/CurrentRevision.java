/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.core;

/**
 *
 * @author shilad
 */
public class CurrentRevision {
    private Page page;
    private Revision revision;

    CurrentRevision(Page page, Revision revision) {
        this.page = page;
        this.revision = revision;
    }

    public Page getPage() {
        return page;
    }

    public Revision getRevision() {
        return revision;
    }
}
