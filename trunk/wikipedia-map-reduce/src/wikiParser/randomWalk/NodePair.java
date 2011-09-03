/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.randomWalk;

/**
 *
 * @author Nathaniel Miller
 */
public class NodePair {
    
    private String nodeOne;
    private String nodeTwo;
    
    public NodePair(String nodeOne, String nodeTwo) {
        this.nodeOne = nodeOne;
        this.nodeTwo = nodeTwo;
    }
    
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() == this.getClass()) {//I don't know if that is right...
            if ((nodeOne.equals(((NodePair)o).getNodeOne()) && nodeTwo.equals(((NodePair)o).getNodeTwo()))
                    || (nodeOne.equals(((NodePair)o).getNodeTwo()) && nodeTwo.equals(((NodePair)o).getNodeOne()))) {
                return true;
            }
        }
        return false;
    }
    
    public int hashCode() {
        return nodeOne.hashCode() * nodeTwo.hashCode();
    }
    
    public String getNodeOne() {
        return nodeOne;
    }
    
    public String getNodeTwo() {
        return nodeTwo;
    }
    
}
