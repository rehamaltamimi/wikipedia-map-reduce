package wikiParser.randomWalk;

import java.util.HashSet;

/**
 *
 * @author Nathaniel Miller
 */
public class ClusterSimilarity implements Comparable<ClusterSimilarity> {
    private HashSet<String> one;
    private HashSet<String> two;
    private double similarity;

    public ClusterSimilarity(HashSet<String> one, HashSet<String> two, double similarity) {
        this.one = one;
        this.two = two;
        this.similarity = similarity;
    }
    
    public ClusterSimilarity(NodePair np, double similarity) {
        one = new HashSet<String>();
        one.add(np.getNodeOne());
        two = new HashSet<String>();
        two.add(np.getNodeTwo());
        this.similarity = similarity;
    }
    
    public boolean contains(HashSet<String> s) {
        if (one.equals(s) || two.equals(s)) {
            return true;
        }
        return false;
    }
    /**
     * This equals method ignores similarity to allow detection of a 
     * ClusterSimilarity with the same one and two, but different similarity
     * @param o
     * @return 
     */
    public boolean equals(ClusterSimilarity o) {
        if(contains(o.getOne()) && contains(o.getTwo())) {
            return true;
        }
        return false;
    }
    
    @Override
    public int compareTo(ClusterSimilarity t) {
        if (similarity > t.getSimilarity()) {
            return 1;
        } else if (similarity < t.getSimilarity()) {
            return -1;
        }
        return 0;
    }
    
    public HashSet<String> getOtherHalf(HashSet<String> s) {
        if(one.equals(s)) {
            return two;
        } else if (two.equals(s)) {
            return one;
        } else {
            return null;//probably should throw an error here, but I can't think of what error it should be...
        }
    }
    
    public double getSimilarity() {
        return similarity;
    }
    
    public HashSet<String> getOne() {
        return one;
    }
    
    public HashSet<String> getTwo() {
        return two;
    }
    
}
