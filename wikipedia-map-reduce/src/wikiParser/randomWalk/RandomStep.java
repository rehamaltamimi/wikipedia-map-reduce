/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.randomWalk;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

/**
 *
 * @author Nathaniel Miller
 */
public class RandomStep {
    
    private HashMap<Integer, EdgeWeight> weightMap = new HashMap<Integer, EdgeWeight>();
    private int[] limits;
    private Random rand = new Random();
    
    /**
     * Sets up the RandomStep using an edge list given by the output from CommutativeArticleLinkMapReduce
     * @param edgeList an output value from CommutativeArticleLinkMapReduce
     */
    public RandomStep(String edgeList) {
        String[] edges = edgeList.split(" ");
        LinkedList<EdgeWeight> edgeWeights = new LinkedList<EdgeWeight>();
        limits = new int[edges.length];
        limits[0] = 0;
        for (int i = 0; i < edges.length; i++) {
            String[] properties = edges[i].split("\\|");
            int weight = Integer.parseInt(properties[1]);
            if (i < limits.length - 1) {
                limits[i + 1] = weight + limits[i-1];
            }
            weightMap.put(limits[i],new EdgeWeight(properties[2], weight));
        }
    }
    
    public int getTotalWeight() {
        return limits[limits.length - 1] + weightMap.get(limits[limits.length - 1]).getWeight();
    }
    
    /**
     * finds a random next node based upon the edge weights
     * @return 
     */
    public String getNextNode() {
        int w = rand.nextInt(this.getTotalWeight());
        int l = 0;
        int h = limits.length;
        while (h - l > 1) {
            int i = (int)Math.floor((h-l)/2) + l;
            if (limits[i] > w) {
                h = i;
            } else if (limits[i] < w) {
                l = i;
            } else {
                return weightMap.get(limits[i]).getName();
            }
        }
        return weightMap.get(limits[l]).getName();
    }
    
    private class EdgeWeight {
        private String name;
        private int weight;
        
        public EdgeWeight(String name, int weight) {
            this.name = name;
            this.weight = weight;
        }
        
        public int getWeight() {
            return weight;
        }
        
        public String getName() {
            return name;
        }
    }
    
}
