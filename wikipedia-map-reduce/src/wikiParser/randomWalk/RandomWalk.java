/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.randomWalk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
* @author Nathaniel Miller
 */
public class RandomWalk {
    
    //private HashMap<HashSet<String>,HashSet<NodePair>> vectors = new HashMap<HashSet<String>,HashSet<NodePair>>();//for use in accessing a single row of the matrix later - not necessary?
    public static final int NUMBER_OF_STEPS = 14;//possible value. should experiment on sample in order to find better one...
    
    public HashMap<NodePair,Integer> randomWalk(InputStream is) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        line = reader.readLine();
        HashMap<String,String> adjacencyList = new HashMap<String,String>();
        while (line != null) {
            String[] split = line.split("\t");
            adjacencyList.put(split[0], split[1]);
            line = reader.readLine();
        }
        HashMap<NodePair,Integer> similarities = new HashMap<NodePair,Integer>();
        for (String s : adjacencyList.keySet()) {
            HashSet<String> nodes = new HashSet<String>();
            nodes.add(s);
            String currentNode = s;
            for (int i = 0; i < NUMBER_OF_STEPS; i++) {
                currentNode = (new RandomStep(adjacencyList.get(s))).getNextNode();
                nodes.add(currentNode);
            }
            for (String nodeOne : nodes) {
                for (String nodeTwo : nodes) {
                    if (!nodeOne.equals(nodeTwo)) {
                        NodePair np = new NodePair(nodeOne,nodeTwo);
                        //adds pair to sparse similarity matrix
                        if (similarities.get(np) == null) {
                            similarities.put(np, 1);
                        } else {
                            similarities.put(np, similarities.get(np) + 1);
                        }
                        /*
                        //adds pair to vector for node one
                        HashSet<String> one = new HashSet<String>();
                        one.add(nodeOne);
                        if (vectors.get(one) == null) {
                            vectors.put(one, new HashSet<NodePair>());
                            vectors.get(one).add(np);
                        } else if (!vectors.get(one).contains(np)) {
                            vectors.get(one).add(np);
                        }
                        //adds pair to vector for node two
                        HashSet<String> two = new HashSet<String>();
                        two.add(nodeTwo);
                        if (vectors.get(two) == null) {
                            vectors.put(two, new HashSet<NodePair>());
                            vectors.get(two).add(np);
                        } else if (!vectors.get(two).contains(np)) {
                            vectors.get(two).add(np);
                        }*/
                    }
                }
            }
        }
        return similarities;
    }
    
    /*public HashMap<HashSet<String>,HashSet<NodePair>> getVectors() {
        return vectors;
    }*/
    
    
}
