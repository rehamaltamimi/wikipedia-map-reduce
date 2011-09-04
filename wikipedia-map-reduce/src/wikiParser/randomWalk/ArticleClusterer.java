/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.randomWalk;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

/**
 *
 * @author Nathaniel Miller
 */
public class ArticleClusterer {

    
    public static final double THRESHOLD = 0.1;//is this a good number? we need to think about this...
    
    public static void main(String args[]) throws IOException {
        RandomWalk randWalk = new RandomWalk();
        HashMap<NodePair,Integer> similarities = randWalk.randomWalk(System.in);
        TreeSet<ClusterSimilarity> matrix = new TreeSet<ClusterSimilarity>();
        for (NodePair np : similarities.keySet()) {
            matrix.add(new ClusterSimilarity(np,(double)similarities.get(np)));
        }
        
        while(matrix.size() > 1  && matrix.last().getSimilarity() >= THRESHOLD) {
            ClusterSimilarity greatest = matrix.last();
            HashSet<String> halfOne = greatest.getOne();
            HashSet<String> halfTwo = greatest.getTwo();
            HashSet<String> nextCluster = new HashSet<String>();
            nextCluster.addAll(halfOne);
            nextCluster.addAll(halfTwo);
            HashSet<ClusterSimilarity> toRemove = new HashSet<ClusterSimilarity>();
            toRemove.add(greatest);
            HashMap<HashSet<String>,Double> toAdd = new HashMap<HashSet<String>,Double>();
            for (ClusterSimilarity cs : matrix) {
                if(cs.contains(halfOne)) {
                    if(!cs.contains(halfTwo)) {
                        toRemove.add(cs);
                        HashSet<String> other = cs.getOtherHalf(halfOne);
                        if (toAdd.keySet().contains(other)) {
                            toAdd.put(other,toAdd.get(other) + (halfOne.size()*cs.getSimilarity()/nextCluster.size()));
                        } else {
                            toAdd.put(other, halfOne.size()*cs.getSimilarity()/nextCluster.size());
                        }
                    }
                } else if (cs.contains(halfTwo)) {
                    //we already know cs doesn't contain halfOne
                    toRemove.add(cs);
                    HashSet<String> other = cs.getOtherHalf(halfTwo);
                    if (toAdd.keySet().contains(other)) {
                        toAdd.put(other, toAdd.get(other) + (halfTwo.size()*cs.getSimilarity()/nextCluster.size()));
                    } else {
                        toAdd.put(other, halfTwo.size()*cs.getSimilarity()/nextCluster.size());
                    }
                }
            }
            for (ClusterSimilarity cs : toRemove) {
                matrix.remove(cs);
            }
            for (HashSet<String> s : toAdd.keySet()) {
                matrix.add(new ClusterSimilarity(nextCluster, s, toAdd.get(s)));
            }
        }
        HashSet<HashSet<String>> clusters = new HashSet<HashSet<String>>();
        for (ClusterSimilarity cs : matrix) {
            clusters.add(cs.getOne());
            clusters.add(cs.getTwo());
        }
        
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(args[1]))));
        for (HashSet<String> cluster : clusters) {
            StringBuilder builder = new StringBuilder();
            for (String s : cluster) {
                builder.append(s).append("|");
            }
            writer.write(builder.toString());
            writer.newLine();
        }        
    }
    
}
