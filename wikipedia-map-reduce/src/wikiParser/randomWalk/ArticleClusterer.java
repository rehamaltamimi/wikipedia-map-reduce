/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.randomWalk;

import java.io.IOException;
import java.util.HashMap;

/**
 *
 * @author Nathaniel Miller
 */
public class ArticleClusterer {

    
    public static void main(String args[]) throws IOException {
        RandomWalk randWalk = new RandomWalk();
        HashMap<NodePair,Integer> similarities = randWalk.randomWalk(System.in);
        HashMap<String,NodePair> vectors = randWalk.getVectors();
        /**
         * agglomerate clusters here, then output to file somehow...
         */
    }
    
}
