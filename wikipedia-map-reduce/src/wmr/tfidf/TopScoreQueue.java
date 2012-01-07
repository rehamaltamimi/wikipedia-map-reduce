/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.tfidf;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeSet;

/**
 *
 * @author shilad
 */
public class TopScoreQueue implements Iterable<WordScore> {
    private TreeSet<WordScore> pqueue;
    private int capacity;

    public TopScoreQueue(int capacity) {
        this.pqueue = new TreeSet<WordScore>();
        this.capacity = capacity;
    }

    public void add(String word, double score) {
        if (pqueue.size() < capacity || score > getLowestScore().score) {
            add(new WordScore(word, score));
        }
    }

    public void add(WordScore ws) {
        if (pqueue.size() < capacity || ws.score > getLowestScore().score) {
            pqueue.add(ws);
        }
        if (pqueue.size() > capacity) {
            pqueue.remove(pqueue.last());
        }
    }

    public Iterator<WordScore> iterator() {
        return pqueue.iterator();
    }

    public WordScore getLowestScore() {
        return pqueue.last();
    }


    public void clear() {
        pqueue.clear();
    }

}
