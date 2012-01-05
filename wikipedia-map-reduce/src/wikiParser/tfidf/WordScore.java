/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.tfidf;

/**
 *
 * @author shilad
 *
 * Sort order is higher scores first, with ties broken by alphabetical order.
 */
public class WordScore implements Comparable<WordScore> {

    public String word;
    public double score;

    public WordScore(String word, double score) {
        this.word = word;
        this.score = score;
    }

    public int compareTo(WordScore o) {
        if (score > o.score) {
            return -1;
        } else if (score < o.score) {
            return 1;
        } else {
            return word.compareTo(o.word);
        }
    }

    public boolean equals(Object o) {
        return compareTo((WordScore)o) == 0;
    }
}
