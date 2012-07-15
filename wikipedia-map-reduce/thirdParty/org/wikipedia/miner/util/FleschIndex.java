/*
 * Adapted from Daniel Shiffman, www.shiffman.net/itp/classes/a2z/week01/FleschIndex.java
 */
package org.wikipedia.miner.util;

import java.util.StringTokenizer;

public class FleschIndex {
    static final String PATTERN_NON_WORD = "[^\\pL\\pM\\p{Nd}\\p{Nl}\\p{Pc}[\\p{InEnclosedAlphanumerics}&&\\p{So}]]+";

    public static float calculate(String content) {
        int syllables = 0;
        int sentences = 0;
        int words = 0;

        //go through all words
        for (String word : content.split(PATTERN_NON_WORD)) {
            if (word.length() > 0) {
//                if (words < 10) {
//                    System.err.print("'" + word + "' ");
//                }
                syllables += countSyllables(word);
                words++;
            }
        }
//        System.err.println("words is " + words);
        //look for sentence delimiters
        String sentenceDelim = ".:;?!";
        StringTokenizer sentenceTokenizer = new StringTokenizer(content, sentenceDelim);
        sentences = sentenceTokenizer.countTokens();

        //calculate flesch index
        final float f1 = (float) 206.835;
        final float f2 = (float) 84.6;
        final float f3 = (float) 1.015;
        float r1 = (float) syllables / (float) words;
        float r2 = (float) words / (float) sentences;
        float flesch = f1 - (f2 * r1) - (f3 * r2);

        return flesch;
    }

// A method to count the number of syllables in a word
// Pretty basic, just based off of the number of vowels
// This could be improved
    public static int countSyllables(String word) {
        int syl = 0;
        boolean vowel = false;
        int length = word.length();

        //check each word for vowels (don't count more than one vowel in a row)
        for (int i = 0; i < length; i++) {
            if (isVowel(word.charAt(i)) && (vowel == false)) {
                vowel = true;
                syl++;
            } else if (isVowel(word.charAt(i)) && (vowel == true)) {
                vowel = true;
            } else {
                vowel = false;
            }
        }

        char tempChar = word.charAt(word.length() - 1);
        //check for 'e' at the end, as long as not a word w/ one syllable
        if (((tempChar == 'e') || (tempChar == 'E')) && (syl != 1)) {
            syl--;
        }
        return syl;
    }

//check if a char is a vowel (count y)
    public static boolean isVowel(char c) {
        if ((c == 'a') || (c == 'A')) {
            return true;
        } else if ((c == 'e') || (c == 'E')) {
            return true;
        } else if ((c == 'i') || (c == 'I')) {
            return true;
        } else if ((c == 'o') || (c == 'O')) {
            return true;
        } else if ((c == 'u') || (c == 'U')) {
            return true;
        } else if ((c == 'y') || (c == 'Y')) {
            return true;
        } else {
            return false;
        }
    }
}
