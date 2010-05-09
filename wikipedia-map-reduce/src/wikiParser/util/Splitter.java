package wikiParser.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author shilad
 */
public class Splitter {
    public static final int DELIM_TABLE_SIZE = 256;
    public static final String ALPHA_CHARS = "abcdefghijklmnopqrstuvwxyz";
    public static final String NUMERIC_CHARS = "0123456789";
    public static final String ALPHANUMERIC_CHARS =
            ALPHA_CHARS + ALPHA_CHARS.toUpperCase() + NUMERIC_CHARS;
    public static final String DEFAULT_DELIMS = createInverseOfChars(ALPHANUMERIC_CHARS);

    /**
	 * @uml.property  name="isDelim" multiplicity="(0 -1)" dimension="1"
	 */
    private boolean[] isDelim = new boolean[DELIM_TABLE_SIZE];
    /**
	 * @uml.property  name="keepDelims"
	 */
    private boolean keepDelims;

    /**
     * Creates a new splitter object.
     * @param delimChars Characters that should be used as delimiters.  (Not a regex).
     * @param keepDelims Whether or not the splitter should return the delimiter sequences.
     */
    public Splitter(String delimChars, boolean keepDelims) {
        Arrays.fill(isDelim, false);
        for (char c : delimChars.toCharArray()) {
            if (c >= isDelim.length) {
                throw new IllegalArgumentException(
                        "delim char " + c +
                        " does not have ascii codes less than " + isDelim.length);
            }
            isDelim[c] = true;
        }
        this.keepDelims = keepDelims;
    }
    
    public Splitter(boolean keep_delimiters){
        this(DEFAULT_DELIMS, keep_delimiters);
    }

    private static final int STATE_NONE = 0;
    private static final int STATE_WORD = 1;
    private static final int STATE_DELIM = 2;

    /**
     *Splits the text using the previously specified delimiter characters.
     * @param text The text that will be split.
     */
    public String[] split(String text) {
//        System.out.println("TEXT WAS " + text);
        if (text == null) {
            text = "";
        }

        StringBuilder builder = new StringBuilder();

        List<String> tokens = new ArrayList<String>();
        int state = STATE_NONE;
//        char[] chars = text.toCharArray();
//
//        for (int i = 0; i < chars.length; i++) {
//            char c = chars[i];
        for (char c : text.toCharArray()) {
            if (c < isDelim.length && isDelim[c]) {
                if (state != STATE_DELIM) {
                    state = STATE_DELIM;
                    if (builder.length() > 0) {
                        tokens.add(builder.toString());
                        builder = new StringBuilder();
                    }
                }
            } else {
                if (state != STATE_WORD) {
                    state = STATE_WORD;
                    if (builder.length() > 0) {
                        if (keepDelims) {
                            tokens.add(builder.toString());
                        }
                        builder = new StringBuilder();
                    }
                }
            }
            builder.append(c);
        }
        if (builder.length() > 0) {
            if (state == STATE_WORD) {
                tokens.add(builder.toString());
            } else if (state == STATE_DELIM && keepDelims) {
                tokens.add(builder.toString());
            }
        }
        return tokens.toArray(new String[0]);
    }
    
    
    /**
     * Creates a string containing characters less than DELIM_TABLE_SIZE
     * that are not in the input string.
     * @param input
     * @return inverse of String.
     */
    public static String createInverseOfChars(String input) {
        String inverse = "";
        for (char c = 0; c < DELIM_TABLE_SIZE; c++) {
            if (input.indexOf(c) < 0) {
                inverse += c;
            }
        }
        return inverse;
    }
}
