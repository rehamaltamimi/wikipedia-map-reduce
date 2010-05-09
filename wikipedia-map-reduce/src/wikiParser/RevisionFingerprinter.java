package wikiParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import wikiParser.util.Splitter;

public class RevisionFingerprinter {

    /**
	 * @uml.property  name="hashCodeMap"
	 * @uml.associationEnd  multiplicity="(1 1)"
	 */
    private HashCodeMap hashCodeMap;
    
    /** Window of num words + delims that yields a neighborly edit */
    private static final int NEIGHBORHOOD_SIZE = 100;

    /** Minimum num of words + delims that we expect to be "meaningful" */
    private static final int WINDOW_SIZE = 10;

    /** Number of recent hashcodes to remember per article.  Hashcode needed
     * for every word and delimiter */
    private static final int NUM_CACHED_HASHES = 100000;

    private static final String EOF_MARKER = "2&~!#'";
    
    private static final int EOF_HASH = getEofHash();

    public RevisionFingerprinter(int mapSize) {
        this.hashCodeMap = new HashCodeMap(mapSize);
    }
    public RevisionFingerprinter() {
        this(NUM_CACHED_HASHES);
    }

    public Set<User> neighboringContributors(Revision rev) {
        return neighboringContributors(rev, findUniqueText(rev));
    }

    /**
     *
     * @param revision
     * @return
     */
    public Set<User> neighboringContributors(Revision rev, ArrayList<String> uniqueText) {
        Set<User> neighborSet = new HashSet<User>();
        /* 1. For each in uniqueText
         * 2. Find the text in the revision text
         * 3. Travel back a number of words equal to the NEIGHBORHOOD_SIZE
         * 4. Determine old text owners and add them to neighbors
         * 5. Return neighbors.
         */
        String revText = rev.getText();
        String[] tokens = makeTokens(revText, true);
        int tokenIndex = 0;
        for (String text : uniqueText) {
            String[] splitText = makeTokens(text, true);

            try {

            // Find start of text
            while (!isSubstring(tokens, tokenIndex, splitText)) {
                tokenIndex++;
            }
            
            // have now found unique text
            // neighborhood before unique text
            int i = Math.max(0, tokenIndex - NEIGHBORHOOD_SIZE);
            for (; i < tokenIndex; i++) {
                int hash = hash(tokens, i);
                if (!this.hashCodeMap.isNew(hash)) {
                    User neighbor = this.hashCodeMap.get(hash).getContributor();
                    if (!neighbor.equals(rev.getContributor())) {
                        neighborSet.add(neighbor);
                    }
                }
            }

            // neighborhood after unique text
            tokenIndex += splitText.length - WINDOW_SIZE*2;
            i = tokenIndex;
            int end = Math.min(i + NEIGHBORHOOD_SIZE, tokens.length-WINDOW_SIZE);
            for (; i < end; i++) {
                int hash = hash(tokens, i);
                if (!hashCodeMap.isNew(hash)) {
                    User neighbor = this.hashCodeMap.get(hash).getContributor();
                    if (!neighbor.equals(rev.getContributor())) {
                        neighborSet.add(neighbor);
                    }
                }
            }
        } catch (RuntimeException e) {
            System.err.println("converted ||" + text + "|| to ||" + Arrays.toString(splitText) + "||");
            throw e;
        }
        }
        return neighborSet;
    }

    /**
	 * See thirdParty.splitter.Splitter Does not alter the RevisionFingerprinter's internal HashCodeMap. Keeps a local instance, but that may be removed. Warning: NOT THREADSAFE RIGHT NOW (use threadlocal for cache).
	 * @param revision
	 * @return
	 * @uml.property  name="lastRevision"
	 * @uml.associationEnd  
	 */
    private Revision lastRevision = null;
    /**
	 * @uml.property  name="lastUniqueText"
	 * @uml.associationEnd  multiplicity="(0 -1)" elementType="java.lang.String"
	 */
    private ArrayList<String> lastUniqueText = null;
    
    public ArrayList<String> findUniqueText(Revision revision) {
        if (revision == lastRevision) {
            return lastUniqueText;
        }

        ArrayList<String> uniqueText = new ArrayList<String>();
        StringBuilder newTextSection = new StringBuilder();
        String[] mixed = makeTokens(revision.getText(), true);    // tokens and delims
        boolean newSection = false;

        // start at index 1 to skip the opening EOF_MARKER
        // assuming that we're in old text at the beginning
        for (int i = 1; i < mixed.length - WINDOW_SIZE; i++) {
            int hash = hash(mixed, i);
            if (this.hashCodeMap.isNew(new Integer(hash))) {
                // in new text
                if (newSection == false) {
                    // have moved from old text to new text;;
                    // new word is at the end of the window
                    i += WINDOW_SIZE - 1;
                    newSection = true;
                }
                // The following can happen in some crazy circumstances,
                // for instance if an editor deletes at the end of an article.
                // != is okay because we all use the EOF_MARKER object.
                if (mixed[i] != EOF_MARKER) {
                    newTextSection.append(mixed[i]);
                }
            } else {
                // in old text
                if (newSection == true) {
                    // have moved from new text to old text
                    // end of new text section
                    if (newTextSection.length() > 0) {
                        uniqueText.add(newTextSection.toString());
                    }
                    newTextSection = new StringBuilder();
                    newSection = false;
                }
            }
        }
        uniqueText.add(newTextSection.toString());
        
        // update the cache
        lastRevision = revision;
        lastUniqueText = uniqueText;

        return uniqueText;
    }

    /**
     * Returns a simple hashcode for an array of words.
     * @param words
     * @return
     */
    private static int hash(String[] words) {
        return hash(words, 0);
    }

    private static int hash(String[] words, int offset) {
        int hashcode = 0;
        for (int w = 0; w < WINDOW_SIZE; w++) {
            hashcode = hashcode * 31 + words[w + offset].hashCode();
        }
        return hashcode;
        
    }

    /**
     * Returns the hashcode for a window repeating the EOF marker.
     * @return
     */
    private static int getEofHash() {
        String[] words = new String[WINDOW_SIZE];
        Arrays.fill(words, EOF_MARKER);
        return hash(words);
    }

    // Splitters with and without delims
    private static final Splitter SPLITTER_DELIMS = new Splitter(true);
    private static final Splitter SPLITTER_NO_DELIMS = new Splitter(false);

    /**
     * Returns the tokens in the input (with or without delims), fulled
     * by WINDOW_SIZE tokens of EOF_MARKER.
     * @param input
     * @param keepDelims
     * @return
     */
    private String[] makeTokens(String input, boolean keepDelims) {
        Splitter splitter = (keepDelims) ? SPLITTER_DELIMS : SPLITTER_NO_DELIMS;
        String[] tokens = splitter.split(input);
        String[] result = new String[tokens.length + WINDOW_SIZE*2];
        int i = 0;
        for (int j = 0; j < WINDOW_SIZE; j++) {
            result[i++] = EOF_MARKER;
        }
        for (int j = 0; j < tokens.length; j++) {
            result[i++] = tokens[j];
        }
        for (int j = 0; j < WINDOW_SIZE; j++) {
            result[i++] = EOF_MARKER;
        }
        assert(i == result.length);
        return result;
    }

    /**
     * Updates the internal HashCodeMap with this revision's data.
     * Called after any other RevisionFingerprinter methods.
     * @param revision new revision to hash
     */
    public void update(Revision revision) {
        String[] tokens = makeTokens(revision.getText(), true);
        for (int i = 0; i < tokens.length - WINDOW_SIZE; i++) {
            int h = hash(tokens, i);
            if (this.hashCodeMap.isNew(h)) {
                this.hashCodeMap.add(h, revision);
            }
        }
    }

    public int getCacheSize() {
        return hashCodeMap.size();
    }

    // Checks if the text matches the query at position offset.
    // Assumes the query is padded with the EOF_MARKER
    private static boolean isSubstring(String[] text, int offset, String[] query) {
        for (int i = 0; i < query.length - 2 * WINDOW_SIZE; i++) {
            if (!text[offset+i].equals(query[i + WINDOW_SIZE])) {
                return false;
            }
        }
        return true;

    }
}
