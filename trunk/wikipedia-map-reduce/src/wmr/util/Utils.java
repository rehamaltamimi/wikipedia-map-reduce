/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.util;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author shilad
 */
public class Utils {

    public static final Set<String> STOP_WORDS;
    static {
        STOP_WORDS = new HashSet<String>();
        STOP_WORDS.add("I"); STOP_WORDS.add("a"); STOP_WORDS.add("about");
        STOP_WORDS.add("an"); STOP_WORDS.add("are"); STOP_WORDS.add("as");
        STOP_WORDS.add("at"); STOP_WORDS.add("be"); STOP_WORDS.add("by");
        STOP_WORDS.add("com"); STOP_WORDS.add("de"); STOP_WORDS.add("en");
        STOP_WORDS.add("for"); STOP_WORDS.add("from"); STOP_WORDS.add("how");
        STOP_WORDS.add("in"); STOP_WORDS.add("is"); STOP_WORDS.add("it");
        STOP_WORDS.add("la"); STOP_WORDS.add("of"); STOP_WORDS.add("on");
        STOP_WORDS.add("or"); STOP_WORDS.add("that"); STOP_WORDS.add("the");
        STOP_WORDS.add("this"); STOP_WORDS.add("to"); STOP_WORDS.add("was");
        STOP_WORDS.add("what"); STOP_WORDS.add("when"); STOP_WORDS.add("where");
        STOP_WORDS.add("who"); STOP_WORDS.add("will"); STOP_WORDS.add("with");
        STOP_WORDS.add("and"); STOP_WORDS.add("the"); STOP_WORDS.add("www");
        STOP_WORDS.add("td"); STOP_WORDS.add("br"); STOP_WORDS.add("nbsp");
        STOP_WORDS.add("tr"); STOP_WORDS.add("p"); STOP_WORDS.add("nbsp");
    }


    public static String stem(String word) {
        Stemmer s = new Stemmer();
        s.add(word.toCharArray(), word.length());
        s.stem();
        return s.toString();
    }

    public static String truncateDouble(String s, int n) {
        if (s.length() <= n) {
            return s;
        }
        int i = s.indexOf("E");
        if (i >= 0) {
            return s.substring(0, n-2) + s.substring(i);
        } else {
            return s.substring(0, n);
        }
    }

    public static String escape(String s) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
            case '"':
                sb.append("\\\"");
                break;
            case '\\':
                sb.append("\\\\");
                break;
            case '\b':
                sb.append("\\b");
                break;
            case '\f':
                sb.append("\\f");
                break;
            case '\n':
                sb.append("\\n");
                break;
            case '\r':
                sb.append("\\r");
                break;
            case '\t':
                sb.append("\\t");
                break;
            case '/':
                sb.append("\\/");
                break;
            default:
                //Reference: http://www.unicode.org/versions/Unicode5.1.0/
                if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F') || (ch >= '\u2000' && ch <= '\u20FF')) {
                    String ss = Integer.toHexString(ch);
                    sb.append("\\u");
                    for (int k = 0; k < 4 - ss.length(); k++) {
                        sb.append('0');
                    }
                    sb.append(ss.toUpperCase());
                } else {
                    sb.append(ch);
                }
            }
        }
        return sb.toString();
    }

    public static String md5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.reset();
            md.update(input.getBytes("UTF-8"));
            byte[] digest = md.digest();
            String hash = new BigInteger(1, digest).toString(16);
            while(hash.length() < 32 ){
              hash = "0"+hash;
            }
            return hash;
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public static long longHashCode(String s) {
        long hash = 0;
        for (int i = 0; i < s.length(); i++) {
            hash = s.charAt(i) + hash * 31;
        }
        return hash;
    }

    public static String cleanupString(String str, int maxLen) {
        str = (str.length() > maxLen) ? str.substring(0, maxLen - 3) + "..." : str;
        str = str.replaceAll("\\s+", " ");
        return str;
    }
}
