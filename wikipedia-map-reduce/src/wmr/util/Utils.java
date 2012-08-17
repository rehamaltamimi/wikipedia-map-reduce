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

import org.apache.hadoop.io.Text;



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

    public static String escapeWhitespace(String s) {
    	return new String(escapeWhitespace(s.getBytes()));
    }
    
    public static byte[] escapeWhitespace(byte[] in) {
    	int escapes = 0;
    	for (int i = 0; i < in.length; i++) {
    		byte c = in[i];
    		if (c == '\\' || c == '\n' || c == '\r' || c == '\t') {
    			escapes++;
    		}
    	}
    	
    	byte[] out = new byte[in.length + escapes];
    	int j = 0;
    	
    	for (int i = 0; i < in.length; i++) {
            byte ch = in[i];
            switch (ch) {
            case '\\':
            	out[j++] = '\\';
            	out[j++] = '\\';
                break;
            case '\n':
            	out[j++] = '\\';
            	out[j++] = 'n';
                break;
            case '\r':
            	out[j++] = '\\';
            	out[j++] = 'r';
                break;
            case '\t':
            	out[j++] = '\\';
            	out[j++] = 't';
                break;
            default:
            	out[j++] = ch;
            }
        }
    	return out;
    }
    
    public static String unescapeWhitespace(String s) {
    	byte[] bytes = s.getBytes();
    	int l = unescapeInPlace(bytes, bytes.length);
    	return new String(bytes, 0, l);
    }

	public static byte[] unescape(byte [] escaped, int length) {
		int newLength = 0;
		for (int i = 0; i < length; i++) {
			newLength++;
			if (escaped[i] == '\\') {
				i++;
			}
		}
		byte [] unescaped = new byte[newLength];
		int j = 0;	// index into unescaped string
		for (int i = 0; i < length; i++) {
			byte b = escaped[i];
			if (escaped[i] == '\\') {
				switch (escaped[i+1]) {
				case 'r': b = '\r'; break;
				case '\\': b = '\\'; break;
				case 'n': b = '\n'; break;
				case 't': b = '\t'; break;
	                                
				default:
					throw new RuntimeException("unexpected character following escape " + escaped[i+1]);
				}
				i++;
			}
			unescaped[j++] = b;
		}
		return unescaped;
	}

	public static int unescapeInPlace(byte [] escaped, int length) {
	    return Utils.unescapeInPlace(escaped, 0, length);
	}

	public static int unescapeInPlace(byte [] escaped, int offset, int length) {
		int j = 0;	// index into unescaped string
		for (int i = offset; i < length; i++) {
			byte b = escaped[i];
			if (escaped[i] == '\\') {
				switch (escaped[i+1]) {
				case 'r': b = '\r'; break;
				case '\\': b = '\\'; break;
				case 'n': b = '\n'; break;
				case 't': b = '\t'; break;
				default:
					throw new RuntimeException("unexpected character following escape " + escaped[i+1]);
				}
				i++;
			}
			escaped[j++] = b;
		}
		return j;
	}

	/**
	 * Convert a map-reduce key such as "324242.xml.7z" to 324242
	 * @param key
	 * @return
	 */
	static public long keyToId(Text key) {
	    String s = key.toString();
	    int i = s.indexOf('.');
	    if (i >= 0) {
	        return Long.valueOf(s.substring(0, i));
	    } else {
	        return -1;
	    }
	}

}
