package wikiParser.mapReduce.util;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.apache.hadoop.io.Text;

import wmr.core.PageParser;
import wmr.util.LzmaDecompresser;

public class MapReduceUtils {
        public static String [] S3_INPUTS = new String[] {
                "s3n://macalester/data/wikipedia.*.txt",
            };
        public static String [] S3_TEST = new String[] {
                "s3n://macalester/data/wikipedia.00.txt",
            };
        public static String [] MIST_INPUTS = new String[] {
                "/user/shilad/wikipedia.txt"
            };
	public static String [] MIST_TEST = new String[] {
                "/user/cwelch/10000.txt"
            };
        public static String [] SHILAD_TEST = new String[] {
                "/Users/shilad/Documents/Intellij/Macademia/all.0.txt"
            };
	public static String [] COMM_LINK_INUPTS = new String[] {
		"/user/cwelch/sub_links_alt.txt"
	    };
        public static String S3_OUTPUT = "s3n://macalesterout";


	public static PageParser formatArticleParser(Text value)
	throws XMLStreamException, IOException {
		LzmaDecompresser pipe = null;
		try {
			byte [] unescaped = unescape(value.getBytes(), value.getLength());
			pipe = new LzmaDecompresser(unescaped);
			PageParser parser = new PageParser(pipe.decompress());
			parser.setStoreFullTextInArticle(false);
			parser.setStoreRevisionMetadata(false);
			return parser;
		} finally {
			if (pipe != null) {
				pipe.cleanup();
			}
		}
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
            return unescapeInPlace(escaped, 0, length);
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
