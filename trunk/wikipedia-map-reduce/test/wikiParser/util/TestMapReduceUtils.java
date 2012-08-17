package wikiParser.util;


import static org.junit.Assert.assertEquals;


import org.apache.hadoop.io.Text;
import org.junit.Test;

import wmr.util.Utils;


public class TestMapReduceUtils {
	@Test public void testKeyToLong() throws Exception {
            Text t = new Text("324242.xml.7z");
            assertEquals(Utils.keyToId(t), 324242);
	}

}
