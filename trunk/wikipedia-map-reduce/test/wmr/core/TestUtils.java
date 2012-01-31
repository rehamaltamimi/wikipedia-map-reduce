package wmr.core;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.Assert.*;
import wmr.util.Utils;


public class TestUtils {
	
	@Test public void testMD5() {
            String md5 = Utils.md5("foo\n");
            assertEquals(md5, "d3b07384d113edec49eaa6238ad5ff00");
	}

	@Test public void testMD52() {
            String md5 = Utils.md5("April 6 is");
            System.out.println(md5);
            assertEquals(md5, "f710975926f0094ae9a8d069a8bc7c58");
	}

//
//	@Test public void testMD52() {
//            String s = "April 6 is the 96th day of the year in the [[Gregorian calendar]] (97th in [[leap year]]s). There are 269 days remaining.\n\nEvents:\n*1937: The [[Hindenburg zeppelin]] explodes\n\nDeaths:\n*1992: [[Isaac Asimov]] (author)\n\n-----\n<B>See Also:</B> [[January]], [[February]], [[March]], [[April]], [[May]], [[June]], [[July]], [[August]], [[September]], [[October]], [[November]], [[December]]\n\n[[April 5]] - [[April 7]] - [[March 6]] - [[May 6]] - more [[historical anniversaries]]";
//            String md5 = Utils.md5(s);
//            System.out.println(md5);
//            assertEquals(md5, "d3b07384d113edec49eaa6238ad5ff00");
//	}
	
}
