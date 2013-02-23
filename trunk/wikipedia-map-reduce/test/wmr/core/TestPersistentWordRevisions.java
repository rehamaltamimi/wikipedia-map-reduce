package wmr.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import org.mockito.Mockito;

import wmr.wmf.PersistentWordRevisions;

public class TestPersistentWordRevisions {

	public AllRevisions fakeRevisions() {
		Page page = new Page("Some page", "15", "0");
		List<Revision> revisions = new ArrayList<Revision>();
		
		// PWRs: foo=2, bar=4, baz=0
		revisions.add(new Revision(
				"1", null, new User("shilad", "17"), "foo bar baz", null, false  
				));		

		// PWRs: sloth=1, de=3
		revisions.add(new Revision(
				"5", null, new User("joey", "23"), "foo bar sloth de", null, false  
				));		
		
		// PWRs: none, it's a bot
		revisions.add(new Revision(
				"10", null, new User("foobot", "23"), "foo bar sloth de batty", null, false  
				));		
		
		// PWRs: fungy=1
		revisions.add(new Revision(
				"17", null, new User("manny", "23"), "bar fungy de", null, false  
				));

		return new AllRevisions(page, revisions);
	}
	
	@Test
	public void test() throws IOException, InterruptedException {
		Mapper.Context context = Mockito.mock(Mapper.Context.class);
		PersistentWordRevisions.MyMapper mapper = new PersistentWordRevisions.MyMapper();
		mapper.map(17l, fakeRevisions(), context);
		Mockito.verify(context, Mockito.times(5)).progress();
		verifyWrite(context, "Some page", "1\tshilad\t6");
		verifyWrite(context, "Some page", "5\tjoey\t4");
		verifyWrite(context, "Some page", "17\tmanny\t1");
		Mockito.verifyNoMoreInteractions(context);
	}
	
	static void verifyWrite(Mapper.Context context, String key, String val) throws IOException, InterruptedException {
		Mockito.verify(context).write(new Text(key), new Text(val));
	}

}
