package wmr.wmf;

import gnu.trove.set.hash.TIntHashSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.minidev.json.JSONValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import wmr.core.AllRevisions;
import wmr.core.Page;
import wmr.core.Revision;
import wmr.util.EasyLineReader;

public class WmfDiffMapper extends Mapper<Long, AllRevisions, Text, Text> {
    private static final Logger LOG = Logger.getLogger(WmfDiffMapper.class.getName());

    TIntHashSet filter = null;

    @Override
    public void setup(Mapper.Context context) throws IOException {
        String path = context.getConfiguration().get(WmfDiffCreator.KEY_ID_FILTER);
        if (path == null) {
            LOG.log(Level.INFO, "No id filter found in mapper.");
            return;
        }
        LOG.log(Level.INFO, "filter path was {0}", path);
        filter = new TIntHashSet();
        EasyLineReader reader = new EasyLineReader(new Path(path), context.getConfiguration());
        for (String line : reader) {
            try {
                filter.add(Integer.valueOf(line.split("\\s+")[0]));
            } catch (NumberFormatException e) {
                LOG.log(Level.WARNING, "Invalid line in id filter file: {0}", line);
            }
        }
        LOG.log(Level.INFO, "Read {0} pages into filter", filter.size());
    }

    @Override
    public void map(Long pageId, AllRevisions revs, Context context) throws IOException, InterruptedException {
        try {
            Page page = revs.getPage();
            context.progress();
            if (filter != null && !filter.contains(pageId.intValue())) {
                return;
            }
            for (Revision r : revs.getRevisions()) {
                context.progress();
                try {
                    String json = revisionJson(r, pageId, page);
                    context.write(new Text("" + pageId + " " + r.getTimestamp()), new Text(json));
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "Error processing page " + pageId + ", rev " + r.getId(), e);
                }
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error processing page " + pageId, e);
        }
    }

    private String revisionJson(Revision r, Long pageId, Page page) {
        Map<String, Object> record = new HashMap<String, Object>();
        record.put("_id", Integer.valueOf(r.getId()));
        record.put("pageId", new Integer(pageId.intValue()));
        record.put("title", page.getName());
        record.put("ns", page.getNamespace());
        record.put("tstamp", r.getTimestamp());
        record.put("comment", r.getComment());
        record.put("text", r.getText());
        if (r.getContributor().getName() != null) {
            record.put("editorName", r.getContributor().getName());
        }
        if (r.getContributor().getId() != null) {
            record.put("editorId", r.getContributor().getId());
        }
        if (r.isRevert()) {
            record.put("revert", true);
        }
        if (r.isVandalismLoose()) {
            record.put("vandalism", 'l');
        }
        if (r.isVandalismStrict()) {
            record.put("vandalism", 's'); // strict overrides loose
        }
        if (r.isMinor()) {
            record.put("minor", true);
        }
        if (r.isRedirect()) {
            record.put("redirectTo", r.getRedirectDestination());
        }
        if (r.isDisambiguation()) {
            record.put("dab", true);
        }
        String revId = "" + r.getId();
        while (revId.length() < 11) {
            revId = "0" + revId;
        }
        return JSONValue.toJSONString(record);
    }
}
