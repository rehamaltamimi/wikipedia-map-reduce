/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.categories;

import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.procedure.TIntProcedure;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the WikiRelate! semantic similarity algorithm.
 * @author shilad
 */
public abstract class CategoryComparer {
  private static final Logger LOG  = Logger.getLogger(CategoryComparer.class.getPackage().getName());

    private TLongIntHashMap categoryIndexes = new TLongIntHashMap();
    private TLongHashSet topLevelCategories = new TLongHashSet();

    private int catPages[][];
    private int catParents[][];
    private int catChildren[][];

    public void prepareDataStructures() throws IOException {
        LOG.info("preparing data structures...");
        setCategoryIndexes();
        allocateArrays();
        fillArrays();
        calculateTopLevelCategories();
    }

    public void searchPages() throws IOException {
        openFile();
        int i = 0;
        while (true) {
            String line = readLine();
            if (line == null) {
                break;
            }
            CategoryRecord record = parseLine(line, true);
            if (record != null) {
                findSimilar(record);
                if (i++ % 10000 == 0) {
                    LOG.info("exploring page " + i);
                }
            }
        }
        closeFile();
    }

    protected void findSimilar(final CategoryRecord record) {
        TIntHashSet pagesTraversed = new TIntHashSet();
        pagesTraversed.add(record.getPageId());
        for (int depth = 0; depth <= 4; depth++) {
//            LOG.log(Level.INFO, "exploring to depth {0}", depth);
            TIntHashSet newPagesTraversed = new TIntHashSet(pagesTraversed);
            for (int ci : record.getCategoryIndexes()) {
                TIntHashSet catsTraversed = new TIntHashSet();
                exploreToDepth(ci, depth, 10000, newPagesTraversed, catsTraversed, +1);
            }
            final TIntHashSet pt = pagesTraversed;
            final int d = depth;
            newPagesTraversed.forEach(new TIntProcedure() {
                public boolean execute(int pageId) {
                    if (!pt.contains(pageId)) {
                        try {
                            writeResult(record.getPageId(), pageId, d);
                        } catch (IOException ex) {
                            LOG.log(Level.SEVERE, null, ex);
                        }
                    }
                    return true;
                }

            });
            pagesTraversed = newPagesTraversed;
            LOG.log(Level.INFO, "found {0} pages up to depth {1}", new Object[] {pagesTraversed.size(), depth});
        }
    }

    public void setCategoryIndexes() throws IOException {
        LOG.info("setting category indexes...");
        openFile();
        while (true) {
            String line = readLine();
            if (line == null) {
                break;
            }
            CategoryRecord record = parseLine(line, false);
            if (record == null || !record.isCategory()) {
                continue;
            }
            long hash = getCategoryHash(record.getPageName());
            if (!categoryIndexes.containsKey(hash)) {
                categoryIndexes.put(hash, categoryIndexes.size());
            }
        }
        closeFile();
        LOG.log(Level.INFO, "indexed {0} categories.", categoryIndexes.size());
    }

    public void allocateArrays() throws IOException {
        LOG.info("allocating arrays...");
        
        // hack: each subarray contains a single entry for counting
        catPages = new int[categoryIndexes.size()][];
        catParents = new int[categoryIndexes.size()][];
        catChildren = new int[categoryIndexes.size()][];
        for (int i = 0; i < catPages.length; i++) {
            catPages[i] = new int[1];
            catChildren[i] = new int[1];
        }
        
        openFile();
        while (true) {
            String line = readLine();
            if (line == null) {
                break;
            }
            CategoryRecord record = parseLine(line, true);
            if (record == null) {
                // do nothing
            } else if (record.isCategory()) {   // category page
                for (int ci : record.getCategoryIndexes()) {
                    catChildren[ci][0]++;
                }
            } else {    // regular page
                for (int ci : record.getCategoryIndexes()) {
                    catPages[ci][0]++;
                }
            }
        }
        closeFile();

        for (int i = 0; i < catPages.length; i++) {
            catPages[i] = new int[catPages[i][0]];
            catChildren[i] = new int[catChildren[i][0]];
        }
    }


    public void fillArrays() throws IOException {
        LOG.info("filling arrays...");

        int catPageIndexes[] = new int[categoryIndexes.size()];;
        int catChildrenIndexes[] = new int[categoryIndexes.size()];
        
        openFile();
        while (true) {
            String line = readLine();
            if (line == null) {
                break;
            }
            CategoryRecord record = parseLine(line, true);
            if (record == null) {
                // do nothing
            } else if (record.isCategory()) {
                int index = getCategoryIndex(record.getPageName());
                catParents[index] = record.getCategoryIndexes();
                for (int ci : record.getCategoryIndexes()) {
                    catChildren[ci][catChildrenIndexes[ci]++] = index;
                }
            } else {    // regular page
                for (int ci : record.getCategoryIndexes()) {
                    catPages[ci][catPageIndexes[ci]++] = record.getPageId();
                }
            }
        }
        closeFile();

    }

    private void calculateTopLevelCategories() {
        LOG.info("marking top level categories off-limits.");
        int numSecondLevel = 0;
        for (String name : TOP_LEVEL_CATS) {
            int index = getCategoryIndex("Category:" + name);
            if (index >= 0) {
                topLevelCategories.add(index);
                for (int ci : catChildren[index]) {
                    topLevelCategories.add(ci);
                    numSecondLevel++;
                }
            }
        }
        LOG.log(Level.INFO, "marked {0} top-level and {1} second-level categories.",
                new Object[] {TOP_LEVEL_CATS.length, numSecondLevel} );
    }
    
    public abstract void openFile() throws IOException;
    public abstract String readLine() throws IOException;
    public abstract void writeResult(int pageId, int similarPageId, int distance) throws IOException;
    public abstract void closeFile() throws IOException;

    public long getCategoryHash(String cat) {
        cat = cat.trim();
        long h = 1;
        for (int i = 0; i < cat.length(); i++) {
            h = h * 37 + (long)cat.charAt(i);
        }
        return h;
    }

    public int getCategoryIndex(String cat) {
        long h = getCategoryHash(cat);
        if (categoryIndexes.contains(h)) {
            return categoryIndexes.get(h);
        } else {
            return -1;
        }
    }

    public CategoryRecord parseLine(String line, boolean fillCats) {
        String tokens [] = line.trim().split("\t");
        if (tokens.length < 3) {
            return null;
        }
        int pageId = -1;
        try {
            pageId = Integer.valueOf(tokens[0]);
        } catch (Exception e) {
            return null;
        }
        char type = tokens[1].charAt(0);    // 'p' (page) or 'c' (cat)
        String name = tokens[2].trim();
        CategoryRecord record = new CategoryRecord(type, pageId, name);
        if (fillCats) {
            TIntArrayList indexes = new TIntArrayList(tokens.length - 3);
            for (int i = 3; i < tokens.length; i++) {
                String cat = tokens[i].trim();
                long hash = getCategoryHash(cat);
                if (categoryIndexes.contains(hash)) {
                    indexes.add(categoryIndexes.get(hash));
                }
            }
            record.setCategoryIndexes(indexes.toArray());
        }
        return record;
    }

    private final void exploreToDepth(
            int ci, int stepsRemaining, int maxPages,
            TIntHashSet pagesTraversed, TIntHashSet catsTraversed,
            int direction) {
        assert(direction == +1 || direction == -1);
        if (pagesTraversed.size() >= maxPages 
        ||  catsTraversed.contains(direction*ci)
        ||  topLevelCategories.contains(ci)) {
            return;
        }
        catsTraversed.add(direction*ci);

        // base case
        if (stepsRemaining == 0) {
            for (int pageId : catPages[ci]) {
                pagesTraversed.add(pageId);
                if (pagesTraversed.size() >= maxPages) {
                    return;
                }
            }
            return;
        }

        if (direction == +1) {
            for (int ci2 : catParents[ci]) {
                exploreToDepth(ci2, stepsRemaining - 1, maxPages, pagesTraversed, catsTraversed, +1);
            }
        }
        for (int ci2 : catChildren[ci]) {
            exploreToDepth(ci2, stepsRemaining - 1, maxPages, pagesTraversed, catsTraversed, -1);
        }
    }

    public static String [] TOP_LEVEL_CATS = {
            "Agriculture", "Applied Sciences", "Arts", "Belief", "Business", "Chronology", "Computers",
            "Culture", "Education", "Environment", "Geography", "Health", "History", "Humanities",
            "Language", "Law", "Life", "Mathematics", "Nature", "People", "Politics", "Science", "Society",

            "Concepts", "Life", "Matter", "Society",

           };


}
