package wmr.categories;

import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntDoubleHashMap;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the WikiRelate! semantic similarity algorithm.
 * @author shilad
 */
public abstract class CategoryComparer {
  private static final Logger LOG  = Logger.getLogger(CategoryComparer.class.getPackage().getName());

    protected boolean debuggingMode = false;

    private TLongIntHashMap categoryIndexes = new TLongIntHashMap();
    private TLongHashSet topLevelCategories = new TLongHashSet();

    private int catPages[][];
    private int catParents[][];
    private int catChildren[][];
    private double catPageRanks[];  // -log(page rank) for categories
    private String catNames[];

    public void prepareDataStructures() throws IOException {
        LOG.info("preparing data structures...");
        setCategoryIndexes();
        allocateArrays();
        fillArrays();
        calculateTopLevelCategories();
        computePageRanks();
        if (debuggingMode) {
            dumpPageRanks();
        }
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
                LinkedHashMap<Integer, Double> distances = findSimilar(record);
                LOG.info("exploring page " + i);
                if (i++ % 10000 == 0) {
                    LOG.info("exploring page " + i);
                }
                writeResults(record, distances);
                if (i > 1000) {
                    break;
                }
            }
        }
        closeFile();
    }
    
    protected LinkedHashMap<Integer, Double> findSimilar(final CategoryRecord record) {
        TIntDoubleHashMap catDistances = new TIntDoubleHashMap();
        LinkedHashMap<Integer, Double> pageDistances = new LinkedHashMap<Integer, Double>();
        pageDistances.put(record.getPageId(), 0.000000);

        TIntHashSet closedCats = new TIntHashSet();
        PriorityQueue<CategoryDistance> openCats = new PriorityQueue<CategoryDistance>();
        for (int ci : record.getCategoryIndexes()) {
            openCats.add(new CategoryDistance(ci, catPageRanks[ci], (byte)+1));
        }

        while (openCats.size() > 0 && pageDistances.size() < 20000) {
            CategoryDistance cs = openCats.poll();

            // already processed better match
            if (closedCats.contains(cs.getCatIndex())) {
                continue;
            }
            closedCats.add(cs.getCatIndex());

//            System.out.println("processing " + cs.getCatIndex() +
//                    ", score: " + cs.getDistance() +
//                    ", length: " + catPages[cs.getCatIndex()].length);

            // add directly linked pages
            for (int i : catPages[cs.getCatIndex()]) {
                if (!pageDistances.containsKey(i) || pageDistances.get(i) > cs.getDistance()) {
                    pageDistances.put(i, cs.getDistance());
                }
                if (pageDistances.size() >= 20000) {
                    break;  // may be an issue for huge categories
                }
            }

            // next steps downwards
            for (int i : catChildren[cs.getCatIndex()]) {
                double d = cs.getDistance() + catPageRanks[cs.getCatIndex()];
                if (!closedCats.contains(i) && !catDistances.containsKey(i)) {
                    catDistances.put(i, d);
                    openCats.add(new CategoryDistance(i, d, (byte)-1));
                } else {
                     assert(catDistances.get(i) <= d);
                }
            }

            // next steps upwards (if still possible)
            if (cs.getDirection() == +1) {
                for (int i : catParents[cs.getCatIndex()]) {
                    double d = cs.getDistance() + catPageRanks[cs.getCatIndex()];
                    if (!closedCats.contains(i) && (!catDistances.containsKey(i) || catDistances.get(i) > d)) {
                        catDistances.put(i, d);
                        openCats.add(new CategoryDistance(i, d, (byte)-1));
                    }
                }
            }

        }

        return pageDistances;
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
        catPageRanks = new double[categoryIndexes.size()];
        if (debuggingMode) {
            catNames = new String[categoryIndexes.size()];
        }
        
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
                if (debuggingMode) {
                    catNames[index] = record.getPageName();
                }
            } else {    // regular page
                for (int ci : record.getCategoryIndexes()) {
                    catPages[ci][catPageIndexes[ci]++] = record.getPageId();
                }
            }
        }
        closeFile();
    }

    private static final double DAMPING_FACTOR = 0.85;

    public void computePageRanks() {
        LOG.info("computing category page ranks...");
        Arrays.fill(catPageRanks, 1.0 / catPageRanks.length);
        for (int i = 0; i < 20; i++) {
            LOG.log(Level.INFO, "performing page ranks iteration {0}.", i);
            double error = onePageRankIteration();
            LOG.log(Level.INFO, "Error for iteration is {0}.", error);
        }
        for (int i = 0; i < catParents.length; i++) {
            catPageRanks[i] = 1.0/-Math.log(catPageRanks[i]);
        }
        LOG.info("finished computing page ranks...");
    }

    protected double onePageRankIteration() {
        double nextRanks [] = new double[catPageRanks.length];
        Arrays.fill(nextRanks, (1.0 - DAMPING_FACTOR) / catPageRanks.length);
        for (int i = 0; i < catParents.length; i++) {
            int d = catParents[i].length;   // degree
            double pr = catPageRanks[i];    // current page-rank
            for (int j : catParents[i]) {
                nextRanks[j] += DAMPING_FACTOR * pr / d;
            }
        }
        double diff = 0.0;
        for (int i = 0; i < catParents.length; i++) {
            diff += Math.abs(catPageRanks[i] - nextRanks[i]);
        }
        catPageRanks = nextRanks;
        return diff;
    }

    protected void dumpPageRanks() {
        DecimalFormat df = new DecimalFormat("0.#########");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/page_ranks.txt"));
            for (int i = 0; i < catPageRanks.length; i++) {
                writer.write("" + df.format(catPageRanks[i]) + "\t" + catNames[i] + "\n");
            }
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(CategoryComparer.class.getName()).log(Level.SEVERE, null, ex);
        }
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
    public abstract void writeResults(CategoryRecord record, LinkedHashMap<Integer, Double> distances) throws IOException;
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
