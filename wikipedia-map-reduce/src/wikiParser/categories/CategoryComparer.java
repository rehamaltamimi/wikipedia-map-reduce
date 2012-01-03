/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wikiParser.categories;

import gnu.trove.TIntArrayList;
import gnu.trove.TLongIntHashMap;
import java.io.IOException;

/**
 * @author shilad
 */
public abstract class CategoryComparer {
    private TLongIntHashMap categoryIndexes = new TLongIntHashMap();

    private int catPages[][];
    private int catParents[][];
    private int catChildren[][];

    public void prepareDataStructures() throws IOException {
        setCategoryIndexes();
        allocateArrays();
        fillArrays();
    }

    public void setCategoryIndexes() throws IOException {
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
    }

    public void allocateArrays() throws IOException {
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
        int catPageIndexes[] = new int[categoryIndexes.size()];
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
    
    public abstract void openFile() throws IOException;
    public abstract String readLine() throws IOException;
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
            record.setCategoryIndexes(indexes.toNativeArray());
        }
        return record;
    }

}
