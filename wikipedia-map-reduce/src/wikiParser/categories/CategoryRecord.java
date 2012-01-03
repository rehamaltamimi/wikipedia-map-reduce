/*
 * A single entry in the category file.
 */

package wikiParser.categories;

/**
 *
 * @author shilad
 */
public class CategoryRecord {
    public static final char TYPE_CATEGORY = 'c';
    public static final char TYPE_PAGE = 'p';

    private char type;
    private int pageId;
    private String pageName;
    private int [] categoryIndexes;

    public CategoryRecord(char type, int pageId, String pageName) {
        if (type != 'p' && type != 'c') {
            throw new IllegalArgumentException("invalid category code: " + type);
        }
        this.type = type;
        this.pageId = pageId;
        this.pageName = pageName;
    }

    public int[] getCategoryIndexes() {
        return categoryIndexes;
    }

    public int getPageId() {
        return pageId;
    }

    public String getPageName() {
        return pageName;
    }

    public char getType() {
        return type;
    }

    public boolean isCategory() {
        return pageName.startsWith("Category:");
    }

    void setCategoryIndexes(int[] categoryIndexes) {
        this.categoryIndexes = categoryIndexes;
    }

}
