package wikiParser.categories;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 *
 * @author shilad
 */
public class LocalCategoryComparer extends CategoryComparer {
    File path;
    BufferedReader reader;

    public LocalCategoryComparer(File path) {
        this.path = path;
    }

    public void openFile() throws IOException {
        if (reader != null) {
            reader.close();
        }
        reader = new BufferedReader(new FileReader(path));
    }

    public String readLine() throws IOException {
        return reader.readLine();
    }

    public void closeFile() throws IOException {
        if (reader != null) {
            reader.close();
        }
        reader = null;
    }

    public static void main(String args[]) throws IOException {
        LocalCategoryComparer lcc = new LocalCategoryComparer(new File(args[0]));
        lcc.prepareDataStructures();
    }
}
