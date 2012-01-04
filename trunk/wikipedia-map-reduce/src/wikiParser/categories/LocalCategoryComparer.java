package wikiParser.categories;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *
 * @author shilad
 */
public class LocalCategoryComparer extends CategoryComparer {
    private final File inputPath;
    private BufferedReader reader;
    private final BufferedWriter writer;
    private final int numThreads;
    private final ThreadPoolExecutor pool;
    private final Semaphore semaphore;
    private final ThreadLocal<StringBuilder> threadLocal = new ThreadLocal<StringBuilder>();

    public LocalCategoryComparer(File path, BufferedWriter writer, int numThreads) {
        this.inputPath = path;
        this.writer = writer;
        this.numThreads = numThreads;
        this.pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads - 1);   // save one for output
        this.semaphore = new Semaphore(numThreads);
    }

    @Override
    public void searchPages() throws IOException {
        super.searchPages();
        this.flushBuilderBuffer();
    }

    @Override
    public void findSimilar(final CategoryRecord record) {
        try {
            semaphore.acquire();
        } catch (InterruptedException ex) {
            System.err.println("findSimilar in LocalCategoryComparer interrupted");
            return;
        }
        this.pool.submit(new Runnable() {
            public void run() {
                try {
                    LocalCategoryComparer.super.findSimilar(record);
                } finally {
                    semaphore.release();
                }
            }
        });
    }
    
    public void openFile() throws IOException {
        if (reader != null) {
            reader.close();
        }
        reader = new BufferedReader(new FileReader(inputPath));
    }

    public String readLine() throws IOException {
        return reader.readLine();
    }

    public void writeResult(int targetPageId, int similarPageId, int distance) throws IOException {
        StringBuilder builder = threadLocal.get();
        if (builder == null) {
            builder = new StringBuilder();
            threadLocal.set(builder);
        }
        String prefix = "" + targetPageId + "\t";
        if (builder.length() > prefix.length() && builder.substring(0, prefix.length()).equals(prefix)) {
            this.flushBuilderBuffer();
            builder.append(prefix);
        }
        builder.append(similarPageId).append("=").append(distance).append("|");
    }

    private void flushBuilderBuffer() throws IOException {
        StringBuilder builder = threadLocal.get();
        builder.append("\n");
        this.writeLine(builder.toString());
        builder.setLength(0);
    }

    public synchronized void writeLine(String line) throws IOException {
        this.writer.write(line);
    }

    public void closeFile() throws IOException {
        if (reader != null) {
            reader.close();
        }
        reader = null;
    }

    public static void main(String args[]) throws IOException {
        String input = (args.length > 0)
                ? args[0]
                : "/Users/shilad/Documents/NetBeans/wikipedia-map-reduce/dat/all_cats.txt";
        String output = (args.length > 1)
                ? args[1]
                : "/Users/shilad/Documents/NetBeans/wikipedia-map-reduce/dat/test/page_sims.txt";
        output = "/dev/null";
        BufferedWriter writer = new BufferedWriter(
                output.equals("stdout") 
                        ? new OutputStreamWriter(System.out)
                        : (new FileWriter(output)));
        LocalCategoryComparer lcc = new LocalCategoryComparer(new File(input), writer, 5);
        lcc.prepareDataStructures();
        lcc.searchPages();
    }
}
