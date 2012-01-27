/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.userCharacterization;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Nathaniel
 * 
 * Compute average edit size in bytes for entire cluster, as well
 * as each component
 */
public class ComponentCharacterizer {
     private static ThreadPoolExecutor tpe;
     private static LinkedBlockingQueue<String> results;
     private static HashMap<Long,String> clusterComponents;
     private static HashMap<Long,String> clusterChanges;
     private static final int PRINT_INTERVAL = 10000;
     private static final int QUEUE_LENGTH = 100000;
     
     public static void main(String args[]) throws FileNotFoundException, IOException {
         if (args.length < 4) {
             System.err.println("Usage: output clusterComponents coreSize clusterBytesAdded");
         }
         int coreSize = Integer.parseInt(args[2]);
         tpe = new ThreadPoolExecutor(coreSize,coreSize,1,TimeUnit.SECONDS, new ArrayBlockingQueue(QUEUE_LENGTH));
         results = new LinkedBlockingQueue<String>();
         BufferedReader componentReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[1]))));
         BufferedReader bytesChanged = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[3]))));
         String components = componentReader.readLine();
         String changes = bytesChanged.readLine();
         Writer writer = null;
         int submitted = 0;
         clusterChanges = new HashMap<Long,String>();
         clusterComponents = new HashMap<Long,String>();
         while (components != null || changes != null) {
             if (submitted == QUEUE_LENGTH/2) {
                 writer = new Writer(results, args[0]);
                 tpe.execute(writer);
             }
             if (components != null) {
                 String[] split = components.split("\t");
                 long componentsCluster = Long.parseLong(split[0]);
                 if (clusterChanges.containsKey(componentsCluster)) {
                     //both lines have been read, queue task
                     String componentList = "";
                     if (split.length > 1) {
                         componentList = split[1];
                     }
                     waitUntilQueued(new Characterizer(componentsCluster,componentList,clusterChanges.get(componentsCluster),results));
                     clusterChanges.remove(componentsCluster);
                     submitted++;
                 } else {
                     if (split.length > 1) {
                         //only one line has been read in and it has information
                         //store data for later
                         clusterComponents.put((long)componentsCluster, split[1]);
                     } //else no data, do nothing
                 }
             }
             if (changes != null) {
                 String[] split = changes.split("\t");
                 long changesCluster = Long.parseLong(split[0]);
                 if (clusterComponents.containsKey(changesCluster)) {
                     //both lines from cluster have been read in, queue task
                     waitUntilQueued(new Characterizer(changesCluster,clusterComponents.get(changesCluster),split[1],results));
                     clusterComponents.remove(changesCluster);
                     submitted++;
                 } else {
                     //only one line has been read, store data for later
                     clusterChanges.put(changesCluster, split[1]);
                 }
             }
             components = componentReader.readLine();
             changes = bytesChanged.readLine();
         }
         for (long cluster : clusterChanges.keySet()) {
             waitUntilQueued(new Characterizer(cluster,clusterChanges.get(cluster),"",results));
             submitted++;
         }
         writer.setTotalCases(submitted);
     }
     
     /**
      * Continues attempting to queue until tryExecute returns true
      * (RejectedExecutionExceptions are only thrown when
      * there is no room left in the queue, so it makes sense to
      * wait for space to open up)
      * @param c 
      */
     private static void waitUntilQueued(Characterizer c) {
         boolean added = false;
         while (!added) {
             added = tryExecute(c);
         } 
     }
     
     /**
      * 
      * @param c
      * @return true if successful, false if RejectedExecutionException thrown
      */
     private static boolean tryExecute(Characterizer c) {
         try {
             tpe.execute(c);
         } catch (RejectedExecutionException e) {
             return false;
         }
         return true;
     }
     
     private static class Writer implements Runnable {

         private int cases;
         private boolean moreTasks;
         private LinkedBlockingQueue<String> toWrite;
         private String outputPath;
         
         public Writer(LinkedBlockingQueue<String> results, String outputPath) {
             moreTasks = true;
             toWrite = results;
             this.outputPath = outputPath;
         }
         
        public void setTotalCases (int cases) {
            this.cases = cases;
            moreTasks = false;
        }

        @Override
        public void run() {
            int written = 0;
            try {
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outputPath))));
                while (moreTasks || written < cases) {
                    String output = toWrite.take();
                    writer.write(output);
                    writer.newLine();
                    written++;
                }
                writer.flush();
            } catch (Exception ex) {
                Logger.getLogger(ComponentCharacterizer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
     }
     
     private static class Characterizer implements Runnable {

         private String components;
         private String changes;
         private long cluster;
         private LinkedBlockingQueue<String> results;
         
         public Characterizer(long cluster, String components, String changes, LinkedBlockingQueue<String> results) {
             this.components = components;
             this.changes = changes;
             this.results = results;
             this.cluster = cluster;
         }
         
        @Override
        public void run() {
            /*
             * Components 0 and 1 are from the first conflict
             * 0 is first, 1 is second.
             * 2 and 3 are from the second conflict, 2 first,
             * 3 second.  And so on with higher numbers
             */
            //initialize data structures with input
            HashMap<Integer,HashSet<Integer>> componentMapping = new HashMap<Integer,HashSet<Integer>>();
            HashMap<Integer,Integer> userChanges = new HashMap<Integer,Integer>();
            int side = 0;
            int users = 0;
            int totalChanged = 0;
            for (String conflict : components.split("\\|")) {
                componentMapping.put(side, new HashSet<Integer>());
                componentMapping.put(side + 1, new HashSet<Integer>());
                for (String component : conflict.split(" ")) {
                    for (String user : component.split(",")) {
                        int u = Integer.parseInt(user);
                        componentMapping.get(side).add(u);
                        userChanges.put(u,0);
                    }
                    side++;
                }
            }
            for (String userInfo : changes.split("\\|")) {
                String[] userDelta = userInfo.split("#");
                int user;
                if (userDelta[0].contains(":")) {
                    //we may want to fix this later
                    //we'd need two longs to represent IPv6 addresses, I think...
                    continue;
                } else if (userDelta[0].contains("\\.")) {
                    user = -ipv4ToInt(userDelta[0]);
                } else {
                    user = Integer.parseInt(userDelta[0]);
                }
                int delta = Integer.parseInt(userDelta[1]);
                users++;
                totalChanged += delta;
                if (userChanges.containsKey(user)) {
                    userChanges.put(user,delta);
                }
            }
            StringBuilder sb = new StringBuilder();
            int average = totalChanged/users;
            //add cluster average edit size
            sb.append(average).append("|");
            //for each side, compute average without decimal
            //add to output string
            for (int component = 0; component < componentMapping.keySet().size()/2; component++) {
                int totalZero = 0;
                for (int user : componentMapping.get(component*2)) {
                    totalZero += userChanges.get(user);
                }
                int averageZero = totalZero/componentMapping.get(component*2).size();
                int totalOne = 0;
                for (int user : componentMapping.get(component*2 + 1)) {
                    totalOne += userChanges.get(user);
                }
                int averageOne = totalOne/componentMapping.get(component*2 + 1).size();
                sb.append(averageZero).append(" ").append(averageOne).append("|");
            }
            boolean added = false;
            while (!added) {
                added = results.add(cluster + "\t" + sb.toString());
            }
        }
        
        private static int ipv4ToInt(String ipv4) {
            String[] ipAddress = ipv4.split("\\.");
            return 16777216*Integer.parseInt(ipAddress[0]) + 65536*Integer.parseInt(ipAddress[1])
                            + 256*Integer.parseInt(ipAddress[2]) + Integer.parseInt(ipAddress[3]);
        }
         
     }
}
