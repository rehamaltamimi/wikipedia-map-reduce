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
             if (submitted > QUEUE_LENGTH/2 && writer == null) {
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
         if (writer == null) {
             writer = new Writer(results, args[0]);
             tpe.execute(writer);
         }
         for (long cluster : clusterChanges.keySet()) {
             waitUntilQueued(new Characterizer(cluster,"",clusterChanges.get(cluster),results));
             submitted++;
         }         
         if (writer == null) {
             writer = new Writer(results, args[0]);
             tpe.execute(writer);
         }
         writer.setTotalCases(submitted);
         tpe.shutdown();
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
            HashMap<Integer,HashSet<Long>> componentMapping = new HashMap<Integer,HashSet<Long>>();
            HashMap<Long,Integer> userChanges = new HashMap<Long,Integer>();
            int side = 0;
            int users = 0;
            int totalChanged = 0;
            if (components.length() > 0) {
                for (String conflict : components.split("\\|")) {
                    componentMapping.put(side, new HashSet<Long>());
                    componentMapping.put(side + 1, new HashSet<Long>());
                    for (String component : conflict.split(" ")) {
                        for (String user : component.split(",")) {
                            Long u = userIDtoLong(user);
                            if (u == null) {
                                continue;
                            }
                            componentMapping.get(side).add(u);
                            userChanges.put(u,0);
                        }
                        side++;
                    }
                }
            }
            int usersUpdated = 0;
            for (String userInfo : changes.split("\\|")) {
                String[] userDelta = userInfo.split("#");
                Long user = userIDtoLong(userDelta[0]);
                if (userDelta.length > 1) {
                    int delta = Integer.parseInt(userDelta[1]);
                    users++;
                    totalChanged += delta;
                    //System.out.println("UId: " + user + ", UName: " + userDelta[0]);
                    if (user != null && userChanges.containsKey(user)) {
                        //System.out.println("User updated!");
                        usersUpdated++;
                        userChanges.put(user,delta);
                    }
                }
            }
            if (usersUpdated < userChanges.size()) {
                System.out.println("C#" + cluster + " " + 
                        (userChanges.size() - usersUpdated));
            }
            StringBuilder sb = new StringBuilder();
            int average;
            if (users > 0) {
                average = totalChanged/users;
            } else {
                average = 0;
            }
            //add cluster average edit size
            sb.append(average).append("|");
            //for each side, compute average without decimal
            //add to output string
            for (int component = 0; component < componentMapping.keySet().size(); component++) {
                int componentAverage;
                if (componentMapping.containsKey(component) && componentMapping.get(component).size() > 0) {
                    int total = 0;
                    for (long user : componentMapping.get(component)) {
                        total += userChanges.get(user);
                    }
                    componentAverage = total/componentMapping.get(component).size();
                } else {
                    componentAverage = 0;
                }
                sb.append(componentAverage);
                if (component % 2 == 0) {
                    sb.append(" ");
                } else {
                    sb.append("|");
                }
            }
            boolean added = false;
            while (!added) {
                added = results.add(cluster + "\t" + sb.toString());
            }
        }
        
        /**
         * 
         * @param uid
         * @return user ID as a long, or null for invalid ID
         */
        private static Long userIDtoLong(String uid) {
            try {
                if (uid.length() == 0) {
                    //Empty user ID
                    return null;
                } else if (uid.contains(":")) {
                    //IPv6 address
                    return null;
                } else if (uid.contains(".")) {
                    return -ipv4ToLong(uid);
                } else {
                    return Long.parseLong(uid);
                }
            } catch  (Exception e) {
                //Non-numeric, IPv4, or IPv6 user ID
                System.out.println("U#" + uid);
                return null;
            }
        }
        
        private static long ipv4ToLong(String ipv4) {
            String[] ipAddress = ipv4.split("\\.");
            return 16777216*Long.parseLong(ipAddress[0]) + 65536*Long.parseLong(ipAddress[1])
                + 256*Long.parseLong(ipAddress[2]) + Long.parseLong(ipAddress[3]);
        }
    }
      
}
