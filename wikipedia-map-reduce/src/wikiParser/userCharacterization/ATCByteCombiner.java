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

/**
 *
 * @author Nathaniel Miller
 * Takes output from DeltaByteCounter and makes it ready for ComponentCharacterizer
 * Input:
 *  Stream output from DeltaByteCounter
 *  Command line arguments should be an article clusters file with Wikipedia article IDs
 *      and an output directory
 */
public class ATCByteCombiner {
    
    public static void main(String args[]) throws FileNotFoundException, IOException {
        if (args.length < 4) {
            System.out.println("Usage: [clusters outputDirectory]");
            System.exit(0);
        }
        
        HashMap<Long,Long> aidCluster = new HashMap<Long,Long>(); //Maps from article ID to cluster #
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[0]))));
        String line = reader.readLine();
        System.out.println("Reading article clusters file...");
        long cluster = 0;
        while (line != null) {
            for (String id : line.split("\\|")) {
                aidCluster.put(Long.parseLong(id),cluster);
            }
            cluster++;
            line = reader.readLine();
        }
        System.out.println("Read in " + cluster + " clusters.");
        
        HashMap<Long,HashMap<String,Integer>> clusterDBytes = new HashMap<Long,HashMap<String,Integer>>();
        reader = new BufferedReader(new InputStreamReader(System.in));
        line = reader.readLine();
        String[] info;
        System.out.println("Combining delta bytes information across clusters...");
        while (line != null) {
            info = line.split("\t");
            long article = Long.parseLong(info[0]);
            if (info.length > 1 && aidCluster.containsKey(article)) {
                cluster = aidCluster.get(article);
                if (!clusterDBytes.containsKey(cluster)) {
                    clusterDBytes.put(cluster, new HashMap<String,Integer>());
                }
                for (String user : info[1].split("\\|")) {
                    String[] userDelta = user.split(":");
                    String userName = userDelta[0];
                    for (int i = 1; i < userDelta.length - 1; i++) {//IPv6 addresses are separated by colons
                        userName += ":" + userDelta[i];
                    }
                    if (!clusterDBytes.get(cluster).containsKey(userName)) {
                        clusterDBytes.get(cluster).put(userName,Integer.parseInt(userDelta[userDelta.length - 1]));
                    } else {
                        clusterDBytes.get(cluster).put(userName,clusterDBytes.get(cluster).get(userName)
                                + Integer.parseInt(userDelta[userDelta.length - 1]));
                    }
                }
            }
            line = reader.readLine();
        }
        
        System.out.println("Writing output...");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(args[1]))));
        for (long c : clusterDBytes.keySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(c).append("\t");
            HashMap<String,Integer> dBytes = clusterDBytes.get(c);
            for (String user : dBytes.keySet()) {
                sb.append(user).append("#").append(dBytes.get(user)).append("|");
            }
            writer.write(sb.toString());
            writer.newLine();
        }
        writer.flush();
        writer.close();
        
    }
    
}
