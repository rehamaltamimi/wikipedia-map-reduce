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

/**
 *
 * @author Nathaniel Miller
 * Takes output from DeltaByteCounter and makes it ready for ComponentCharacterizer
 * Input:
 *  Stream output from DeltaByteCounter
 *  Command line arguments should be an article titles to article IDs mapping
 *      and an article clusters file
 */
public class ATCByteCombiner {
    
    public static void main(String args[]) throws FileNotFoundException, IOException {
        if (args.length < 3) {
            System.out.println("Usage: [articleIDs clusters outputDirectory]");
            System.exit(0);
        }
        
        HashMap<String, Long> articles = new HashMap<String, Long>();
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[0]))));
        String line = reader.readLine();
        System.out.println("Reading article to IDs file...");
        while (line != null) {
            String[] info = line.split("\t");
            if (info.length > 1) {
                articles.put(info[0].intern(), Long.parseLong(info[1]));
            }
            line = reader.readLine();
        }
        
        HashMap<Long,Long> aidCluster = new HashMap<Long,Long>(); //Maps from article ID to cluster #
        //Articles not in clusters are represented by the negative of their article ID
        
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[1]))));
        line = reader.readLine();
        System.out.println("Reading article clusters file...");
        long cluster = 0;
        while (line != null) {
            for (String title : line.split("\\|")) {
                Long i = articles.get(title);
                articles.remove(title);
                aidCluster.put(i,cluster);
            }
            cluster++;
            line = reader.readLine();
        }
        
        for(String title : articles.keySet()) { //Only articles not in clusters
            aidCluster.put(articles.get(title),-articles.get(title));
        }
        
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
                    if (!clusterDBytes.get(cluster).containsKey(userDelta[0])) {
                        clusterDBytes.get(cluster).put(userName,Integer.parseInt(userDelta[userDelta.length - 1]));
                    } else {
                        clusterDBytes.get(cluster).put(userName,clusterDBytes.get(cluster).get(userDelta[0])
                                + Integer.parseInt(userDelta[userDelta.length - 1]));
                    }
                }
            }
            line = reader.readLine();
        }
        
        System.out.println("Writing output...");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(args[2]))));
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
