/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.citations;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author Nathaniel Miller
 * 
 * TODO: make this read from standard in, write to file.
 */
public class CitationMetrics {
    
    public static final int LIMIT = 100;
    static ArrayList<Ranked> added = new ArrayList<Ranked>();
    static ArrayList<Ranked> removed = new ArrayList<Ranked>();
    static ArrayList<Ranked> present = new ArrayList<Ranked>();
    static ArrayList<Ranked> revisions = new ArrayList<Ranked>();
    static ArrayList<RankedDouble> resilient = new ArrayList<RankedDouble>();//most revisions/added
    static ArrayList<RankedDouble> unResilient = new ArrayList<RankedDouble>();//least revisions/added
    static ArrayList<RankedDouble> resRA = new ArrayList<RankedDouble>(); //least removed/added
    static ArrayList<RankedDouble> unResRA = new ArrayList<RankedDouble>(); //most removed/added
    static ArrayList<RankedDouble> mostKept = new ArrayList<RankedDouble>(); //most present articles/total articles
    static ArrayList<RankedDouble> leastKept = new ArrayList<RankedDouble>(); //least present articles/total articles
    static int totDomains = 0;
    static int totAdded = 0;
    static int totRemoved = 0;
    static int totRevisions = 0;
    static int totArticles = 0;
    public static void main(String args[]) throws FileNotFoundException, IOException {
        int lines = 0;
        BufferedReader r = new BufferedReader(new FileReader("/Users/research/Documents/citeDomainTwo/output.txt"));
        
        while (r.ready()) {
            String[] lineSplit = r.readLine().split("\t");
            lines++;
            String name = lineSplit[0];
            int a = Integer.parseInt(lineSplit[1]);
            int rem = Integer.parseInt(lineSplit[2]);
            int rev = Integer.parseInt(lineSplit[3]);
            int art = Integer.parseInt(lineSplit[4]);
            if (!name.equals("citation needed")) {
                totAdded = totAdded + a;
                totRemoved = totRemoved + rem;
                totRevisions = totRevisions + rev;
                totArticles = totArticles + art;
            }
            if (!name.equals("nourl") && !name.equals("citation needed") && (name.length() < 5 || !"wiki:".equals(name.substring(0,5)))) {
                totDomains++;
            }
            addTo(added, name, a);
            addTo(removed, name, rem);
            addTo(present, name, a - rem);
            addTo(revisions, name, rev);
            if (a > 1000) {
                checkResilience(name, (1.0*rev)/a);
                checkResRA(name, (1.0*rem)/a);
                checkPercentKept(name, (1.0*(a - rem))/art);
            }
            /*if (lines % 1000 == 0) {
                System.err.println("Now completed " + lines + " domains.");
                if (lines % 10000 == 0) {
                    System.err.println("Added length: " + added.size());
                    System.err.println("Removed length: " + removed.size());
                    System.err.println("Present length: " + present.size());
                    System.err.println("Revisions length: " + revisions.size());
                }
            }*/
        }
        print();
    }

    
    public static void print() {
        //System.err.println("!!OUTPUT STARTING!!");
        System.out.println("TOTAL DOMAINS: " + totDomains);
        System.out.println("TOTAL ADDED: " + totAdded);
        System.out.println("TOTAL REMOVED: " + totRemoved);
        System.out.println("TOTAL REVISIONS: " + totRevisions);
        System.out.println("AVERAGE RESILIENCE: " + (1.0*totRevisions)/totAdded);
        System.out.println("Most Added");
        printList(added);
        System.out.println("Most Removed");
        printList(removed);
        System.out.println("Most Articles Present");
        printList(present);
        System.out.println("Most Total Revisions");
        printList(revisions);
        System.out.println("Most Resilient");
        printListDouble(resilient);
        System.out.println("Least Removed per Addition");
        printListDouble(resRA);
        System.out.println("Most Present Articles per Unique URL-Article Added");
        printListDouble(mostKept);
        System.out.println("Least Resilient");
        printListDouble(unResilient);
        System.out.println("Most Removed per Addition");
        printListDouble(unResRA);
        System.out.println("Least Present Articles per Unique URL-Article Added");
        printListDouble(leastKept);
    }
    
    static void printList(ArrayList<Ranked> a) {
        for (int i = 0; i < a.size(); i++) {
            System.out.println((i+1) + ". " + a.get(i).getName() + '\t' + a.get(i).getAmount());
        }
    }
    
    static void printListDouble(ArrayList<RankedDouble> a) {
        for (int i = 0; i < a.size(); i++) {
            System.out.println((i+1) + ". " + a.get(i).getName() + '\t' + a.get(i).getAmount());
        }
    }
    
    static void checkPercentKept(String name, double kept) {
        int i = 0;
        while (i < mostKept.size() && kept < mostKept.get(i).getAmount()) {
            i++;
        }
        if (i < mostKept.size()) {
            mostKept.add(i, new RankedDouble(name, kept));
            if (mostKept.size() > LIMIT) {
                mostKept.remove(mostKept.size() - 1);
            }
        } else if (mostKept.size() < LIMIT) {
            mostKept.add(new RankedDouble(name, kept));
        }
        int j = 0;
        while (j < leastKept.size() && kept > leastKept.get(j).getAmount()) {
            j++;
        }
        if (j < leastKept.size()) {
            leastKept.add(j, new RankedDouble(name, kept));
            if (leastKept.size() > LIMIT) {
                leastKept.remove(leastKept.size() - 1);
            }
        } else if (leastKept.size() < LIMIT) {
            leastKept.add(new RankedDouble(name, kept));
        }
    }
    
    static void checkResilience(String name, double resilience) {
        int i = 0;
        while (i < resilient.size() && resilience < resilient.get(i).getAmount()) {
            i++;
        }
        if (i < resilient.size()) {
            resilient.add(i, new RankedDouble(name, resilience));
            if (resilient.size() > LIMIT) {
                resilient.remove(resilient.size() - 1);
            }
        } else if (resilient.size() < LIMIT) {
            resilient.add(new RankedDouble(name, resilience));
        }
        int j = 0;
        while (j < unResilient.size() && resilience > unResilient.get(j).getAmount()) {
            j++;
        }
        if (j < unResilient.size()) {
            unResilient.add(j, new RankedDouble(name, resilience));
            if (unResilient.size() > LIMIT) {
                unResilient.remove(unResilient.size() - 1);
            }
        } else if (unResilient.size() < LIMIT) {
            unResilient.add(new RankedDouble(name, resilience));
        }
    }
    
    static void checkResRA(String name, double resilience) {
        int i = 0;
        while (i < unResRA.size() && resilience < unResRA.get(i).getAmount()) {
            i++;
        }
        if (i < unResRA.size()) {
            unResRA.add(i, new RankedDouble(name, resilience));
            if (unResRA.size() > LIMIT) {
                unResRA.remove(unResRA.size() - 1);
            }
        } else if (unResRA.size() < LIMIT) {
            unResRA.add(new RankedDouble(name, resilience));
        }
        int j = 0;
        while (j < resRA.size() && resilience > resRA.get(j).getAmount()) {
            j++;
        }
        if (j < resRA.size()) {
            resRA.add(j, new RankedDouble(name, resilience));
            if (resRA.size() > LIMIT) {
                resRA.remove(resRA.size() - 1);
            }
        } else if (resRA.size() < LIMIT) {
            resRA.add(new RankedDouble(name, resilience));
        }
    }
    
    
    static ArrayList<Ranked> addTo(ArrayList<Ranked> a, String name, int num) {
        int i = 0;
        while (i < a.size() && num < a.get(i).getAmount()) {
            i++;
        }
        if (i < a.size()) {
            a.add(i, new Ranked(name, num));
            if (a.size() > LIMIT) {
                a.remove(a.size() - 1);
            }
        } else if (a.size() < LIMIT) {
            a.add(new Ranked(name, num));
        }
        return a;
    }
    
    private static class Ranked {
        private String name;
        private int amount;
        
        public Ranked(String name, int amount) {
            this.name = name;
            this.amount = amount;
        }
        
        public int getAmount() {
            return amount;
        }
        
        public String getName() {
            return name;
        }
    }   

    private static class RankedDouble {
        private String name;
        private double amount;
        
        public RankedDouble(String name, double amount) {
            this.name = name;
            this.amount = amount;
        }
        
        public double getAmount() {
            return amount;
        }
        
        public String getName() {
            return name;
        }
    }
    
}

