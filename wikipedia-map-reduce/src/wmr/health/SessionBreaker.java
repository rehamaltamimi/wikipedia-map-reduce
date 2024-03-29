package wmr.health;

import java.io.*;
import java.text.Collator;
import java.util.Locale;

import org.joda.time.Duration;
import org.joda.time.DateTime;

/**
 * 
 * Takes a list of tab-delimited username, timestamp doubles and determines which 
 * edits constitute a session, and creates an output file of tab-delimited 
 * username, beginning session timestamp, ending session timestamp, session 
 * duration (in seconds) 4-tuples, each corresponding to the session of a user.
 * 
 * The input list must be sorted by username and timestamp. If ordered correctly, 
 * the output file will be ordered the same way.
 * 
 * Currently the criteria for a session is an hour between edits, although this can be 
 * easily parameterized with a few minor changes. Sessions consisting of one edit 
 * are defined as having zero length. An offset is provided as an argument to give 
 * these edits a nonzero value (the offset in seconds is added to the duration of every 
 * listed session).
 * 
 * Usage:
 * <Run SessionBreaker.java as main class> <input file path> <output file path> <session offset>
 * 
 * @author Guanyu Wang
 * @author Andy Bristol
 *
 */
public class SessionBreaker {
	
	public static String user = null;
	public static DateTime firsttime = null;
	public static DateTime lasttime = null;
	public static Duration interSessionDuration = null;
	public static Duration sessionDuration = null;
	public static String currentuser = null;
	public static DateTime currenttime = null;
	public static long offset = 0;
	
	public static FileInputStream inputStream = null;
	public static InputStreamReader streamReader = null;
	public static BufferedReader bufferedReader = null;
	
	public static FileOutputStream outputStream = null;
	public static OutputStreamWriter streamWriter = null;
	public static BufferedWriter bufferedWriter = null;
	
	public static final String UTF_8 = "UTF-8";
	
	public static void breakSessions (File inputFile, File outputFile){
		
		
		try {
			
			inputStream = new FileInputStream(inputFile);
			streamReader = new InputStreamReader(inputStream, UTF_8);
			bufferedReader = new BufferedReader(streamReader);
			
			outputStream = new FileOutputStream(outputFile);
			streamWriter = new OutputStreamWriter(outputStream, UTF_8);
			bufferedWriter = new BufferedWriter(streamWriter);
			
	        String line = bufferedReader.readLine();
	       
	        while (line != null) {	        		        		
	        	String[] value = line.split("\t");
	        	
	        	if(value.length < 2)
	        		System.err.println("Expecting two values: username, timestamp!");
	        	
	        	currentuser = value[0];
	        	currenttime = DateTime.parse(value[1]);
	        	
	        	if (firsttime == null || lasttime == null || user == null ) {
	        		firsttime = currenttime;
	        		lasttime = currenttime;
	        		user = currentuser;
	        	}
	        	
	        	if (user.equals(currentuser) && lasttime.compareTo(currenttime) > 0)
	        		System.err.println("Input file not sorted by timestamp: "+user+", "+currentuser);
	        	
	        	/* This does not work, even though by the Collator/Locale doc it should.
	        	 
	        	Collator collator = Collator.getInstance(Locale.ROOT);
	        	if (collator.compare(user, currentuser) > 0)
	        		System.err.println("Input file not sorted by username: "+user+", "+currentuser);
	        	*/
	        	
	        	interSessionDuration = new Duration(lasttime, currenttime);
	        	if (!currentuser.equals(user) || interSessionDuration.getStandardHours() >= 1)
	        		newSession();
	        	
	        	lasttime = currenttime;
	        	
	        	line = bufferedReader.readLine();
	        }
	        
	        bufferedReader.close();
	        bufferedWriter.close();
			
		} catch (FileNotFoundException e) {
			System.err.println("File not found!");
			e.printStackTrace();
		}
		catch (IOException e) {
			System.err.println("There was an IO error reading or writing.");
			e.printStackTrace();
		}
		
	}
	
	public static void newSession() throws IOException {
		//System.out.println(user+"\t"+firsttime.toString()+"\t"+lasttime.toString());
		sessionDuration = new Duration(firsttime, lasttime);
		long seconds = sessionDuration.getStandardSeconds()+offset;
		bufferedWriter.write(user + "\t" + firsttime.toString() + "\t" + 
				lasttime.toString() + "\t" + seconds +"\n");
		firsttime = currenttime;
		lasttime = currenttime;
		user = currentuser;
	}
	
	public static void main (String[] args){
		if (args.length < 2) {
			System.out.println("Required arguments: input file path, output file path");
			System.exit(1);
		}
	
		
		if (args.length >= 3)
			offset = Long.valueOf(args[2]);
		
		File inputFile = new File(args[0]);
		File outputFile = new File(args[1]);
		
		if (!outputFile.exists())
			try {
				outputFile.createNewFile();
			} catch (IOException e) {
				System.err.println("IO error occurred while creating output file.");
				e.printStackTrace();
				System.exit(1);
			}
		
		breakSessions(inputFile, outputFile);
			
	}

}
