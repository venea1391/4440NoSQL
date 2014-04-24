import net.spy.memcached.MemcachedClient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;

import java.util.Random;

public class Memcached {

	public static MemcachedClient c;
	public static Random randomGenerator;
	public static ArrayList<String> randomKeys;
	public static Map<String, String> defaultValue;
	public static final double PERCENTAGE = .1;
	static File file;
	static BufferedWriter bw;
	
	public static void main(String[] args) {
		try {
			file = new File("memcached-results.txt");
			if (!file.exists()) {
				file.createNewFile();
			}
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			
			c = new MemcachedClient(
				    new InetSocketAddress("localhost", 11211));
			c.flush();
			randomGenerator = new Random();
			randomKeys = new ArrayList<String>();
			defaultValue = new HashMap<String, String>();
			defaultValue.put("age", "null");
			defaultValue.put("job", "null");
			defaultValue.put("marital", "null");
			defaultValue.put("education", "null");
			defaultValue.put("default", "null");
			defaultValue.put("balance", "null");
			defaultValue.put("housing", "null");
			defaultValue.put("loan", "null");
			defaultValue.put("contact", "null");
			defaultValue.put("day", "null");
			defaultValue.put("month", "null");
			defaultValue.put("duration", "null");
			defaultValue.put("campaign", "null");
			defaultValue.put("pdays", "null");
			defaultValue.put("previous", "null");
			defaultValue.put("poutcome", "null");
			defaultValue.put("y", "null");
			
			/*Object myObject=c.get("item0");
			System.out.println(myObject);
			myObject=c.get("item439");
			System.out.println(myObject);*/
		    runSmallDataset();
			runMediumDataset();
			runLargeDataset();
			c.shutdown();
			
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
public static void runSmallDataset(){
		
		c.flush();
		randomKeys.clear();
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB("bank-small.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load small: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("item"+randomInt);
		    }
		    run100Read();
		    run100Write();
		    run5050();
		    run9010();
		    run1090();
		    
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	public static void runMediumDataset(){
		c.flush();
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB("bank-medium.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load medium: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("item"+randomInt);
		    }
		    run100Read();
		    run100Write();
		    run5050();
		    run9010();
		    run1090();
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	public static void runLargeDataset(){
		c.flush();
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB("bank-large.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load large: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("item"+randomInt);
		    }
		    run100Read();
		    run100Write();
		    run5050();
		    run9010();
		    run1090();
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}

	static public int loadDB(String file) throws IOException{
		CSVReader reader = new CSVReader(new FileReader(file));
		Map<String, String> properties;
		String [] nextLine;
		String [] items;
		String key = "";
		//String value = "";
		int n = 0;

		nextLine = reader.readNext(); //skip header
		while ((nextLine = reader.readNext()) != null) {
			// nextLine[] is an array of values from the line
			items = nextLine[0].split(";");
			key = "item"+n;

			/*value = "age " + items[0] + " job " + items[1] + " marital " + items[2] + 
			    " education " + items[3] + " default " + items[4] + " balance " + items[5] + 
			    " housing " + items[6] + " loan " + items[7] + " contact " + items[8] +
			     " day " + items[9] + " month " + items[10] + " duration " + items[11] +
			     " campaign " + items[12] + " pdays " + items[13] + " previous " + items[14] +
			      " poutcome " + items[15] + " y " + items[16];*/
			properties = new HashMap<String, String>();
			properties.put("age", items[0]);
			properties.put("job", items[1]);
			properties.put("marital", items[2]);
			properties.put("education", items[3]);
			properties.put("default", items[4]);
			properties.put("balance", items[5]);
			properties.put("housing", items[6]);
			properties.put("loan", items[7]);
			properties.put("contact", items[8]);
			properties.put("day", items[9]);
			properties.put("month", items[10]);
			properties.put("duration", items[11]);
			properties.put("campaign", items[12]);
			properties.put("pdays", items[13]);
			properties.put("previous", items[14]);
			properties.put("poutcome", items[15]);
			properties.put("y", items[16]);
			n++;
		
			c.set(key, 0, properties);
			//value = "";
		}

		System.out.println(n);
		reader.close();
		return n;
	}
	
	public static void run100Read(){
		
		int size = randomKeys.size();
		long pre_100read = System.currentTimeMillis();
		for (int i=0; i<size; i++){
			c.get((String) randomKeys.get(i));
		}
		long after_100read = System.currentTimeMillis();
		try {
			bw.write("time to run 100% read workload: "+(after_100read-pre_100read)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void run100Write(){
		
		int size = randomKeys.size();
		int firstHalf = size/2;
		//int secondHalf = size-firstHalf;
		
		long pre_100write = System.currentTimeMillis();
		
		for (int i=0; i<firstHalf; i++){
			c.replace((String) randomKeys.get(i), 0, defaultValue);
		}
		for (int i=firstHalf; i<size; i++){
			c.delete((String) randomKeys.get(i));
			c.set((String) randomKeys.get(i), 0, defaultValue);
		}
		
		long after_100write = System.currentTimeMillis();
		try {
			bw.write("time to run 100% write workload: "+(after_100write-pre_100write)+" ms\n");
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	public static void run5050(){
		int size = randomKeys.size();
		int firstHalf = size/2;
		//int secondHalf = size-firstHalf;
		
		long pre_5050 = System.currentTimeMillis();
		
		for (int i=0; i<size; i++){
			c.get((String) randomKeys.get(i));
		}
		for (int i=0; i<firstHalf; i++){
			c.replace((String) randomKeys.get(i), 0, defaultValue);
		}
		for (int i=firstHalf; i<size; i++){
			c.delete((String) randomKeys.get(i));
			c.set((String) randomKeys.get(i), 0, defaultValue);
		}
		
		long after_5050 = System.currentTimeMillis();
		try {
			bw.write("time to run 50% read, 50% write workload: "+(after_5050-pre_5050)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void run9010(){
		int size = randomKeys.size();
		int tenPercent = (int) (size*.1); //write
		int ninetyPercent = (int) (size*.9); //read
		int halfOfTen = tenPercent/2;
		
		
		long pre_9010 = System.currentTimeMillis();
		
		//90% read
		for (int i=0; i<ninetyPercent; i++){
			c.get((String) randomKeys.get(i));
		}
		
		//10% write
		for (int i=0; i<halfOfTen; i++){
			c.replace((String) randomKeys.get(i), 0, defaultValue);
		}
		for (int i=halfOfTen; i<tenPercent; i++){
			c.delete((String) randomKeys.get(i));
			c.set((String) randomKeys.get(i), 0, defaultValue);
		}
		
		long after_9010 = System.currentTimeMillis();
		try {
			bw.write("time to run 90% read, 10% write workload: "+(after_9010-pre_9010)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void run1090(){
		int size = randomKeys.size();
		int tenPercent = (int) (size*.1); //read
		int ninetyPercent = (int) (size*.9); //write
		int halfOfNinety = ninetyPercent/2;
		
		
		long pre_1090 = System.currentTimeMillis();
		
		//10% read
		for (int i=0; i<tenPercent; i++){
			c.get((String) randomKeys.get(i));
		}
		
		//90% write
		for (int i=0; i<halfOfNinety; i++){
			c.replace((String) randomKeys.get(i), 0, defaultValue);
		}
		for (int i=halfOfNinety; i<ninetyPercent; i++){
			c.delete((String) randomKeys.get(i));
			c.set((String) randomKeys.get(i), 0, defaultValue);
		}
		
		long after_1090 = System.currentTimeMillis();
		try {
			bw.write("time to run 10% read, 90% write workload: "+(after_1090-pre_1090)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
