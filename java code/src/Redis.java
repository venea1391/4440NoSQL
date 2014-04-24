import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import redis.clients.jedis.Jedis;
import au.com.bytecode.opencsv.CSVReader;

public class Redis {
	public static Jedis jedis;
	static File file;
	static BufferedWriter bw;
	public static Random randomGenerator;
	public static ArrayList<String> randomKeys;
	public static HashMap<String, String> defaultValue;
	public static final double PERCENTAGE = .1;
	
	public static void main(String[] args) {
		try {
			file = new File("redis-results.txt");
			if (!file.exists()) {
				file.createNewFile();
			}
		
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
		
			jedis = new Jedis("localhost");
			jedis.connect();
			System.out.println(jedis.randomKey());
			jedis.flushDB();
			System.out.println(jedis.randomKey());
			jedis.select(1);
			System.out.println(jedis.randomKey());
			
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
			
			runSmallDataset();
			runMediumDataset();
			runLargeDataset();
			
			bw.close();
		} catch (IOException e){
			e.printStackTrace();
		}
		
	    
	}
	
public static void runSmallDataset(){
		
		jedis.select(1);
		jedis.flushDB();
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB("bank-small.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load small: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("items:row"+randomInt);
		    }
		    run100Read();
		    run100Write();
		    run5050();
		    run9010();
		    run1090();
		    
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	public static void runMediumDataset(){
		
		jedis.select(2);
		jedis.flushDB();
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB("bank-medium.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load medium: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("items:row"+randomInt);
		    }
		    run100Read();
		    run100Write();
		    run5050();
		    run9010();
		    run1090();
		    
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	public static void runLargeDataset(){
		
		jedis.select(3);
		jedis.flushDB();
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB("bank-large.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load large: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("items:row"+randomInt);
		    }
		    run100Read();
		    run100Write();
		    run5050();
		    run9010();
		    run1090();
		    
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	static public int loadDB(String file) throws IOException{
			CSVReader reader = new CSVReader(new FileReader(file));
			String [] nextLine;
			String[] items;
			int n = 0;

				while ((nextLine = reader.readNext()) != null) {
				    // nextLine[] is an array of values from the line
					items = nextLine[0].split(";");
					Map<String, String> properties = new HashMap<String, String>();
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
					
				    jedis.hmset("items:row"+n, properties);
				    n++;
				}
				//System.out.println(n);
				//System.out.println(jedis.hgetAll("items:row444"));
				//System.out.println(jedis.randomKey());
				
				reader.close();
				return n;
				
	}
	
	public static void run100Read(){
		
		int size = randomKeys.size();
		long pre_100read = System.currentTimeMillis();
		for (int i=0; i<size; i++){
			jedis.hgetAll((String) randomKeys.get(i));
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
		
		long pre_100write = System.currentTimeMillis();
		
		for (int i=0; i<size; i++){
			jedis.del((String) randomKeys.get(i));
			jedis.hmset((String) randomKeys.get(i), defaultValue);
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
			jedis.hgetAll((String) randomKeys.get(i));
		}
		for (int i=0; i<firstHalf; i++){
			jedis.del((String) randomKeys.get(i));
			jedis.hmset((String) randomKeys.get(i), defaultValue);
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
			jedis.hgetAll((String) randomKeys.get(i));
		}
		
		//10% write
		for (int i=0; i<halfOfTen; i++){
			jedis.del((String) randomKeys.get(i));
			jedis.hmset((String) randomKeys.get(i), defaultValue);
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
			jedis.hgetAll((String) randomKeys.get(i));
		}
		
		//90% write
		for (int i=0; i<halfOfNinety; i++){
			jedis.del((String) randomKeys.get(i));
			jedis.hmset((String) randomKeys.get(i), defaultValue);
		}
		
		long after_1090 = System.currentTimeMillis();
		try {
			bw.write("time to run 10% read, 90% write workload: "+(after_1090-pre_1090)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
