import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;
import org.lightcouch.*;

import au.com.bytecode.opencsv.CSVReader;

public class Couch {
	//static CouchDbClient dbClient;
	public static Random randomGenerator;
	public static ArrayList<String> randomKeys;
	public static JSONObject defaultValue;
	public static final double PERCENTAGE = .1;
	
	static File file;
	static BufferedWriter bw;
	public static void main(String[] args) {
		/* initialize a client instance - reuse */
		
		try {
			file = new File("couch-results.txt");
			if (!file.exists()) {
				file.createNewFile();
			}
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			//bw.write(content);
			System.setOut(null); //disable Couch's obnoxious printing to console
			
			randomGenerator = new Random();
			randomKeys = new ArrayList<String>();
			defaultValue = new JSONObject("{ \"age\": \"null\", \"job\": \"null\", \"marital\": \"null\", "+ 
				    "\"education\": \"null\", \"default\": \"null\", \"balance\": \"null\", " +
				    "\"housing\": \"null\", \"loan\": \"null\", \"contact\": \"null\", " +
				     "\"day\": \"null\", \"month\": \"null\", \"duration\": \"null\", " +
				     "\"campaign\": \"null\", \"pdays\": \"null\", \"previous\": \"null\", " +
				      "\"poutcome\": \"null\", \"y\": \"null\" }");

			runSmallDataset();
			runMediumDataset();
			runLargeDataset();
		
			bw.close();
		} catch (IOException e){
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void runSmallDataset(){
		CouchDbProperties properties_small = new CouchDbProperties()
		  .setDbName("small")
		  .setCreateDbIfNotExist(true)
		  .setProtocol("http")
		  .setHost("localhost")
		  .setPort(5984);
		CouchDbClient smallDBClient = new CouchDbClient(properties_small);
		smallDBClient.context().deleteDB("small", "delete database");
		smallDBClient.context().createDB("small");
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB("bank-small.csv", smallDBClient);
			long after_load = System.currentTimeMillis();
			bw.write("time to load small: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("item"+randomInt);
		    }
		    run100Read(smallDBClient);
		    run100Write(smallDBClient);
		    run5050(smallDBClient);
		    run9010(smallDBClient);
		    run1090(smallDBClient);
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
			
	}
	
	public static void runMediumDataset(){
		
		CouchDbProperties properties_medium = new CouchDbProperties()
		  .setDbName("medium")
		  .setCreateDbIfNotExist(true)
		  .setProtocol("http")
		  .setHost("localhost")
		  .setPort(5984);
		CouchDbClient mediumDBClient = new CouchDbClient(properties_medium);
		mediumDBClient.context().deleteDB("medium", "delete database");
		mediumDBClient.context().createDB("medium");
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB("bank-medium.csv", mediumDBClient);
			long after_load = System.currentTimeMillis();
			bw.write("time to load medium: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("item"+randomInt);
		    }
		    run100Read(mediumDBClient);
		    run100Write(mediumDBClient);
		    run5050(mediumDBClient);
		    run9010(mediumDBClient);
		    run1090(mediumDBClient);
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}	
			
	}
	
	public static void runLargeDataset(){
		
		CouchDbProperties properties_large = new CouchDbProperties()
		  .setDbName("large")
		  .setCreateDbIfNotExist(true)
		  .setProtocol("http")
		  .setHost("localhost")
		  .setPort(5984);
		CouchDbClient largeDBClient = new CouchDbClient(properties_large);
		largeDBClient.context().deleteDB("large", "delete database");
		largeDBClient.context().createDB("large");
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB("bank-large.csv", largeDBClient);
			long after_load = System.currentTimeMillis();
			bw.write("time to load large: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("item"+randomInt);
		    }
		    run100Read(largeDBClient);
		    run100Write(largeDBClient);
		    run5050(largeDBClient);
		    run9010(largeDBClient);
		    run1090(largeDBClient);
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}	
			
		
	}
	
	public static int loadDB(String file, CouchDbClient dbClient) throws JSONException, IOException{
		CSVReader reader = new CSVReader(new FileReader(file));
		String [] nextLine;
		String [] items;
		String s = "";
		int n = 0;
		
	    
	    nextLine = reader.readNext(); //skip header
		while ((nextLine = reader.readNext()) != null) {
		 // nextLine[] is an array of values from the line
			Map<String, Object>map = new HashMap<String, Object>();
			
			items = nextLine[0].split(";");
			s = "{ \"age\": " + items[0] + ", \"job\": " + items[1] + ", \"marital\": " + items[2] + 
					    ", \"education\": " + items[3] + ", \"default\": " + items[4] + ", \"balance\": " + items[5] + 
					    ", \"housing\": " + items[6] + ", \"loan\": " + items[7] + ", \"contact\": " + items[8] +
					     ", \"day\": " + items[9] + ", \"month\": " + items[10] + ", \"duration\": " + items[11] +
					     ", \"campaign\": " + items[12] + ", \"pdays\": " + items[13] + ", \"previous\": " + items[14] +
					      ", \"poutcome\": " + items[15] + ", \"y\": " + items[16] + " }";
			JSONObject json = new JSONObject(s);
			map.put("_id", "item"+n);
			map.put("data", json);
			dbClient.save(map);
			s = "";
			n++;
			
	    }
		reader.close();
		return n;
	}
	
	public static void run100Read(CouchDbClient c){
		
		int size = randomKeys.size();
		long pre_100read = System.currentTimeMillis();
		for (int i=0; i<size; i++){
			c.find(JSONObject.class, (String) randomKeys.get(i));
		}
		long after_100read = System.currentTimeMillis();
		try {
			bw.write("time to run 100% read workload: "+(after_100read-pre_100read)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void run100Write(CouchDbClient c){
		
		int size = randomKeys.size();
		HashMap<String, Object> map = new HashMap<String, Object>();
		ArrayList<HashMap<String, Object>> objectList = new ArrayList<HashMap<String, Object>>();
		for (int i=0; i<size; i++){
			objectList.add(c.find(HashMap.class, (String) randomKeys.get(i)));
		}

		
		long oldTime = System.currentTimeMillis();
		long running_time = 0;
		
		for (int i=0; i<size; i++){
			c.remove(objectList.get(i));
			map.clear();
			map.put("_id", (String) randomKeys.get(i));
			map.put("data", defaultValue);
			c.save(map);
			running_time += (System.currentTimeMillis() - oldTime);
			objectList.clear();
			for (int k=0; k<size; k++){
				objectList.add(c.find(HashMap.class, (String) randomKeys.get(k)));
			}
			//don't count the above loop in the running time
			oldTime = System.currentTimeMillis();
		}
		
		try {
			bw.write("time to run 100% write workload: "+running_time+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void run5050(CouchDbClient c){
		int size = randomKeys.size();
		int firstHalf = size/2;
		//int secondHalf = size-firstHalf;
		HashMap<String, Object> map = new HashMap<String, Object>();
		ArrayList<HashMap<String, Object>> objectList = new ArrayList<HashMap<String, Object>>();
		for (int i=0; i<size; i++){
			objectList.add(c.find(HashMap.class, (String) randomKeys.get(i)));
		}

		
		long oldTime = System.currentTimeMillis();
		long running_time = 0;
		
		//50% read
		for (int i=0; i<size; i++){
			c.find(JSONObject.class, (String) randomKeys.get(i));
		}
		
		//50% write
		for (int i=0; i<firstHalf; i++){
			c.remove(objectList.get(i));
			map.clear();
			map.put("_id", (String) randomKeys.get(i));
			map.put("data", defaultValue);
			c.save(map);
			running_time += (System.currentTimeMillis() - oldTime);
			objectList.clear();
			for (int k=0; k<size; k++){
				objectList.add(c.find(HashMap.class, (String) randomKeys.get(k)));
			}
			//don't count the above loop in the running time
			oldTime = System.currentTimeMillis();
		}
		
		try {
			bw.write("time to run 50% read, 50% write workload: "+running_time+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void run9010(CouchDbClient c){
		int size = randomKeys.size();
		int tenPercent = (int) (size*.1); //write
		int ninetyPercent = (int) (size*.9); //read
		int halfOfTen = tenPercent/2;
		HashMap<String, Object> map = new HashMap<String, Object>();
		ArrayList<HashMap<String, Object>> objectList = new ArrayList<HashMap<String, Object>>();
		for (int i=0; i<size; i++){
			objectList.add(c.find(HashMap.class, (String) randomKeys.get(i)));
		}

		
		long oldTime = System.currentTimeMillis();
		long running_time = 0;
		
		
		//90% read
		for (int i=0; i<ninetyPercent; i++){
			c.find(JSONObject.class, (String) randomKeys.get(i));
		}
		
		//10% write
		for (int i=0; i<halfOfTen; i++){
			c.remove(objectList.get(i));
			map.clear();
			map.put("_id", (String) randomKeys.get(i));
			map.put("data", defaultValue);
			c.save(map);
			running_time += (System.currentTimeMillis() - oldTime);
			objectList.clear();
			for (int k=0; k<size; k++){
				objectList.add(c.find(HashMap.class, (String) randomKeys.get(k)));
			}
			//don't count the above loop in the running time
			oldTime = System.currentTimeMillis();
		}
		
		try {
			bw.write("time to run 90% read, 10% write workload: "+running_time+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void run1090(CouchDbClient c){
		int size = randomKeys.size();
		int tenPercent = (int) (size*.1); //read
		int ninetyPercent = (int) (size*.9); //write
		int halfOfNinety = ninetyPercent/2;
		HashMap<String, Object> map = new HashMap<String, Object>();
		ArrayList<HashMap<String, Object>> objectList = new ArrayList<HashMap<String, Object>>();
		for (int i=0; i<size; i++){
			objectList.add(c.find(HashMap.class, (String) randomKeys.get(i)));
		}

		long oldTime = System.currentTimeMillis();
		long running_time = 0;

		//10% read
		for (int i=0; i<tenPercent; i++){
			c.find(JSONObject.class, (String) randomKeys.get(i));
		}
		
		//90% write
		for (int i=0; i<halfOfNinety; i++){
			c.remove(objectList.get(i));
			map.clear();
			map.put("_id", (String) randomKeys.get(i));
			map.put("data", defaultValue);
			c.save(map);
			running_time += (System.currentTimeMillis() - oldTime);
			objectList.clear();
			for (int k=0; k<size; k++){
				objectList.add(c.find(HashMap.class, (String) randomKeys.get(k)));
			}
			//don't count the above loop in the running time
			oldTime = System.currentTimeMillis();
		}

		try {
			bw.write("time to run 10% read, 90% write workload: "+running_time+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

