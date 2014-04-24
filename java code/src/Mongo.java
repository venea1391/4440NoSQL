import au.com.bytecode.opencsv.CSVReader;

import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class Mongo {
	public static MongoClient mongoClient;
	static File file;
	static BufferedWriter bw;
	
	public static Random randomGenerator;
	public static ArrayList<String> randomKeys;
	public static Map<String, String> defaultValue;
	public static final double PERCENTAGE = .1;
	
	public static void main(String[] args) {
		try {
			file = new File("mongo-results.txt");
			if (!file.exists()) {
				file.createNewFile();
			}
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			
			mongoClient = new MongoClient( "localhost" , 27017 );
			mongoClient.dropDatabase("small");
			mongoClient.dropDatabase("medium");
			mongoClient.dropDatabase("large");
			
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
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public static void runSmallDataset(){
		
		DB db = mongoClient.getDB("small");
		DBCollection coll = db.getCollection("small_coll");
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB(coll, "bank-small.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load small: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("item"+randomInt);
		    }
		    run100Read(coll);
		    run100Write(coll);
		    run5050(coll);
		    run9010(coll);
		    run1090(coll);
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	public static void runMediumDataset(){
		
		DB db = mongoClient.getDB("medium");
		DBCollection coll = db.getCollection("medium_coll");
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB(coll, "bank-medium.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load medium: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("item"+randomInt);
		    }
		    run100Read(coll);
		    run100Write(coll);
		    run5050(coll);
		    run9010(coll);
		    run1090(coll);
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	public static void runLargeDataset(){
		
		DB db = mongoClient.getDB("large");
		DBCollection coll = db.getCollection("large_coll");
		try {
			long pre_load = System.currentTimeMillis();
			int n = loadDB(coll, "bank-large.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load large: "+(after_load-pre_load)+" ms\n");
			
			//start workloads
			//generate (PERCENTAGE*size) random numbers
			
		    for (int idx = 0; idx <= (n*PERCENTAGE) ; ++idx){
		      int randomInt = randomGenerator.nextInt(n);
		      randomKeys.add("item"+randomInt);
		    }
		    run100Read(coll);
		    run100Write(coll);
		    run5050(coll);
		    run9010(coll);
		    run1090(coll);
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	
	public static int loadDB(DBCollection coll, String file) 
			throws IOException, FileNotFoundException {
		CSVReader reader = new CSVReader(new FileReader(file), ';');
		String [] nextLine;
		String[] items;
		HashMap<String, String> properties;
		int n = 0;
	    nextLine = reader.readNext(); //skip header
		while ((nextLine = reader.readNext()) != null) {
			items = nextLine[0].split(";");
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

			BasicDBObject doc = new BasicDBObject("item"+n, properties);
			coll.insert(doc);

			n++;
		}

		//System.out.println(n);
		//DBObject myDoc = coll.findOne();
		//System.out.println(myDoc);
		reader.close();
		return n;
	}
	
public static void run100Read(DBCollection coll){
		
		int size = randomKeys.size();
		long pre_100read = System.currentTimeMillis();
		for (int i=0; i<size; i++){
			coll.distinct((String) randomKeys.get(i));
		}
		long after_100read = System.currentTimeMillis();
		try {
			bw.write("time to run 100% read workload: "+(after_100read-pre_100read)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void run100Write(DBCollection coll){
		
		int size = randomKeys.size();

		BasicDBObject doc;
		ArrayList<BasicDBObject> objectList = new ArrayList<BasicDBObject>();
		for (int i=0; i<size; i++){
			objectList.add((BasicDBObject) (coll.distinct((String) randomKeys.get(i)).get(0)));
		}
		
		long oldTime = System.currentTimeMillis();
		long running_time = 0;
		
		for (int i=0; i<size; i++){
			coll.remove(objectList.get(i));
			doc = new BasicDBObject("item"+randomKeys.get(i), defaultValue);
			coll.insert(doc);
			
			running_time += (System.currentTimeMillis() - oldTime);
			objectList.clear();
			for (int k=0; k<size; k++){
				objectList.add((BasicDBObject) (coll.distinct((String) randomKeys.get(i)).get(0)));
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
	
	public static void run5050(DBCollection coll){
		int size = randomKeys.size();
		int firstHalf = size/2;
		//int secondHalf = size-firstHalf;
		
		BasicDBObject doc;
		ArrayList<BasicDBObject> objectList = new ArrayList<BasicDBObject>();
		for (int i=0; i<size; i++){
			objectList.add((BasicDBObject) (coll.distinct((String) randomKeys.get(i)).get(0)));
		}
		
		long oldTime = System.currentTimeMillis();
		long running_time = 0;
		
		for (int i=0; i<size; i++){
			coll.distinct((String) randomKeys.get(i));
		}
		for (int i=0; i<firstHalf; i++){
			coll.remove(objectList.get(i));
			doc = new BasicDBObject("item"+randomKeys.get(i), defaultValue);
			coll.insert(doc);
			
			running_time += (System.currentTimeMillis() - oldTime);
			objectList.clear();
			for (int k=0; k<size; k++){
				objectList.add((BasicDBObject) (coll.distinct((String) randomKeys.get(i)).get(0)));
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
	
	
	public static void run9010(DBCollection coll){
		int size = randomKeys.size();
		int tenPercent = (int) (size*.1); //write
		int ninetyPercent = (int) (size*.9); //read
		int halfOfTen = tenPercent/2;
		
		
		BasicDBObject doc;
		ArrayList<BasicDBObject> objectList = new ArrayList<BasicDBObject>();
		for (int i=0; i<size; i++){
			objectList.add((BasicDBObject) (coll.distinct((String) randomKeys.get(i)).get(0)));
		}
		
		long oldTime = System.currentTimeMillis();
		long running_time = 0;
		
		//90% read
		for (int i=0; i<ninetyPercent; i++){
			coll.distinct((String) randomKeys.get(i));
		}
		
		//10% write
		for (int i=0; i<halfOfTen; i++){
			coll.remove(objectList.get(i));
			doc = new BasicDBObject("item"+randomKeys.get(i), defaultValue);
			coll.insert(doc);
			
			running_time += (System.currentTimeMillis() - oldTime);
			objectList.clear();
			for (int k=0; k<size; k++){
				objectList.add((BasicDBObject) (coll.distinct((String) randomKeys.get(i)).get(0)));
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
	
	public static void run1090(DBCollection coll){
		int size = randomKeys.size();
		int tenPercent = (int) (size*.1); //read
		int ninetyPercent = (int) (size*.9); //write
		int halfOfNinety = ninetyPercent/2;
		
		BasicDBObject doc;
		ArrayList<BasicDBObject> objectList = new ArrayList<BasicDBObject>();
		for (int i=0; i<size; i++){
			objectList.add((BasicDBObject) (coll.distinct((String) randomKeys.get(i)).get(0)));
		}
		
		long oldTime = System.currentTimeMillis();
		long running_time = 0;
		
		//10% read
		for (int i=0; i<tenPercent; i++){
			coll.distinct((String) randomKeys.get(i));
		}
		
		//90% write
		for (int i=0; i<halfOfNinety; i++){
			coll.remove(objectList.get(i));
			doc = new BasicDBObject("item"+randomKeys.get(i), defaultValue);
			coll.insert(doc);
			
			running_time += (System.currentTimeMillis() - oldTime);
			objectList.clear();
			for (int k=0; k<size; k++){
				objectList.add((BasicDBObject) (coll.distinct((String) randomKeys.get(i)).get(0)));
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
