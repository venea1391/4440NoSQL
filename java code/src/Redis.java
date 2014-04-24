import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.DB;
import com.mongodb.DBCollection;

import redis.clients.jedis.Jedis;
import au.com.bytecode.opencsv.CSVReader;

public class Redis {
	public static Jedis jedis;
	static File file;
	static BufferedWriter bw;
	
	public static void main(String[] args) {
		try {
			file = new File("couch-results.txt");
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
			loadDB("bank-small.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load small: "+(after_load-pre_load)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	public static void runMediumDataset(){
		
		jedis.select(2);
		jedis.flushDB();
		try {
			long pre_load = System.currentTimeMillis();
			loadDB("bank-medium.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load medium: "+(after_load-pre_load)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	public static void runLargeDataset(){
		
		jedis.select(3);
		jedis.flushDB();
		try {
			long pre_load = System.currentTimeMillis();
			loadDB("bank-large.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load large: "+(after_load-pre_load)+" ms\n");
		} catch (IOException e) {
			e.printStackTrace();
		}	
			
	}
	
	static public void loadDB(String file) throws IOException{
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
				
	}
}
