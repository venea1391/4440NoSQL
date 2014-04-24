import au.com.bytecode.opencsv.CSVReader;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;


public class Mongo {
	public static MongoClient mongoClient;
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
			
			mongoClient = new MongoClient( "localhost" , 27017 );
			mongoClient.dropDatabase("small");
			mongoClient.dropDatabase("medium");
			mongoClient.dropDatabase("large");
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
			loadDB(db, coll, "bank-small.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load small: "+(after_load-pre_load)+" ms\n");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
			
	}
	
	public static void runMediumDataset(){
		
		DB db = mongoClient.getDB("medium");
		DBCollection coll = db.getCollection("medium_coll");
		try {
			long pre_load = System.currentTimeMillis();
			loadDB(db, coll, "bank-medium.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load medium: "+(after_load-pre_load)+" ms\n");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
			
	}
	
	public static void runLargeDataset(){
		
		DB db = mongoClient.getDB("large");
		DBCollection coll = db.getCollection("large_coll");
		try {
			long pre_load = System.currentTimeMillis();
			loadDB(db, coll, "bank-large.csv");
			long after_load = System.currentTimeMillis();
			bw.write("time to load large: "+(after_load-pre_load)+" ms\n");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
			
	}
	
	
	public static void loadDB(DB db, DBCollection coll, String file) 
			throws IOException, FileNotFoundException {
		CSVReader reader = new CSVReader(new FileReader(file), ';');
		String [] nextLine;
		String[] items;
		int n = 0;
		//age;"job";"marital";"education";"default";"balance";"housing";
		//"loan";"contact";"day";"month";"duration";"campaign";"pdays";
		//"previous";"poutcome";"y"
	    nextLine = reader.readNext(); //skip header
		while ((nextLine = reader.readNext()) != null) {
			items = nextLine[0].split(";");
			    /*s += "{ \"age\": " + items[0] + ", \"job\": " + items[1] + ", \"marital\": " + items[2] + 
			    ", \"education\": " + items[3] + ", \"default\": " + items[4] + ", \"balance\": " + items[5] + 
			    ", \"housing\": " + items[6] + ", \"loan\": " + items[7] + ", \"contact\": " + items[8] +
			     ", \"day\": " + items[9] + ", \"month\": " + items[10] + ", \"duration\": " + items[11] +
			     ", \"campaign\": " + items[12] + ", \"pdays\": " + items[13] + ", \"previous\": " + items[14] +
			      ", \"poutcome\": " + items[15] + ", \"y\": " + items[16] + " }"; */
			    //System.out.println(s);
			BasicDBObject doc = new BasicDBObject("age", items[0])
                        .append("job", items[1])
                        .append("marital", items[2])
                        .append("education", items[3])
                        .append("default", items[4])
                        .append("balance", items[5])
                        .append("housing", items[6])
                        .append("loan", items[7])
                        .append("contact", items[8])
                        .append("day", items[9])
                        .append("month", items[10])
                        .append("duration", items[11])
                        .append("campaign", items[12])
                        .append("pdays", items[13])
                        .append("previous", items[14])
                        .append("poutcome", items[15])
                        .append("y", items[16]);

			coll.insert(doc);

			n++;
		}

		//System.out.println(n);
		//DBObject myDoc = coll.findOne();
		//System.out.println(myDoc);
		reader.close();
	}
	
	/**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    /*public int delete(String database, String table, String key) {
        com.mongodb.DB db = null;
        try {
            db = mongoClient.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q);
            return res.getN() == 1 ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }*/

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    /*public int insert(String database, String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongoClient.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append("_id", key);
            for (String k : values.keySet()) {
                r.put(k, values.get(k).toArray());
            }
            WriteResult res = collection.insert(r);
            return res.getError() == null ? 0 : 1;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }*/

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    /*public int read(String database, String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        com.mongodb.DB db = null;
        try {
            db = mongoClient.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject fieldsToReturn = new BasicDBObject();

            DBObject queryResult = null;
            if (fields != null) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.findOne(q, fieldsToReturn);
            }
            else {
                queryResult = collection.findOne(q);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
            }
            return queryResult != null ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }*/

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    /*public int update(String database, String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongoClient.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext()) {
                String tmpKey = keys.next();
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

            }
            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u, false, false);
            return res.getN() == 1 ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }*/
}
