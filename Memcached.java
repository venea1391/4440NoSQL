import net.spy.memcached.MemcachedClient;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;

import au.com.bytecode.opencsv.CSVReader;


public class Memcached {

	public static void main(String[] args) {
		MemcachedClient c;
		try {
			c = new MemcachedClient(
				    new InetSocketAddress("localhost", 11211));
			
			try {
				CSVReader reader = new CSVReader(new FileReader("Wholesale-customers-data.csv"));
				String [] nextLine;
				String key = "";
				String value = "";
				int n = 0;
				
			    try {
			    	nextLine = reader.readNext(); //skip header
			    	
					while ((nextLine = reader.readNext()) != null) {
					    // nextLine[] is an array of values from the line
						key = "item"+n;
					    value = "Channel " + nextLine[0] + " Region " + nextLine[1] + " Fresh " + nextLine[2] + 
					    " Milk " + nextLine[3] + " Grocery " + nextLine[4] + " Frozen " + nextLine[5] + 
					    " Detergents_Paper " + nextLine[6] + " Delicassen " + nextLine[7]; 
					    n++;
					    
					    c.set(key, 0, value);
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			Object myObject=c.get("item0");
			System.out.println(myObject);
			myObject=c.get("item439");
			System.out.println(myObject);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
