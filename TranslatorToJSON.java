import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import au.com.bytecode.opencsv.CSVReader;

public class TranslatorToJSON {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			CSVReader reader = new CSVReader(new FileReader("Wholesale-customers-data.csv"));
			String [] nextLine;
			String s = "";
			int n = 0;
			
		    try {
		    	nextLine = reader.readNext(); //skip header
		    	s = "{ \"docs\": [";
				while ((nextLine = reader.readNext()) != null) {
				    // nextLine[] is an array of values from the line
				    s += "{\"Channel\": " + nextLine[0] + ", \"Region\": " + nextLine[1] + ", \"Fresh\": " + nextLine[2] + 
				    ", \"Milk\": " + nextLine[3] + ", \"Grocery\": " + nextLine[4] + ", \"Frozen\": " + nextLine[5] + 
				    ", \"Detergents_Paper\": " + nextLine[6] + ", \"Delicassen\": " + nextLine[7] + "}, \n"; 
				    n++;
				}
				s = s.substring(0, s.length()-3);
				s += "]}";
				//System.out.println(s);
				System.out.println(n);
				try {
			          File file = new File("Wholesale-customers-data.json");
			          System.out.println("Wholesale-customers-data.json file created");
			          BufferedWriter output = new BufferedWriter(new FileWriter(file));
			          output.write(s);
			          output.close();
			        } catch ( IOException e ) {
			           e.printStackTrace();
			        }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
