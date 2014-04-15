import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import au.com.bytecode.opencsv.CSVReader;

public class TranslatorForRedis {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			CSVReader reader = new CSVReader(new FileReader("Wholesale-customers-data.csv"));
			String [] nextLine;
			String s = "";
			int n = 0;
			
		    try {
		    	nextLine = reader.readNext(); //skip header
		    	
				while ((nextLine = reader.readNext()) != null) {
				    // nextLine[] is an array of values from the line
				    s += "hmset items:row"+n+" Channel " + nextLine[0] + " Region " + nextLine[1] + " Fresh " + nextLine[2] + 
				    " Milk " + nextLine[3] + " Grocery " + nextLine[4] + " Frozen " + nextLine[5] + 
				    " Detergents_Paper " + nextLine[6] + " Delicassen " + nextLine[7] + "\n"; 
				    n++;
				}
				
				System.out.println(n);
				try {
			          File file = new File("Wholesale-customers-data.txt");
			          System.out.println("Wholesale-customers-data.txt file created");
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
