package flow_model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class DatasetParser {

    public static void main(String[] args) throws Throwable {
	String inputFilename = "F:/git/Grid_simulation/grid/src/main/java/flow_model/input/KISTI_60k_filtered.csv";
	String outputPrefix = "input/T2Kpart";
	int maxFiles = 60000;
	int[] portionSizes = {66, 15, 7, 3, 9};
	String cvsSplitBy = ",";
	
	int totalLines = 0;
	double totalSize = 0;
	int currentLine = 0;
	double curentSize = 0;
	int lineIter = 0;
	
	
	BufferedReader br = null;
	System.out.println("Reading file: " + inputFilename);
	br = new BufferedReader(new FileReader(inputFilename));
	String line;
	String[] jobData; 
	long inputFileSize;
	
	//calculate total size of data
	while ((line = br.readLine()) != null &&  totalLines < maxFiles) {	    
	    jobData = line.split(cvsSplitBy);
	    inputFileSize = Long.parseLong(jobData[10]); //read filesize
	    
	    totalSize += inputFileSize;
	    totalLines++;
	}
	br.close();
	System.out.println("TOTAL: " + totalLines +" files "+ totalSize + " data units " + totalSize / DataUnits.getSize() + " " + DataUnits.getName());
	
	
	//write output files
	Writer writer = null;
	double portion;
	String outputFilename;
	int i = 0;
	br = new BufferedReader(new FileReader(inputFilename));
	for (int percentage: portionSizes){
	    i++;
	    outputFilename = outputPrefix + i + "_" + percentage + ".csv";
	    writer = new BufferedWriter(new OutputStreamWriter(
		          new FileOutputStream(outputFilename)));
	    portion = totalSize * (double) percentage / 100;
	    currentLine = 0;
	    curentSize = 0;	
	    System.out.println(percentage + " % portion: " + portion);
	    while ((line = br.readLine()) != null && lineIter < maxFiles && curentSize < portion ){
		    jobData = line.split(cvsSplitBy);
		    inputFileSize = Long.parseLong(jobData[10]); //read filesize
		    curentSize += inputFileSize;
		    currentLine++;		
		    lineIter++;
		    writer.write(line + "\n");
	    }
	    if(curentSize < portion){
		//System.out.println("WRONG PERCENTAGES");
		//break;
	    }
	    writer.close();
	    System.out.println(outputFilename +" : " 
	    + currentLine +" files ( " + 100 *(double) currentLine / totalLines + " %) " 
		    + curentSize + " data units ( " + 100 * curentSize / totalSize + " %) "
		    + curentSize / DataUnits.getSize() + " " + DataUnits.getName());
	}
	br.close();
	

    }

}
