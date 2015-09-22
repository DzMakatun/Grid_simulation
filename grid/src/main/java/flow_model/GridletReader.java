package flow_model;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import gridsim.GridletList;
 
public class GridletReader {
 
  public static GridletList getGridletList(String gridletFileName, double gridletNumber) {
 
	GridletList gridletList = new GridletList();
	BufferedReader br = null;
	String line = "";

	int jobCounter = 0;
	int linesCounter = 0;
	DPGridlet gridlet = null;
	try { 
		System.out.println("------------------");
		System.out.println("GridletReader: Reading file: " + gridletFileName);
		br = new BufferedReader(new FileReader(gridletFileName));
		
		while ((line = br.readLine()) != null && jobCounter < gridletNumber) {
			linesCounter++;
			gridlet = readGridlet(line);
			if(gridlet != null){ //add only valid records from log
				gridletList.add(gridlet);
				jobCounter++;
				//System.out.println(String.format("Created a gridlet. id %s, length: %f (s), inputSize: %d (bytes), outputSize: %d (bytes)", gridlet.getGridletID(), gridlet.getGridletLength(), gridlet.getGridletFileSize(), gridlet.getGridletOutputSize()));
			}
			//if(linesCounter % 100 == 0){
			//	System.out.println("GridletReader: " + linesCounter + " lines processed, " + jobCounter + "gridlets created");
			//	}			
 
		}
 
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	} finally {
		if (br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	System.out.println("GridletReader: " + linesCounter + " lines were red from a file, " + jobCounter + " gridlets created");
	System.out.println("------------------");
	return gridletList;
  }

private static DPGridlet readGridlet(String line) {
	String cvsSplitBy = ",";
	DPGridlet gridlet = null;	
	
	int id;
	long inputFileSize;
	long outputFileSize;
	String InputFileName;
	
	Interval interval;
	double duration = 0;
	DateTime jobStart;
	DateTime jobFinish;
	//2014-06-15 15:15:05
	DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

	
	String[] jobData = line.split(cvsSplitBy);
	//System.out.println(line); //print the records for debug
	//System.out.println("");
	
	
	try{
		//read the parameters
		id = Integer.parseInt(jobData[0]);
		InputFileName = jobData[9].replace("\"", "");
		inputFileSize = Long.parseLong(jobData[10]); //in bytes
		outputFileSize = Long.parseLong(jobData[31]);  //in bytes
		
		
		
		jobStart = formatter.parseDateTime(jobData[16].replace("\"", ""));
		jobFinish = formatter.parseDateTime(jobData[17].replace("\"", ""));
		interval = new Interval(jobStart,jobFinish); //interval in milliseconds
		duration = interval.toDurationMillis() / 1000; //duration in seconds
		
		if (duration > 0 && inputFileSize > 0 && outputFileSize > 0 ){// filterring good log records here
			//we create gridlets only from jobs that passed requirements
			//gridlet = new Gridlet(id, duration , inputFileSize, outputFileSize);
			gridlet = new DPGridlet(id, duration, inputFileSize, outputFileSize, true); //do  track this
			gridlet.setGridletFinishedSoFar(0);
			//gridlet.setUserID(userId);
		}else{
			System.out.println("Gridlet filtered:\n" + line);
		}
		
		
		
		//g.addRequiredFile(InputFileName);
	}catch (Exception e) {
		e.printStackTrace();
		System.out.println("Failed to read line:" + line);
	}
	
	return gridlet;
}
 
}