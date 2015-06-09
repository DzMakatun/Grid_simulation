package Simulation1;

import gridsim.Gridlet;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class LogParser {

	public static void main(String[] args) {
		int maxJobs = Integer.MAX_VALUE;
		String csvFile = "KISTIlog1.csv";
		String outputFile = "KISTIlogFilerred.csv";		
		String filter_key="";
		
		BufferedReader br = null;
		Writer writer = null;
		String line = "";
		String cvsSplitBy = ",";
		
		//2014-06-15 19:01:02
		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

		int goodCounter = 0;
		int totalCounter =0; 
		
		int id;
		long inputFileSize;
		long outputFileSize;
		String InputFileName;
		
		Interval interval;
		double duration = 0;
		DateTime jobStart;
		DateTime jobFinish;
		
		try { 
			System.out.println("Reading file: " + csvFile);
			br = new BufferedReader(new FileReader(csvFile));
			
			System.out.println("will wright to file: " + outputFile);
			writer = new BufferedWriter(new OutputStreamWriter(
			          new FileOutputStream(outputFile)));
			
			while ((line = br.readLine()) != null && goodCounter < maxJobs) {
				totalCounter++;				
				String[] jobData = line.split(cvsSplitBy);
				//System.out.println(line);//print the records for debug
				
				try{
					//read the parameters
					id = Integer.parseInt(jobData[0]);
					InputFileName = jobData[9].replace("\"", "");
					inputFileSize = Long.parseLong(jobData[10]);
					outputFileSize = Long.parseLong(jobData[31]);		
					
					jobStart = formatter.parseDateTime(jobData[16].replace("\"", ""));
					jobFinish = formatter.parseDateTime(jobData[17].replace("\"", ""));
					interval = new Interval(jobStart,jobFinish); //interval in milliseconds
					duration = interval.toDurationMillis() / 1000; //duration in seconds
					
					if (duration > 0 && inputFileSize > 0 && outputFileSize > 0){// filterring good log records here
						writer.write(line + "\n");
						goodCounter++;
					}					
				}catch (NumberFormatException e) {
				//e.printStackTrace();
				System.out.println("Failed to read line:" + line);
				}catch (Exception e) {
					//e.printStackTrace();
					//System.out.println("Failed to read line:" + line);
					}								
				
				
				if(totalCounter % 1000 == 0){
					System.out.println( totalCounter + " lines processed, " + goodCounter + "lines written");
				}
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
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("");
		System.out.println(totalCounter + " jobs were red from a file," + goodCounter + "lies were written" );
	}

}
