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
		String prefix = ""; //only jobs with input file starting with this prefix are included into results
		String csvFile = "KISTIlog1.csv";
		String outputFile = "KISTI_all_filtered.csv";		

		
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
		
		Interval interval, inputTransferInterval, outputTransferInterval;
		double duration = 0;
		double inputTransferDuration, outputTransferDuration;
		DateTime jobStart;
		DateTime jobFinish;
		
		DateTime inputTransferStart, inputTransferFinish, outputTransferStart, outputTransferFinish;
		Interval inputTransferinterval, outputTransferinterval;
		double inputTransferduration, outputTransferduration, inSpeed, outSpeed, alpha, beta;
		

		
		//statistics		
		double totalDuration = 0;
		double averageDuration = 0;
		double minDuration = Double.MAX_VALUE;
		double maxDuration = 0;
		
		long totalInputSize = 0;
		long averageInputSize = 0;
		long minInputSize = Long.MAX_VALUE;
		long maxInputSize = 0;
		
		long totalOutputSize = 0;
		long averageOutputSize = 0;
		long minOutputSize = Long.MAX_VALUE;
		long maxOutputSize = 0;
		
		double totalInputTransferDuration = 0;
		double averageInputTransferDuration = 0;
		double minInputTransferDuration = Double.MAX_VALUE;
		double maxInputTransferDuration = 0;
		
		double totalOutputTransferDuration = 0;
		double averageOutputTransferDuration = 0;
		double minOutputTransferDuration = Double.MAX_VALUE;
		double maxOutputTransferDuration = 0;
		
		double averageInputTransferSpeed = 0;
		double minInputTransferSpeed = Double.MAX_VALUE;
		double maxInputTransferSpeed = 0;
		
		double averageOutputTransferSpeed = 0;
		double minOutputTransferSpeed = Double.MAX_VALUE;
		double maxOutputTransferSpeed = 0;
		
		double minAlpha = Double.MAX_VALUE;
		double maxAlpha = 0;
		
		double minBeta = Double.MAX_VALUE;
		double maxBeta = 0;
		
		
		
		
		
		
		
		
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
					InputFileName = jobData[8].replace("\"", "");
					inputFileSize = Long.parseLong(jobData[10]) / (1024 * 1024); //in MB
					outputFileSize = Long.parseLong(jobData[31]) / (1024 * 1024); //in MB		
					
					jobStart = formatter.parseDateTime(jobData[16].replace("\"", ""));
					jobFinish = formatter.parseDateTime(jobData[17].replace("\"", ""));				
					interval = new Interval(jobStart,jobFinish); //interval in milliseconds
					duration = interval.toDurationMillis() / 1000; //duration in seconds
					
					inputTransferStart = formatter.parseDateTime(jobData[15].replace("\"", ""));
					inputTransferFinish = formatter.parseDateTime(jobData[16].replace("\"", ""));
					inputTransferinterval = new Interval(inputTransferStart,inputTransferFinish); //interval in milliseconds
					inputTransferduration = inputTransferinterval.toDurationMillis() / 1000; //duration in seconds
					
					outputTransferStart = formatter.parseDateTime(jobData[17].replace("\"", ""));
					outputTransferFinish = formatter.parseDateTime(jobData[18].replace("\"", ""));
					outputTransferinterval = new Interval(outputTransferStart,outputTransferFinish); //interval in milliseconds
					outputTransferduration = outputTransferinterval.toDurationMillis() / 1000; //duration in seconds
					
					if (duration > 0 && inputFileSize > 0 && outputFileSize > 0 && InputFileName.startsWith(prefix)){// filterring good log records here
						writer.write(line + "\n");
						goodCounter++;
						
						//calculate statistics
						inSpeed = inputFileSize / inputTransferduration;
						outSpeed = outputFileSize / outputTransferduration;
						alpha = duration / inputFileSize;
						beta = outputFileSize / inputFileSize;
						
						totalDuration += duration;
						if(duration > maxDuration){maxDuration = duration;} 
						if(duration < minDuration){minDuration = duration;} 
						
						totalInputSize += inputFileSize;
						if(inputFileSize > maxInputSize){maxInputSize = inputFileSize;} 
						if(inputFileSize < minInputSize){minInputSize = inputFileSize;} 
						
						totalOutputSize+= outputFileSize;
						if(outputFileSize > maxOutputSize){maxOutputSize = outputFileSize;} 
						if(outputFileSize < minOutputSize){minOutputSize = outputFileSize;} 
						
						totalInputTransferDuration += inputTransferduration;
						if(inputTransferduration > maxInputTransferDuration){maxInputTransferDuration = inputTransferduration;} 
						if(inputTransferduration < minInputTransferDuration){minInputTransferDuration = inputTransferduration;} 
						
						totalOutputTransferDuration += outputTransferduration;
						if(outputTransferduration > maxOutputTransferDuration){maxOutputTransferDuration = outputTransferduration;} 
						if(outputTransferduration < minOutputTransferDuration){minOutputTransferDuration = outputTransferduration;} 
						
						if(inSpeed > maxInputTransferSpeed){maxInputTransferSpeed = inSpeed;} 
						if(inSpeed < minInputTransferSpeed){minInputTransferSpeed = inSpeed;} 
						
						if(outSpeed > maxOutputTransferSpeed){maxOutputTransferSpeed = outSpeed;} 
						if(outSpeed < minOutputTransferSpeed){minOutputTransferSpeed = outSpeed;} 
						
						if(alpha > maxAlpha){maxAlpha = alpha;} 
						if(alpha < minAlpha){minAlpha = alpha;} 
						
						if(outSpeed > maxBeta){maxBeta = outSpeed;} 
						if(outSpeed < minBeta){minBeta = outSpeed;} 


						
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
		System.out.println("--------------Statistics--------------------------------");
		System.out.println("Input filename prefix: " + prefix);
		System.out.println(totalCounter + " jobs were red from a file, " + goodCounter + " lines were written" );
		System.out.println(String.format("%18s	%18s 	%18s	%18s	%18s", "Value","Total","Average","Min","Max"));
		System.out.println(String.format("Duration (s)			%18f	%18f	%18f	%18f",totalDuration, totalDuration / goodCounter, minDuration, maxDuration));
		System.out.println(String.format("InputFileSize (Mb)		%18d	%18d	%18d	%18d",totalInputSize, totalInputSize / goodCounter, minInputSize, maxInputSize));
		System.out.println(String.format("OutputFileSize (Mb)		%18d	%18d	%18d	%18d",totalOutputSize, totalOutputSize / goodCounter, minOutputSize, maxOutputSize));
		System.out.println(String.format("InputTransferDuration (s)	%18f	%18f	%18f	%18f",totalInputTransferDuration, totalInputTransferDuration / goodCounter, minInputTransferDuration, maxInputTransferDuration));
		System.out.println(String.format("OutputTransferDuration (s)	%18f	%18f	%18f	%18f",totalOutputTransferDuration, totalOutputTransferDuration / goodCounter, minOutputTransferDuration, maxOutputTransferDuration));
		System.out.println(String.format("InputTransferSpeed (Mb/s)	%18s	%18f	%18f	%18f","-", totalInputSize /  totalInputTransferDuration, minInputTransferSpeed, maxInputTransferSpeed));
		System.out.println(String.format("OutputTransferSpeed (Mb/s)	%18s	%18f	%18f	%18f", "-", totalOutputSize / totalOutputTransferDuration, minOutputTransferSpeed, maxOutputTransferSpeed));
		System.out.println(String.format("Alpha duration/inSize (s/Mb)	%18s	%18f	%18f	%18f","-", totalDuration / totalInputSize, minAlpha, maxAlpha));
		System.out.println(String.format("Beta out/in (Mb/Mb)		%18s	%18f	%18f	%18f", "-", ( (double) totalOutputSize )/( (double) totalInputSize), minBeta, maxBeta));
		
	}

}
