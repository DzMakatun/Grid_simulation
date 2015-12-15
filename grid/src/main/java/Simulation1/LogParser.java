package Simulation1;

import gridsim.Gridlet;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.awt.Color;
import java.awt.Font;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;



import com.xeiam.xchart.*;
import com.xeiam.xchart.StyleManager.ChartType;
import com.xeiam.xchart.StyleManager.LegendPosition;

import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class LogParser {

	public static void main(String[] args) {
	        Set<String> prefixSet = new HashSet<String>();
	        String[] prefixes = //{"st_physics_", "st_fgt_", "st_jet_adc_", "st_mtd_", "st_fms_", "st_jet_", "st_laser_", "_adc_"};
	            {"st_physics_", "st_laser_",  "st_zerobias_",  "st_fms_",  "st_mtd_", "st_jet_adc_",  "st_jet_", "st_fgt_", "st_daqtenk_", "st_tof", "st_hlt_",  "_adc_"};
	        //{"st_physics_","st_zerobias_", "st_fgt_", "st_mtd_", "st_fms_", "st_hlt_" };
		int seriesNo = 0;
	        int maxJobs = 106000;//Integer.MAX_VALUE;
		String prefix = "st_physics_adc_"; //only jobs with input file starting with this prefix are included into results
		//String csvFile = "KISTI_60k_filtered.csv";
		String csvFile = "F:/KistProdDataDump/KistProdDataDump.csv";
		String outputFile = "delete_me.csv";		

		
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
		
		double realTimePerEvent;
		double nEvents;

		
		//plot statistics
		Chart fileSizeChart = new ChartBuilder().width(800).height(800).build();
		Chart fileSizeDurationsChart = new ChartBuilder().width(800).height(800).build();
		Chart alphaBetaChart = new ChartBuilder().width(800).height(800).build();
		Chart durationOutputChart = new ChartBuilder().width(800).height(800).build();
		Chart legendChart = new ChartBuilder().width(800).height(800).build();
		
		LinkedList<Double>[] inputSizes = new LinkedList[prefixes.length + 1 ];
		LinkedList<Double>[] outputSizes = new LinkedList[prefixes.length + 1 ];
		LinkedList<Double>[] durations = new LinkedList[prefixes.length + 1 ];
		LinkedList<Double>[] alphas = new LinkedList[prefixes.length + 1 ];
		LinkedList<Double>[] betas = new LinkedList[prefixes.length + 1 ];
		
		for(int i = 0; i < prefixes.length + 1; i++ ){
		    inputSizes[i] = new LinkedList<Double>();
		    outputSizes[i] = new LinkedList<Double>();
		    durations[i] = new LinkedList<Double>();
		    alphas[i] = new LinkedList<Double>();
		    betas[i] = new LinkedList<Double>();
		}

		//double[] inputSizes = new double[maxJobs];
		//double[] outputSizes = new double[maxJobs];
		//double[] durations = new double[maxJobs];
		//double[] alphas = new double[maxJobs];
		//double[] betas = new double[maxJobs];
		
		
		
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
					prefixSet.add( InputFileName.split("\\d")[0] );
					inputFileSize = Math.round( Double.parseDouble(jobData[10]) / (1024.0 * 1024.0)); //in MB
					outputFileSize = Math.round(Double.parseDouble(jobData[31]) / (1024.0 * 1024.0)); //in MB	
					
					
					realTimePerEvent = Double.parseDouble(jobData[28]); //s
					nEvents = Double.parseDouble(jobData[29]); 
					
					//jobStart = formatter.parseDateTime(jobData[16].replace("\"", ""));
					//jobFinish = formatter.parseDateTime(jobData[17].replace("\"", ""));				
					//interval = new Interval(jobStart,jobFinish); //interval in milliseconds
					//duration = interval.toDurationMillis() / 1000; //duration in seconds
					
					//inputTransferStart = formatter.parseDateTime(jobData[15].replace("\"", ""));
					//inputTransferFinish = formatter.parseDateTime(jobData[16].replace("\"", ""));
					//inputTransferinterval = new Interval(inputTransferStart,inputTransferFinish); //interval in milliseconds
					//inputTransferduration = inputTransferinterval.toDurationMillis() / 1000; //duration in seconds
					
					//outputTransferStart = formatter.parseDateTime(jobData[17].replace("\"", ""));
					//outputTransferFinish = formatter.parseDateTime(jobData[18].replace("\"", ""));
					//outputTransferinterval = new Interval(outputTransferStart,outputTransferFinish); //interval in milliseconds
					//outputTransferduration = outputTransferinterval.toDurationMillis() / 1000; //duration in seconds
					
					duration = realTimePerEvent * nEvents;
					alpha = duration / (double) inputFileSize;
					beta = outputFileSize / (double) inputFileSize;
					
					//if (duration > 12 * 3600 && inputFileSize > 0 && outputFileSize > 100 && InputFileName.startsWith(prefix) && alpha < 100 && beta < 1.5 && beta > 0.3){// filterring good log records here
					if (duration > 0 &&  inputFileSize > 0 && outputFileSize > 0 && alpha > 0 && beta > 0 && beta <= 1){// filterring good log records here
						writer.write(line + "\n");
						
						
						//calculate statistics
						//inSpeed = inputFileSize / inputTransferduration;
						//outSpeed = outputFileSize / outputTransferduration;

						
						totalDuration += duration;
						if(duration > maxDuration){maxDuration = duration;} 
						if(duration < minDuration){minDuration = duration;} 
						
						totalInputSize += inputFileSize;
						if(inputFileSize > maxInputSize){maxInputSize = inputFileSize;} 
						if(inputFileSize < minInputSize){minInputSize = inputFileSize;} 
						
						totalOutputSize+= outputFileSize;
						if(outputFileSize > maxOutputSize){maxOutputSize = outputFileSize;} 
						if(outputFileSize < minOutputSize){minOutputSize = outputFileSize;} 
						
						//totalInputTransferDuration += inputTransferduration;
						//if(inputTransferduration > maxInputTransferDuration){maxInputTransferDuration = inputTransferduration;} 
						//if(inputTransferduration < minInputTransferDuration){minInputTransferDuration = inputTransferduration;} 
						
						//totalOutputTransferDuration += outputTransferduration;
						//if(outputTransferduration > maxOutputTransferDuration){maxOutputTransferDuration = outputTransferduration;} 
						//if(outputTransferduration < minOutputTransferDuration){minOutputTransferDuration = outputTransferduration;} 
						
						//if(inSpeed > maxInputTransferSpeed){maxInputTransferSpeed = inSpeed;} 
						//if(inSpeed < minInputTransferSpeed){minInputTransferSpeed = inSpeed;} 
						
						//if(outSpeed > maxOutputTransferSpeed){maxOutputTransferSpeed = outSpeed;} 
						//if(outSpeed < minOutputTransferSpeed){minOutputTransferSpeed = outSpeed;} 
						
						if(alpha > maxAlpha){maxAlpha = alpha;} 
						if(alpha < minAlpha){minAlpha = alpha;} 
						if(beta > maxBeta){maxBeta = beta;} 
						if(beta < minBeta){minBeta = beta;} 
						
						//if(outSpeed > maxBeta){maxBeta = outSpeed;} 
						//if(outSpeed < minBeta){minBeta = outSpeed;} 
						
						//group by file prefixes
						seriesNo = prefixes.length;
						for (int i = 0; i < prefixes.length; i ++){
						    if (InputFileName.contains(prefixes[i]) ){
							seriesNo = i;
						    }
						}
						    

						//add data for the plots
						inputSizes[seriesNo].add( Double.valueOf(inputFileSize));
						outputSizes[seriesNo].add( Double.valueOf(outputFileSize));
						durations[seriesNo].add(Double.valueOf(duration / 3600.0));
						alphas[seriesNo].add(Double.valueOf(alpha));
						betas[seriesNo].add(Double.valueOf(beta));

						goodCounter++;
					}					
				}catch (NumberFormatException e) {
				e.printStackTrace();
				System.out.println("Failed to read line:" + line);
				}catch (Exception e) {
					e.printStackTrace();
					System.out.println("Failed to read line:" + line);
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
		System.out.println("Prefixes" + prefixSet.toString());
		System.out.println("Number of prefixes: " + prefixSet.size());
		
		
		//plot
		String seriesName = "bla";
		for(int i = 0; i < prefixes.length + 1; i++){
		    System.out.println("adding sirie No. " + i);
		    if (inputSizes[i].size() == 0){
			continue;
		    }
		    if (i < prefixes.length) {
			seriesName = prefixes[i];
		    }else{
			seriesName = "misc";
		    }
		    double[] inSizes = new double[inputSizes[i].size()];
		    double[] outSizes = new double[outputSizes[i].size()];
		    double[] dur = new double[durations[i].size()];
		    double[] alp = new double[alphas[i].size()];
		    double[] bet = new double[betas[i].size()];
		    double[] sample = {1};
		    
		    System.out.println("type casting");
		    for (int j = 0; j < inputSizes[i].size(); j++){
			inSizes[j] = inputSizes[i].get(j);
			outSizes[j] = outputSizes[i].get(j);
			dur[j] = durations[i].get(j);
			alp[j] = alphas[i].get(j);
			bet[j] = betas[i].get(j);
		    }
		    System.out.println("adding");
		    fileSizeChart.addSeries(seriesName, inSizes, outSizes).setMarker(SeriesMarker.CIRCLE).setSeriesType(Series.SeriesType.Line);
		    fileSizeDurationsChart.addSeries(seriesName, inSizes, dur).setMarker(SeriesMarker.CIRCLE).setSeriesType(Series.SeriesType.Line);    
		    alphaBetaChart.addSeries(seriesName, alp, bet).setMarker(SeriesMarker.CIRCLE).setSeriesType(Series.SeriesType.Line);
	            durationOutputChart.addSeries(seriesName, outSizes, dur).setMarker(SeriesMarker.CIRCLE).setSeriesType(Series.SeriesType.Line);
	            legendChart.addSeries(seriesName, sample, sample).setMarker(SeriesMarker.CIRCLE).setSeriesType(Series.SeriesType.Line);
	            System.out.println("done");
		}
		
		
		
		
		fileSizeChart.setChartTitle(" ");
		fileSizeChart.setXAxisTitle("input file size (MB)");
		fileSizeChart.setYAxisTitle("output file size (MB)");
		fileSizeChart.getStyleManager().setChartType(ChartType.Scatter);    
		fileSizeChart.getStyleManager().setLegendPosition(LegendPosition.OutsideE);
		durationOutputChart.getStyleManager().setLegendSeriesLineLength(10);
		fileSizeChart.getStyleManager().setChartBackgroundColor(Color.WHITE);
		fileSizeChart.getStyleManager().setChartTitleFont(new Font(Font.DIALOG, Font.PLAIN, 24));
		fileSizeChart.getStyleManager().setLegendFont(new Font(Font.DIALOG, Font.PLAIN, 22));
		fileSizeChart.getStyleManager().setAxisTitleFont(new Font(Font.DIALOG, Font.PLAIN, 30));
		fileSizeChart.getStyleManager().setAxisTickLabelsFont(new Font(Font.DIALOG, Font.PLAIN, 18));
		fileSizeChart.getStyleManager().setLegendVisible(false);
		fileSizeChart.getStyleManager().setMarkerSize(1);
		//chart.getStyleManager().setDecimalPattern("#.#E0");
		new SwingWrapper(fileSizeChart).displayChart();
		    
		//fileSizeDurationsChart.addSeries("fileSizeDuration", inputSizes, durations).setMarker(SeriesMarker.CIRCLE).setMarkerColor(Color.BLACK).setSeriesType(Series.SeriesType.Line);    
		fileSizeDurationsChart.setChartTitle("");
		fileSizeDurationsChart.setXAxisTitle("input file size (MB)");
		fileSizeDurationsChart.setYAxisTitle("job duration (hours)");
		fileSizeDurationsChart.getStyleManager().setChartType(ChartType.Scatter);    
		fileSizeDurationsChart.getStyleManager().setLegendPosition(LegendPosition.OutsideE);
		durationOutputChart.getStyleManager().setLegendSeriesLineLength(10);
		fileSizeDurationsChart.getStyleManager().setChartBackgroundColor(Color.WHITE);
		fileSizeDurationsChart.getStyleManager().setChartTitleFont(new Font(Font.DIALOG, Font.PLAIN, 24));
		fileSizeDurationsChart.getStyleManager().setLegendFont(new Font(Font.DIALOG, Font.PLAIN, 22));
		fileSizeDurationsChart.getStyleManager().setAxisTitleFont(new Font(Font.DIALOG, Font.PLAIN, 30));
		fileSizeDurationsChart.getStyleManager().setAxisTickLabelsFont(new Font(Font.DIALOG, Font.PLAIN, 18));
		//chart.getStyleManager().setDecimalPattern("#.#E0");
		fileSizeDurationsChart.getStyleManager().setLegendVisible(false);
		fileSizeDurationsChart.getStyleManager().setMarkerSize(1);
		new SwingWrapper(fileSizeDurationsChart).displayChart();
		    
		
		//alphaBetaChart.addSeries("alphaBeta", alphas, betas).setMarker(SeriesMarker.CIRCLE).setMarkerColor(Color.BLACK).setSeriesType(Series.SeriesType.Line);
		alphaBetaChart.setChartTitle("");
		alphaBetaChart.setXAxisTitle("alpha (s/MB)");
		alphaBetaChart.setYAxisTitle("beta");
		alphaBetaChart.getStyleManager().setChartType(ChartType.Scatter);    
		alphaBetaChart.getStyleManager().setLegendPosition(LegendPosition.OutsideE);
		durationOutputChart.getStyleManager().setLegendSeriesLineLength(10);
		alphaBetaChart.getStyleManager().setChartBackgroundColor(Color.WHITE);
		alphaBetaChart.getStyleManager().setChartTitleFont(new Font(Font.DIALOG, Font.PLAIN, 24));
		alphaBetaChart.getStyleManager().setLegendFont(new Font(Font.DIALOG, Font.PLAIN, 22));
		alphaBetaChart.getStyleManager().setAxisTitleFont(new Font(Font.DIALOG, Font.PLAIN, 30));
		alphaBetaChart.getStyleManager().setAxisTickLabelsFont(new Font(Font.DIALOG, Font.PLAIN, 18));
		//chart.getStyleManager().setDecimalPattern("#.#E0");
		alphaBetaChart.getStyleManager().setLegendVisible(false);
		alphaBetaChart.getStyleManager().setMarkerSize(1);
		new SwingWrapper(alphaBetaChart).displayChart();
		
				
		//durationOutputChart.addSeries("durationOutput", outputSizes, durations).setMarker(SeriesMarker.CIRCLE).setMarkerColor(Color.BLACK).setSeriesType(Series.SeriesType.Line);
		durationOutputChart.setChartTitle("");
		durationOutputChart.setXAxisTitle("output size (MB)");
		durationOutputChart.setYAxisTitle("duration (hours)");
		durationOutputChart.getStyleManager().setChartType(ChartType.Scatter);    
		durationOutputChart.getStyleManager().setLegendPosition(LegendPosition.OutsideE);
		durationOutputChart.getStyleManager().setLegendSeriesLineLength(10);
		durationOutputChart.getStyleManager().setChartBackgroundColor(Color.WHITE);
		durationOutputChart.getStyleManager().setChartTitleFont(new Font(Font.DIALOG, Font.PLAIN, 24));
		durationOutputChart.getStyleManager().setLegendFont(new Font(Font.DIALOG, Font.PLAIN, 22));
		durationOutputChart.getStyleManager().setAxisTitleFont(new Font(Font.DIALOG, Font.PLAIN, 30));
		durationOutputChart.getStyleManager().setAxisTickLabelsFont(new Font(Font.DIALOG, Font.PLAIN, 18));
		//chart.getStyleManager().setDecimalPattern("#.#E0");
		durationOutputChart.getStyleManager().setLegendVisible(false);
		durationOutputChart.getStyleManager().setMarkerSize(1);
		new SwingWrapper(durationOutputChart).displayChart();
		
		legendChart.setChartTitle("Legend");
		legendChart.setXAxisTitle(" ");
		legendChart.setYAxisTitle(" ");
		legendChart.getStyleManager().setChartType(ChartType.Scatter);    
		legendChart.getStyleManager().setLegendPosition(LegendPosition.OutsideE);
		legendChart.getStyleManager().setLegendSeriesLineLength(10);
		legendChart.getStyleManager().setChartBackgroundColor(Color.WHITE);
		legendChart.getStyleManager().setChartTitleFont(new Font(Font.DIALOG, Font.PLAIN, 24));
		legendChart.getStyleManager().setLegendFont(new Font(Font.DIALOG, Font.PLAIN, 22));
		legendChart.getStyleManager().setAxisTitleFont(new Font(Font.DIALOG, Font.PLAIN, 30));
		legendChart.getStyleManager().setAxisTickLabelsFont(new Font(Font.DIALOG, Font.PLAIN, 18));
		//chart.getStyleManager().setDecimalPattern("#.#E0");
		//LegendChart.getStyleManager().setLegendVisible(false);
		legendChart.getStyleManager().setMarkerSize(10);
		new SwingWrapper(legendChart).displayChart();
		
	}
	


}
