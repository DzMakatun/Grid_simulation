package Simulation1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import gridsim.Gridlet;
import gridsim.GridletList;
 
public class GridletReader {
 
  public GridletList getGridletList(String gridletFileName, double gridletNumber) {
 
	GridletList gridletList = new GridletList();
	String csvFile = gridletFileName;
	BufferedReader br = null;
	String line = "";

	double jobCounter = 0;
	Gridlet gridlet = null;
	try { 
		br = new BufferedReader(new FileReader(csvFile));
		
		while ((line = br.readLine()) != null && jobCounter < gridletNumber) {
			Gridlet gridlet = readGridlet(line);
			gridletList.add(gridlet);
			jobCounter++; 
			
 
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
 
	System.out.println("GridletReader: " + jobCounter + " jobs were red from a file");
	return gridletList;
  }

private Gridlet readGridlet(String line) {
	String cvsSplitBy = ",";
	String[] jobData = line.split(cvsSplitBy);
	System.out.println("job");
		for(String word: jobData){
			System.out.print(word);
		}

	
	return gridlet;
}
 
}