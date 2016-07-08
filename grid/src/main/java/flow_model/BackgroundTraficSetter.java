package flow_model;

import eduni.simjava.distributions.Sim_binomial_obj;
import eduni.simjava.distributions.Sim_uniform_obj;
import gridsim.GridResource;
import gridsim.util.TrafficGenerator;

import java.util.LinkedList;

public class BackgroundTraficSetter {
    //private static double rate = 400000000;//0.1;// (Gbps);
    
    private static long timeInterval = 1000;
    private static int packetNum;// = 1;
    private static int packetSize = 11921;
    //private static int packetSize =(int) Math.round ( rate  * timeInterval / (8.0 * packetNum * DataUnits.getSize() )  ); //1000;  //in units
    
       
    public static String getBackgroundSetupString(){
	double averageRate = packetNum * packetSize * DataUnits.getSize() * 8 / (double) ( timeInterval );
	double createdRate = packetNum * packetSize / (double) ( timeInterval );
	return "Bckgroung trafic setup: [ packetSize: " + packetSize + " timeInterval: " + timeInterval 
		+ " packetNum: " + packetNum + " internal rate: " + createdRate +  "(MB/s) AVERAGE BASE RATE: " + averageRate + " (bits/s) ]";
    }
    
    private static void readFromFile(String filename){

	System.out.println("Reading background trafic setup from file: " + filename);
    }
    
    public static void setupBackgroundTrafic(double backgroundFlow, LinkedList<GridResource> resList){
	//readFromFile(backgroundFlow);
	BackgroundTraficSetter.packetNum = (int) backgroundFlow;
	System.out.println( getBackgroundSetupString() );
        // generates some background traffic using SimJava2 distribution
        // package. NOTE: if you set the values to be too high, then
        // the simulation might finish longer
	TrafficGenerator tg = new TrafficGenerator(
	    new Sim_binomial_obj("freq",1,packetNum),  // num of packets 
	    new Sim_binomial_obj("inter_arrival_time",1,timeInterval) );
	            //new Sim_uniform_obj("freq",packetNum,packetNum+1),  // num of packets 
	            //new Sim_uniform_obj("inter_arrival_time",timeInterval,timeInterval+1) );
	
        // for each time, sends junk packet(s) to all entities
        tg.setPattern(TrafficGenerator.SEND_ONE_ONLY);
        
        tg.setPacketSize(new Sim_binomial_obj("packet_size",1 ,packetSize ) );
	
	for (GridResource res: resList){
	    res.setBackgroundTraffic(tg);
	}
    }
    

}
