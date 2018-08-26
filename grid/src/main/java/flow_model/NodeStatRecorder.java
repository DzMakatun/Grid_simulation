package flow_model;

import gridsim.GridSim;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;

public class NodeStatRecorder {
    private static boolean trace = false;
    private static LinkedList<NodeStatistics> nodes = new LinkedList<NodeStatistics>();
    private static int totalCPUs = 0;
    private static PrintWriter fileWriter; 
    private static final String intend = " ";
    
    /*
     * when all the nodes are register this must be called
     */
    public static void start(String filename){
	System.out.println("Initializing NodeStatRecorder with " + nodes.size() + " nodes");
	if (trace){
        	try {
        	    fileWriter = new PrintWriter(filename, "UTF-8");
        	    fileWriter.println( getHeader());
        	    System.out.println("NodeStatRecorder initialized"); 
        	} catch (FileNotFoundException e) {
        	    // TODO Auto-generated catch block
        	    e.printStackTrace();
        	} catch (UnsupportedEncodingException e) {
        	    // TODO Auto-generated catch block
        	    e.printStackTrace();
        	}
	}
    }
    
    public static void close(){
	if (fileWriter != null){
	    fileWriter.close();
	}	
	System.out.println("NodeStatRecorder exited"); 
    }
    
    public static void registerNode(int id, String name, int nCPUs, boolean isInputDestination){
	System.out.println("adding node:  " + id + " " + name + " " + nCPUs + " " + isInputDestination); 
	nodes.add(new NodeStatistics(id, name, nCPUs, isInputDestination));
	if (isInputDestination){
	    totalCPUs += nCPUs;
	}	
    }
    
    private static NodeStatistics findNode(int id){
	for (NodeStatistics node: nodes){
	    if (node.nodeId == id){
		return node;
	    }
	}
	return null;
    }
    
    private static String getHeader(){
	StringBuffer buf = new StringBuffer();
	buf.append("time" + intend);
	buf.append("TOTAL");
	for (NodeStatistics n : nodes){
	    if (n.isInputDestination){ //to do: filter links here
		buf.append(intend + n.nodeName );
	    }	    
	}	
	return buf.toString();
    }
    
    private static String getStatusString(){
	double usedCPUs = 0;
	StringBuffer buf = new StringBuffer();	
	for (NodeStatistics n : nodes){
	    if (n.isInputDestination){ //to do: filter links here
		buf.append(intend + n.getCpuUsage() );
		usedCPUs += n.getBusyCPUs();
	    }	    
	}
	double totalUsage = (double) usedCPUs / (double) totalCPUs;
	
	return GridSim.clock() + intend + totalUsage + buf.toString();
    }

    public static void updateCpuUsage(int id, int busyCPUs){
	NodeStatistics node = findNode(id);
	if (node == null){
	    return;
	}
	node.setBusyCPUs(busyCPUs);
	if (trace) {
	    fileWriter.println( getStatusString());
	}

    }
    
    public static int getregisteredNodesNum(){
	return nodes.size();
    }
    
    


}
