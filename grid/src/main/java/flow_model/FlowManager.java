package flow_model;

import java.util.LinkedList;

public class FlowManager {
    public static boolean initialized = false;
    private static LinkedList<LinkFlows> links;
    private static String intend = " ";

    public static void setLinks(LinkedList<LinkFlows> newlinks){
	links = newlinks;
	initialized = true;
    }
    
    public static LinkedList<LinkFlows> getLinks(){
	return links;
    }
    
    public static void resetCounters(){
	for (LinkFlows l : links){
	    l.resetCounters();
	}
    }
    
    public static String getNamesLine(){
	StringBuffer buf = new StringBuffer();
	for (LinkFlows l : links){
	    if (!l.name.startsWith("dummy_")){ //to do: filter links here
		buf.append(l.name + "_INPUT" + intend);
		buf.append(l.name + "_OUTPUT"+ intend);
	    }	    
	}	
	return buf.toString();
    }
    
    public static String getConsumptionLine(double timeInterval){
	StringBuffer buf = new StringBuffer();
	for (LinkFlows l : links){
	    if (!l.name.startsWith("dummy_")){//to do: filter links here
		buf.append( l.inputSent / (l.bandwidth * timeInterval)  + intend);
		buf.append( l.outputSent / (l.bandwidth * timeInterval)  + intend);           
	    }	    
	}	
	return buf.toString();
    }
    
    public static LinkFlows getLinkFlows(int id){
	for (LinkFlows l : links){
	    if (l.id == id){
		return l;
	    }
	}
	return null;
    }
    
    public static LinkFlows getLinkFlows(int fromID, int toID){
	for (LinkFlows l : links){
	    if (l.fromID == fromID && l.toID == toID){
		return l;
	    }
	}
	return null;
    }
    
}
