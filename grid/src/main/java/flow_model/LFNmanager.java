package flow_model;

import java.util.LinkedHashMap;
import java.util.Map;

public class LFNmanager {
    private static Map<Integer, LFNrecord> LFNlist = new LinkedHashMap<Integer, LFNrecord>();
    
    public static synchronized void registerInputFile(int id, String site){
	LFNrecord record = LFNlist.get(id);
	if (record != null){
	    //file already registered - add replica
	    record.addReplica(site);
	}else{	    
	    //the first known replica of the file
	    LFNlist.put(id, new LFNrecord(id, site, LFNrecord.INPUT, LFNrecord.QUEUED ));
	}
	
    }
    
    public static boolean AAAregisterFileSend(int id){
	LFNrecord record = LFNlist.get(id);
	if (record != null && record.getStatus() != LFNrecord.SEND_OUT ){
	     record.setStatus(LFNrecord.SEND_OUT);;
	     return true;
	}else{//file not registered or already send
	    return false;
	}
    }
    
    public static synchronized Integer getNumberOfReplicas(int id){
	LFNrecord record = LFNlist.get(id);
	if (record != null){
	    return record.getReplicasNumber();
	}else{//file not registered
	    return 0;
	}
    }
    
    public static boolean AAAfileIsAlreadySendOut(int id){
	LFNrecord record = LFNlist.get(id);
	if (record != null){
	    return record.getStatus() == LFNrecord.SEND_OUT;
	}else{//file not registered
	    return false;
	}
    }
    
    public static synchronized boolean checkAndChange(int id){
	LFNrecord record = LFNlist.get(id);
	if (record != null){
	    if (record.getStatus() == LFNrecord.QUEUED){
		record.setStatus(LFNrecord.SEND_OUT);
		return true;
	    }else{
		//System.out.println(id + "file send before");
		//System.out.println(record.toString());
	    }
	}else{
	    System.out.println("file not found in LFN database");
	}
        return false;
    }
    
    public static synchronized boolean unCheckFile(int id){
	LFNrecord record = LFNlist.get(id);
	if (record != null){
	    record.setStatus(LFNrecord.QUEUED);
	    return true;
	}
        return false;
    }
    
    public static void printAllLFNstatuses(){
	for(LFNrecord record : LFNlist.values() ){
	    System.out.println(record.toString());
	}
    }
    
}
