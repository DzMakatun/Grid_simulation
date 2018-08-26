package flow_model;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class LFNrecord {
    //FILE types
    /** The input file is qued initial storage*/
    public static final int INPUT = 100; 
    /** The input file is send out from storage for processing*/
    public static final int OUTPUT = 101;
    
    //STATUSES
    /** The input file is qued initial storage*/
    public static final int QUEUED = 0; 
    /** The input file is send out from storage for processing*/
    public static final int SEND_OUT = 1;
    
    private int id;
    private Set<String> sites;
    private int replicasNumber;
    private int type;
    private int status;
    
    
    /**
     * creates a first record of a file, based on data of its first replica
     */
    public LFNrecord(int id, String site , int type, int status) {
	sites = new HashSet<String>();	
	this.id = id;
	sites.add(site);
	this.replicasNumber = 1;
	this.type = INPUT;
	this.status = QUEUED;
    }
    
    public synchronized boolean addReplica(String site){
	if (sites.add(site)) { //if this replica is not already registered
	    replicasNumber++;
	    return true;
	}else{
	    System.out.println("Error: file " + this.id + " already registered at " + site );
	    return false;
	}
	
    }   


    /**
     * @return the replicasNumber
     */
    public synchronized int getReplicasNumber() {
        return replicasNumber;
    }
    /**
     * @param replicasNumber the replicasNumber to set
     */
    public void setReplicasNumber(int replicasNumber) {
        this.replicasNumber = replicasNumber;
    }
    /**
     * @return the id
     */
    public int getId() {
        return id;
    }
    /**
     * @return the sites
     */
    public Set<String> getSites() {
        return sites;
    }
    /**
     * @return the type
     */
    public int getType() {
        return type;
    }

    /**
     * @return the status
     */
    public synchronized int getStatus() {
        return status;
    }
    
    /**
     * @param status the status to set
     */
    public synchronized void setStatus(int status) {
        this.status = status;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
	return "LFNrecord [id=" + id + ", sites=" + sites
		+ ", replicasNumber=" + replicasNumber + ", type=" + type
		+ ", status=" + status + "]";
    }
    
    
}
