package flow_model;

public class NodeStatistics {
    public int nodeId;
    public String nodeName;
    public int nCPUs;
    public int busyCPUs;
    public boolean isInputDestination;
    
    
    public int getBusyCPUs() {
        return busyCPUs;
    }

    public NodeStatistics(int nodeId, String nodeName, int nCPUs, boolean isInputDestination) {
	super();
	this.nodeId = nodeId;
	this.nodeName = nodeName;
	this.nCPUs = nCPUs;
	this.isInputDestination = isInputDestination;
    }
    
    public void setBusyCPUs(int busyCPUs){
	this.busyCPUs = busyCPUs;
    }
    
    public double getCpuUsage(){
	return (double) busyCPUs / (double) nCPUs;
    }
    
    

}
