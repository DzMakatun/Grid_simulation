package flow_model;


public class LinkFlows {
    public int id;
    public String name;
    public double bandwidth;
    public int fromID;
    public int toID;
    public double inputFlow;
    public double outputFlow;
    public double inputSent;
    public double outputSent;
    
    public LinkFlows(int id, String name, double bandwidth,  int fromID, int toID){
	this.id = id;
	this.name =name;
	this.bandwidth = bandwidth;
	this.fromID = fromID;
	this.toID = toID;
	this.inputSent = 0;
	this.outputSent = 0;
    }
    
    public void setFlows(double inputFlow, double outputFlow){
	this.inputFlow = inputFlow; 
	this.outputFlow = outputFlow;
    }
    
    public void resetCounters(){
	this.inputSent = 0;
	this.outputSent = 0;
    }
    
    /**
     * counts how much input data was transferred after last reset
     * @param filesize
     */
    public void addInputTransfer(double filesize){
	this.inputSent += filesize;
	//System.out.println("InputSent: " + inputSent);
    }
    
    /**
     * counts how much output data was transferred after last reset
     * @param filesize
     */
    public void addOutputTransfer(double filesize){
	this.outputSent += filesize;
	//System.out.println("Output send: " + outputSent);
    }
    
    @Override
    public String toString(){
	StringBuffer br = new StringBuffer();	
	br.append("fromID: " + this.fromID + " ");
	br.append("toID: " + this.toID + " ");
	br.append("inputFlow: " + this.inputFlow + " ");
	br.append("outputFlow: " + this.outputFlow);
	
	return br.toString();	
    }

}
