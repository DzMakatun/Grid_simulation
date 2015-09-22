package flow_model;



public class LinkFlows {
    public int fromID;
    public int toID;
    public double inputFlow;
    public double outputFlow;
    
    public LinkFlows(int fromID, int toID, double inputFlow, double outputFlow){
	this.fromID = fromID;
	this.toID = toID;
	this.inputFlow = inputFlow;
	this.outputFlow = outputFlow;	
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
