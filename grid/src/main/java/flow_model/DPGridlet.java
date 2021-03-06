package flow_model;

import gridsim.*;
import gridsim.datagrid.*;

/**
 * extends DataGridlet for network-flow model simulations
 * @author Dzmitry Makatun
 */
public class DPGridlet extends Gridlet{
       private int senderID; //the previous resource, that send this gridlet
       private LinkFlows usedLink;//the link which was used for the last transfer

	public DPGridlet(int id, double length, long inputSize, long outputSize, boolean trace) {
		super(id, length, inputSize, outputSize, trace);
		// TODO Auto-generated constructor stub
	}
	
	
	/**
	 * provides summary about the DPGridlet as a string
	 */
	public String toStringShort(){
		StringBuffer br = new StringBuffer();
		br.append("id: " + this.getGridletID() + ", ");
		br.append("length: " + this.getGridletLength() + ", ");
		//br.append("remaining_length: " + super.getRemainingGridletLength() + ", ");
		
		br.append("inSize: " + this.getGridletFileSize());
		br.append(" (" + DataUnits.getName() + ") ");
		//br.append("inSizeUnits: " + this.getInputSizeInUnits() + DataUnits.getName() + " ");
		br.append("outSize: " + this.getGridletOutputSize());
		br.append(" (" + DataUnits.getName() + ") ");
		//br.append("outSizeUnits: " + this.getOutputSizeInUnits() + DataUnits.getName()+ " ");
		//br.append("netServiceLevel: " + this.getNetServiceLevel()+ " ");
		
		
		return br.toString();
	}
	
	@Deprecated
	public double getInputSizeInUnits(){
	    return this.getGridletFileSize();
	    //return super.getGridletFileSize() / DataUnits.getSize();
	}
	
	@Deprecated
	public double getOutputSizeInUnits(){
	    return this.getGridletOutputSize();
	    //return super.getGridletOutputSize() / DataUnits.getSize();
	}


	public int getSenderID() {
	    return senderID;
	}


	public void setSenderID(int senderID) {
	    this.senderID = senderID;
	}


	public LinkFlows getUsedLink() {
	    return usedLink;
	}


	public void setUsedLink(LinkFlows usedLink) {
	    this.usedLink = usedLink;
	}

}
