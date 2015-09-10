package flow_model;

import gridsim.*;
import gridsim.datagrid.*;

/**
 * extends DataGridlet for network-flow model simulations
 * @author Dzmitry Makatun
 */
public class DPGridlet extends DataGridlet{

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
		br.append("inSize: " + this.getGridletFileSize() + "(bytes), ");
		br.append("outSize: " + this.getGridletFileSize() + "(bytes), ");
		
		
		return br.toString();
	}
	
	public double getInputSizeInUnits(){
	    return super.getGridletFileSize() / DataUnits.getSize();
	}
	
	public double getOutputSizeInUnits(){
	    return super.getGridletOutputSize() / DataUnits.getSize();
	}

}
