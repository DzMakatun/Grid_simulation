package flow_model;

import gridsim.*;
import gridsim.datagrid.*;

public class DPGridlet extends DataGridlet{

	public DPGridlet(int id, double length, long inputSize, long outputSize, boolean trace) {
		super(id, length, inputSize, outputSize, trace);
		// TODO Auto-generated constructor stub
	}
	
	public String toStringShort(){
		StringBuffer br = new StringBuffer();
		br.append("id: " + this.getGridletID() + ", ");
		br.append("length: " + this.getGridletLength() + ", ");
		br.append("inSize: " + this.getGridletFileSize() + "(bytes), ");
		br.append("outSize: " + this.getGridletFileSize() + "(bytes), ");
		
		
		return br.toString();
	}

}
