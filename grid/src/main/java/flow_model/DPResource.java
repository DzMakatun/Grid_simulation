package flow_model;

import gridsim.*;
import gridsim.datagrid.*;
import gridsim.net.Link;

public class DPResource extends DataGridResource{

	public DPResource(String name, Link link, ResourceCharacteristics resource, ResourceCalendar calendar, ReplicaManager replicaManager) throws Exception {
		super(name, link, resource, calendar, replicaManager);
		// TODO Auto-generated constructor stub
	}

	public String paramentersToString(){
		StringBuffer br = new StringBuffer();
		ResourceCharacteristics characteristics = super.getResourceCharacteristics();
		
		
		br.append("name: " + super.get_name() + ", ");
		br.append("id: " + super.get_id() + ", ");
		br.append("PEs: " + characteristics.getMachineList().getNumPE() + ", ");
		br.append("storage: " + super.getTotalStorageCapacity() + "(MB), ");
		
		br.append("processingRate: " +characteristics.getMIPSRatingOfOnePE()  + ", ");
		//br.append("processingRate: " +characteristics.  + ", ");
		
		br.append("link bandwidth: " + super.getLink().getBaudRate()  + "(bit/s), ");
		
		br.append("localRC: " + super.hasLocalRC() + ", ");
		//br.append("stat: " + super.get_stat().toString());
		
		return br.toString();
		
	}
}
