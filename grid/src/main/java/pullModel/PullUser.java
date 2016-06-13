package pullModel;

import java.util.LinkedList;

import flow_model.DataUnits;
import flow_model.FlowManager;
import flow_model.Logger;
import flow_model.NodeStatRecorder;
import flow_model.RiftTags;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.GridUser;
import gridsim.ResourceCharacteristics;
import gridsim.net.SimpleLink;

public class PullUser extends GridUser {
    
    private boolean trace_flag;
    private String name_;
    private int myId_;
    private boolean displayMessages = true;
    LinkedList<Integer> resIds;

    // constructor
    public PullUser(String name, double baud_rate, double delay, int MTU) throws Exception {
        super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));    	
    	trace_flag = GridSim.isTraceEnabled();
        this.name_ = name;        
        this.myId_ = super.getEntityId(name);
    }
    
    private void initPullUser(){
        // This to give a time for GridResource entities to register their
        // services to GIS (GridInformationService) entity.
        super.gridSimHold(1000.0);
        write(" retrieving GridResourceList");
        this.resIds = super.getGridResourceList();        
        ResourceCharacteristics resChar;
        LinkedList<ResourceCharacteristics> nodes = new LinkedList<ResourceCharacteristics>();	        
        //get characteristics of resources
        int resID;
        for (Object res: this.resIds) {
            resID = ( (Integer)res ).intValue();
            // Resource list contains list of resource IDs not grid resource
            // objects.
            // Requests to resource entity to send its characteristics
            super.send(resID, GridSimTags.SCHEDULE_NOW,
                       GridSimTags.RESOURCE_CHARACTERISTICS, this.myId_);
            Object obj = super.receiveEventObject();
            resChar = (ResourceCharacteristics) obj; //super.receiveEventObject();
            write("Received ResourceCharacteristics from " +
        	    resChar.getResourceName() + ", with id = " + resChar.getResourceID() + " with  " + resChar.getNumPE() + " PEs");
            NodeStatRecorder.registerNode(resChar.getResourceID(), resChar.getResourceName() , (int) resChar.getNumPE(), resChar.getNumPE() != 1);
            }	           
        
        String nodeStatFilename = "output/" + DataUnits.getPrefix() + "PULL_CpuUsage.csv";
	NodeStatRecorder.start(nodeStatFilename);
    }
    
    
    public void body() {
	initPullUser();
	super.gridSimHold(1000.0);
	
	//send to all resources to init data production
	for(int reId: this.resIds){
	    super.send(reId, GridSimTags.SCHEDULE_NOW,
                    RiftTags.INIT, this.myId_);
	}
	
	super.gridSimHold(5000.0);
	
	//send to all resources to start data production
	for(int reId: this.resIds){
	    super.send(reId, GridSimTags.SCHEDULE_NOW,
                    RiftTags.START, null);
	}
	
	int reportedResources = 0;
	while(reportedResources < this.resIds.size()){
	    Integer res =  (Integer) super.receiveEventObject();
	    reportedResources++;
	    write("resource: " + res + " has no more tasks ( " + reportedResources + " / " + this.resIds.size() + " )");	    
	}
	
	finish();
    }
    
    private void finish(){
            write("Finishing!");
            write("Shutdown entity: "+ GridSim.getGridSimShutdownEntityId());
            write("GIS entity: "+ GridSim.getGridInfoServiceEntityId());
            write("Statistics entity: "+ GridSim.getGridStatisticsEntityId());
            
            shutdownUserEntity();
            terminateIOEntities();            
            FlowManager.initialized = false;
            //close global CPU monitor
            NodeStatRecorder.close();  
            System.out.println(this.name_ + ":%%%% Exiting body() at time " +
            GridSim.clock());      
            write("Shutdown entity: "+ GridSim.getGridSimShutdownEntityId());
    }   

	private void write(String message){
	    String indent = " ";	    
            StringBuffer buf = new StringBuffer();
            buf.append(GridSim.clock() + " ");
            buf.append( super.get_name() + ":" );
            buf.append(super.get_id() + " ");	    
            buf.append(message);
            if (displayMessages ){
		//print to screen
	        System.out.println(buf.toString());
            }
	   //write to file
	   Logger.write(buf.toString());	   
	}
    
}
