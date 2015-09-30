package flow_model;

/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * License:      GPL - http://www.gnu.org/copyleft/gpl.html
 */

import eduni.simjava.Sim_event;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.GridUser;
import gridsim.GridletList;
import gridsim.IO_data;
import gridsim.ResourceCharacteristics;
import gridsim.net.InfoPacket;
import gridsim.net.SimpleLink;  // To use the new flow network package - GridSim 4.2
import gridsim.util.SimReport;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import networkflows.planner.CompNode;
import networkflows.planner.DataProductionPlanner; //My planner model
import networkflows.planner.NetworkLink;

/**
 * This class defines a user which submits raw data for processing in 
 * network-flow model.
 * @author Uros Cibej and Anthony Sulistio
 * @author Dzmitry Makatun
 */
class User extends GridUser {
    private String name_;
    private int myId_;
    private int totalResource;
    private int resourceID[];
    String resourceName[];
    double totalPEs = 0;
    private int totalGridlet; //how many gridlet were red from file
    private GridletList gridlets;          // list of submitted Gridlets
    private SimReport report_;  // logs every events
    boolean trace_flag;
    double startTime, saturationStart, saturationFinish, finishTime ;
    int chunkSize; //for monitoring
    private LinkedList <LinkFlows> newPlan;
    private DataProductionPlanner solver;
    int deltaT;
    float beta;	
    boolean continueDataProduction;
    private int updateCounter = 0;
    

    // constructor
    User(String name, double baud_rate, double delay, int MTU) throws Exception {
    	
    	//super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));

        // NOTE: uncomment this if you want to use the new Flow extension
        super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));
    	
        // creates a report file
        if (trace_flag == true) {
            report_ = new SimReport(name);
        }
    	
    	trace_flag = true;
        this.name_ = name;

        // Gets an ID for this entity
        this.myId_ = super.getEntityId(name);
        //write("Creating a grid user entity with name = " +
        //      name + ", and id = " + this.myId_);
      
    }
    
    //get list of gridlets for submission
    public void setGridletList(GridletList gridlets){
    	this.gridlets = gridlets;  
    	this.totalGridlet = this.gridlets.size();
    	chunkSize = totalGridlet / 20; //for monitoring
    }

    public String toStringShort(){
    	StringBuffer br = new StringBuffer();    	
    	br.append("name: " + this.get_name() + ", ");
    	br.append("id: " + this.get_id() + ", ");
    	br.append("totalGridlets: " + this.totalGridlet + ", ");
    	
    	return br.toString();
    }
    
    private void initPlaner(){
	        //INITIALIZE PLANER
		this.deltaT = ParameterReader.deltaT;
		this.beta =  (float) ParameterReader.beta;	
		solver = new DataProductionPlanner(ParameterReader.planerLogFilename, deltaT, beta);
		for(CompNode node : ResourceReader.planerNodes){
		    solver.addNode(node);
		}
		for(NetworkLink link : DPNetworkReader.planerLinks){
		    solver.addLink(link);
		}	
		//solver.PrintGridSetup();	
		
	        // This to give a time for GridResource entities to register their
	        // services to GIS (GridInformationService) entity.
	        super.gridSimHold(1000.0);
	        System.out.println(name_ + ": retrieving GridResourceList");
	        LinkedList resList = super.getGridResourceList();
	        
	        // initialises all the containers
	        this.totalResource = resList.size();
	        write(name_ + ": obtained " + totalResource + " resource IDs in GridResourceList");
	        this.resourceID = new int[this.totalResource];
	        this.resourceName = new String[this.totalResource];
	        
	        //get characteristics of resources
	        for (int i = 0; i < totalResource; i++)
	        {
	            // Resource list contains list of resource IDs not grid resource
	            // objects.
	            resourceID[i] = ( (Integer)resList.get(i) ).intValue();
	        }
	        
	        this.continueDataProduction = true;
    }
    
    
    /**
     * The core method that handles communications among GridSim entities.
     */
    public void body() {
	int planningIteration = 0;
	
	initPlaner();
	
	Sim_event ev = new Sim_event();
	Map status;
	
	while (continueDataProduction){
	    write("######################## planningIteration: " + planningIteration + " #######################");
	    planningIteration ++;
	    /////////////////////////////////////////////////
	    //GET resource statuses
	    requestStatuses();
	    
	   //receive responses	   
	    this.continueDataProduction = false; //re-check the condition
	   for(int i = 0; i <  totalResource; i++){ 
	       super.sim_get_next(ev);
	       if (ev.get_tag() == RiftTags.STATUS_RESPONSE){
		  //if any node has work to do will set to true
		   if (processStatusResponce(ev)) {this.continueDataProduction = true;}
	       }else{
		   write("Received wrong message: " + ev.get_tag());
	       }
	       
	   } 
	   
	   //create plan
	   //new plan sets continueDataProduction
	   newPlan = createNewPlan();
	        
	        
	   //send plan
	   for(int i = 0; i <  totalResource; i++){          
	       write("sending new plan to resource " + resourceID[i]);
	       //send without network delay
	       super.sim_schedule(resourceID[i], GridSimTags.SCHEDULE_NOW, RiftTags.NEW_PLAN, newPlan);
	   } 
	    
	    
	    super.gridSimHold(deltaT);
	}
	
	finish();
    }
    
    private void finish(){
	

       //ping resources
       //pingAllRes(resourceID);

	
        // don't forget to close the file
        if (report_ != null) {
            report_.finalWrite();
        }   
	
        ////////////////////////////////////////////////////////
        // shut down I/O ports
        shutdownUserEntity();
        terminateIOEntities();
        System.out.println(this.name_ + ":%%%% Exiting body() at time " +
            GridSim.clock());
    }
    
    
    /**
     * sends requests to all resources for statuses
     * cleans old data from the solver
     */
    private void requestStatuses(){
	solver.clean(); //cleans old data from the solver
	//send requests
        for(int i = 0; i <  this.totalResource; i++){ 
                     
            write("sending status request to resource " + this.resourceID[i]);
            //send without network delay
            super.sim_schedule(this.resourceID[i], GridSimTags.SCHEDULE_NOW, RiftTags.STATUS_REQUEST, this.myId_);
            //send(super.output, GridSimTags.SCHEDULE_NOW, RiftTags.STATUS_REQUEST,
                 //new IO_data(this.myId_, 0, this.resourceID[i], 0) 
            //);
          } 
        
    }
    
    /**
     * processes status responses from resources, updates grid information for solver
     * @param ev
     * @return if this node has more tasks to do
     */
    private boolean processStatusResponce(Sim_event ev){	
	//write("received status response" + statusToString(status));
	//extract data
	Map status;
	status = (HashMap) ev.get_data();
	//write(statusToString(status));
	int id = (Integer) status.get("nodeId");
	String name = (String) status.get("nodeName");
	boolean isInputSource = (Boolean) status.get("isInputSource");
	boolean isOutputDestination = (Boolean) status.get("isOutputDestination");
	boolean isInputDestination = (Boolean) status.get("isInputDestination");
	boolean isOutputSource = (Boolean) status.get("isOutputSource");
	double waitingInputSize = (Double) status.get("waitingInputSize");
	double readyOutputSize = (Double) status.get("readyOutputSize");
	double freeStorageSpace = (Double) status.get("freeStorageSpace");
	int submittedInputFiles = (Integer) status.get("submittedInputFiles");

	//will check if there is unprocessed input
	//or untransferred output
	boolean hasMoreWorkToDo = false;
	if (waitingInputSize > 0 //if any node has waiting input files
		|| submittedInputFiles > 0 //if jobs are running
		|| (! isOutputDestination && readyOutputSize > 0) ){ //if not a destination node has ready output files
	    hasMoreWorkToDo = true;
	}

        
	
	if ( solver.updateNode(id, (long) waitingInputSize, (long) readyOutputSize,
		(long) waitingInputSize, (long) freeStorageSpace) ){
	    //this.updateCounter++;
	    write("updated status of node " + id + ":" + name);
	}else{
	    write("WARNING failed to update node status (probably node not found)"  + id + ":" + name);
	}      
	return hasMoreWorkToDo;
    }
    
    
    /** generates new transfer plan
     * The planner logic is connected here 
     * @return
     */
    private LinkedList<LinkFlows> createNewPlan() {  
	double sumFlow = 0;
	//solve the problem
	sumFlow = solver.solve();
	
	//if there is no flow - stop planning
	if (sumFlow == 0){
	    //this.continueDataProduction = false;
	    //write("All calculated flows are zeros. Planer will stop.");
	}
	solver.PrintGridSetup();
	
	//parse the solution
        LinkedList<LinkFlows> plan = new LinkedList<LinkFlows>();
        LinkFlows tempFlow;
        
        //get flows of real network links
        for (NetworkLink link : solver.getGridLinks()){
            tempFlow = new LinkFlows(link.getBeginNodeId(), 
        	    link.getEndNodeId(), link.getInputFlow(), link.getOutputFlow());
            plan.add(tempFlow);
        }
        
        //get flows of dummy links
        for (CompNode node : solver.getGridNodes()){
            tempFlow = new LinkFlows(node.getId(), 
        	    -1, node.getNettoInputFlow(), 0);
            plan.add(tempFlow);
        }

        //printPlan(plan);
 	return plan;
     }
    
    private void printPlan(LinkedList<LinkFlows> plan){
	write("-------------New plan created----------------");	
	for (LinkFlows tempFlow : plan){
	    write(tempFlow.toString() );
	}	
	write("----------------------------------------------");
    }

   /** generates static plan for testing purposes
    * @return
    */
   private LinkedList<LinkFlows> createTestPlan(int[] resourceID) {
       double defaultLocalProcessingFlow = 20000;
       double defaultInputFlow = 10000;
       double defaultOutputFlow = 100000;
              
       LinkedList<LinkFlows> plan = new LinkedList();
       LinkFlows tempFlow;
       int centralStorageID = GridSim.getEntityId("RCF");
       for (int i = 0; i < resourceID.length; i++ ){
	   plan.add(new LinkFlows(resourceID[i], -1, defaultLocalProcessingFlow, 0)); // add local processing flow
	   if (centralStorageID == resourceID[i]){
	       continue;
	   }
	   plan.add(new LinkFlows(centralStorageID, resourceID[i], defaultInputFlow, 0)); // input flow from user to resources
	   plan.add(new LinkFlows(resourceID[i], centralStorageID, 0, defaultOutputFlow)); // flow back to user
	   for (int j = 0; j < resourceID.length; j++ ){
	       if ( i != j ){
		   if (resourceID[i] < resourceID[j] ){
		       // input flow to other resources
		       //plan.add(new LinkFlows(resourceID[i], resourceID[j], defaultInputFlow, 0));		       
		   }else{
		       //output flow to other resources
		       //plan.add(new LinkFlows(resourceID[i], resourceID[j], 0, defaultOutputFlow));
		   }
	       }
	   }
       }
       
	return plan;
    }
   
   

private String statusToString(Map status) {
       StringBuffer buf = new StringBuffer();
       
       buf.append("id: " + status.get("nodeId") + " ");
       buf.append("name: " + status.get("nodeName") + " ");
       buf.append("isInputSource: " + status.get("isInputSource") + " ");
       buf.append("isOutputDestination: " + status.get("isOutputDestination") + " ");
       buf.append("waitingInputSize: " + status.get("waitingInputSize") + " ");
       buf.append("readyOutputSize: " + status.get("readyOutputSize") + " ");
       buf.append("freeStorageSpace: " + status.get("freeStorageSpace") + " ");
     
	return buf.toString();
    }



private void pingAllRes(int[] resIDs){
    for (int id: resIDs){
	      pingRes(id);
	  }
}


private void pingRes(int resourceID){
	// ping functionality
       InfoPacket pkt = null;
       int size = 1024 * 1024; // 1 MB

       // There are 2 ways to ping an entity:
       // a. non-blocking call, i.e.
       //super.ping(resourceID[index], size);    // (i)   ping
       //super.gridSimHold(10);        // (ii)  do something else
       //pkt = super.getPingResult();  // (iii) get the result back

       // b. blocking call, i.e. ping and wait for a result
       pkt = super.pingBlockingCall(resourceID, size);

       // print the result
       write("\n-------- " + name_ + " ----------------");
       write(pkt.toString());
       write("-------- " + name_ + " ----------------\n");
       
   }

   

    /**
     * Prints out the given message into stdout.
     * In addition, writes it into a file.
     * @param msg   a message
     */
    private void write(String msg)
    {
	StringBuffer buf = new StringBuffer();
	buf.append(GridSim.clock() + " ");
	buf.append( super.get_name() + ":" );
	buf.append(super.get_id() + " ");	    
	buf.append(msg);
	
        System.out.println(buf.toString());
        if (report_ != null) {
            report_.write(msg);
        }
    }
} // end class


    
