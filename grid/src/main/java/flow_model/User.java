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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
public class User extends GridUser {
    private String name_;
    private int myId_;
    private int totalResource;
    private int resourceID[];
    String resourceName[];
    double totalPEs = 0;
    private int totalGridlet; //how many gridlet were red from file
    private GridletList gridlets;          // list of submitted Gridlets
    //private static FileWriter report_;  // logs every events
    boolean trace_flag;
    double startTime, saturationStart, saturationFinish, finishTime ;
    int chunkSize; //for monitoring
    private LinkedList <LinkFlows> newPlan;
    private DataProductionPlanner solver;
    int deltaT;
    float beta;	
    boolean continueDataProduction;
    private int updateCounter = 0;
    //writing statistics to a file
    private PrintWriter fileWriter; 
    

    // constructor
    public User(String name, double baud_rate, double delay, int MTU) throws Exception {
    	
    	//super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));

        // NOTE: uncomment this if you want to use the new Flow extension
        super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));
    	

    	
    	trace_flag = true;
        this.name_ = name;
        
        // creates a report file
        if (trace_flag == true) {
            //report_ = new FileWriter(ParameterReader.simulationLogFilename, true);
        }

        // Gets an ID for this entity
        this.myId_ = super.getEntityId(name);
        //write("Creating a grid user entity with name = " +
        //      name + ", and id = " + this.myId_);
        
        //write statistics for planned link bandwith consumprion
        String filename = "output/" + this.name_ + "_planned_net_usage.csv";
	fileWriter = new PrintWriter(filename, "UTF-8");

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
	
	        //INITIALIZE PLANER and FlowManager
		this.deltaT = ParameterReader.deltaT;
		this.beta =  (float) ParameterReader.beta;	
		solver = new DataProductionPlanner(ParameterReader.planerLogFilename, deltaT, beta);
		LinkedList<LinkFlows> allLinkFlows = new LinkedList<LinkFlows>();
		for(CompNode node : ResourceReader.planerNodes){
		    solver.addNode(node);
		    allLinkFlows.add(new LinkFlows(-node.getId(), "dummy_" + node.getName(), -1, node.getId(), -1));
		}
		for(NetworkLink link : DPNetworkReader.planerLinks){
		    solver.addLink(link);
		    allLinkFlows.add(
			    new LinkFlows(link.getId(), link.getName(), link.getBandwidth(), link.getBeginNodeId(), 
		        	    link.getEndNodeId() )  );
		}	
		//solver.PrintGridSetup();
		solver.WriteGridODT("output/grid.dot");
		FlowManager.setLinks(allLinkFlows);
		
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
	        //write header to the statistics file
		fileWriter.println(getStatusHeader() );
		//for CPU usage monitoring
	        while(NodeStatRecorder.getregisteredNodesNum() != totalResource){
	            write("waitenig for all res to register");
	            super.gridSimHold(100.0);
	        }
		NodeStatRecorder.init("output/CpuUsage.csv");
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

	
        ////////////////////////////////////////////////////////
        // shut down I/O ports
	
        shutdownUserEntity();
        FlowManager.initialized = false;
        terminateIOEntities();
        System.out.println(this.name_ + ":%%%% Exiting body() at time " +
            GridSim.clock());
        fileWriter.close();
        NodeStatRecorder.close();
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
	double storageSize = (Double) status.get("storageSize");
	double submittedInputSize = (Double) status.get("submittedInputSize");
	int busyCPUS = (Integer) status.get("busyCPUS");
	double reservedOutputSize = (Double) status.get("reservedOutputSize");
	
	double createdOutput = (Double) status.get("createdOutput");
	double processedInput = (Double) status.get("processedInput");

	
	//will check if there is unprocessed input
	//or untransferred output
	boolean hasMoreWorkToDo = false;
	if (waitingInputSize > 0 //if any node has waiting input files
		//|| submittedInputSize > 0 //if jobs are running
		|| (! isOutputDestination &&  freeStorageSpace != storageSize) ) { //if not a destination node has ready output files // readyOutputSize > 0)
	    hasMoreWorkToDo = true;
	}

	
	if ( solver.updateNode(id, (long) waitingInputSize, (long) readyOutputSize,
		(long) waitingInputSize, (long) freeStorageSpace, busyCPUS,
		(long) freeStorageSpace, (long) submittedInputSize, (long) reservedOutputSize, 
		processedInput, createdOutput )){
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
        //LinkedList<LinkFlows> plan = new LinkedList<LinkFlows>();
        LinkFlows tempFlow;
        
        //get and set flows of real network links 
        for (NetworkLink link : solver.getGridLinks()){
            FlowManager.getLinkFlows(link.getId()).setFlows(link.getInputFlow(), link.getOutputFlow());
        }
        
        //get and set flows of dummy links
        for (CompNode node : solver.getGridNodes()){
            FlowManager.getLinkFlows(-node.getId()).setFlows(node.getNettoInputFlow(), 0);
        }

        //printPlan(plan);
        //record statistics
        fileWriter.println(getLinkPlannedUsage());
 	return FlowManager.getLinks();
     }
    
    /**
     * returnes string with names of all links
     * @return
     */
    private String getStatusHeader() {
	String delimiter = " ";
	StringBuffer buf = new StringBuffer();
	buf.append("time");
        //get names real network links
        for (NetworkLink link : solver.getGridLinks()){
            buf.append(delimiter + link.getName() + "_INPUT");
            buf.append(delimiter + link.getName()+ "_OUTPUT");
        }            
	return buf.toString();
    }
    
    private String getLinkPlannedUsage(){
	String delimiter = " ";
	StringBuffer buf = new StringBuffer();
	buf.append(GridSim.clock());
        for (NetworkLink link : solver.getGridLinks()){           
            buf.append(delimiter + ( link.getInputFlow() / (link.getBandwidth()*deltaT) ) );
            buf.append(delimiter + ( link.getOutputFlow() / (link.getBandwidth()*deltaT) ) );
        } 	
	return buf.toString();
    }
    
    private void printPlan(LinkedList<LinkFlows> plan){
	write("-------------New plan created----------------");	
	for (LinkFlows tempFlow : plan){
	    write(tempFlow.toString() );
	}	
	write("----------------------------------------------");
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
	//print to screen
        System.out.println(buf.toString());
        //write to file
        Logger.write(buf.toString());
    }

} // end class


    
