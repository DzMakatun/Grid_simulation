package push_model;

/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * License:      GPL - http://www.gnu.org/copyleft/gpl.html
 */

import flow_model.*;
import eduni.simjava.Sim_event;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.GridUser;
import gridsim.Gridlet;
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
public class PushUser extends GridUser {
    private String name_;
    private int myId_;
    private int totalResource;
    private int resourceID[];
    String resourceName[];
    double resourcePEs[];
    double totalPEs = 0;
    private int totalGridlet; //how many gridlet were red from file
    private GridletList gridlets,receiveList_;          // list of submitted Gridlets
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
    private int storageId;
    

    // constructor
    public PushUser(String name, double baud_rate, double delay, int MTU) throws Exception {
    	
    	//super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));

        // NOTE: uncomment this if you want to use the new Flow extension
        super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));
    	
    	trace_flag = GridSim.isTraceEnabled();
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
        //String filename = "output/" + this.name_ + "_planned_net_usage.csv";
	//fileWriter = new PrintWriter(filename, "UTF-8");

    }
    
    public void setStorageId(int id){
	this.storageId = id;
    }
    
    //get list of gridlets for submission
    public void setGridletList(GridletList gridlets){
    	this.gridlets = gridlets;  
    	this.totalGridlet = this.gridlets.size();
    	receiveList_ = new GridletList();
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
		//this.deltaT = ParameterReader.deltaT;
		//this.beta =  (float) ParameterReader.beta;	
		//solver = new DataProductionPlanner(ParameterReader.planerLogFilename, deltaT, beta);
		LinkedList<LinkFlows> allLinkFlows = new LinkedList<LinkFlows>();
		for(CompNode node : ResourceReader.planerNodes){
		    //solver.addNode(node);
		    allLinkFlows.add(new LinkFlows(-node.getId(), "dummy_" + node.getName(), -1, node.getId(), -1));
		}
		for(NetworkLink link : DPNetworkReader.planerLinks){
		    //solver.addLink(link);
		    allLinkFlows.add(
			    new LinkFlows(link.getId(), link.getName(), link.getBandwidth(), link.getBeginNodeId(), 
		        	    link.getEndNodeId() )  );
		}	
		//solver.PrintGridSetup();
		FlowManager.setLinks(allLinkFlows);
		
	        // This to give a time for GridResource entities to register their
	        // services to GIS (GridInformationService) entity.
	        super.gridSimHold(1000.0);
	        write(name_ + ": retrieving GridResourceList");
	        LinkedList resList = super.getGridResourceList();
	        
	        // initialises all the containers
	        this.totalResource = resList.size();
	        write(name_ + ": obtained " + totalResource + " resource IDs in GridResourceList");
	        this.resourceID = new int[this.totalResource];
	        this.resourceName = new String[this.totalResource];
	        this.resourcePEs = new double[totalResource];
	        ResourceCharacteristics resChar;
	        
	        //get characteristics of resources
	        for (int i = 0; i < totalResource; i++)
	        {
	            // Resource list contains list of resource IDs not grid resource
	            // objects.
	            resourceID[i] = ( (Integer)resList.get(i) ).intValue();
	            //write("sending res. char. request to " + resourceID[i]);
	            // Requests to resource entity to send its characteristics
	            super.send(resourceID[i], GridSimTags.SCHEDULE_NOW,
	                       GridSimTags.RESOURCE_CHARACTERISTICS, this.myId_);

	            // waiting to get a resource characteristics
	            //write("after send");
	            Object obj = super.receiveEventObject();
	            //write(obj.toString());
	            //Sim_event ev = (Sim_event); 
	            resChar = (ResourceCharacteristics) obj; //super.receiveEventObject();
	            resourceName[i] = resChar.getResourceName();
	            resourcePEs[i] = resChar.getNumPE();

	            write("Received ResourceCharacteristics from " +
	                    resourceName[i] + ", with id = " + resourceID[i] + " with  " + resourcePEs[i] + " PEs");
	            NodeStatRecorder.registerNode(resourceID[i], resourceName[i], (int) resourcePEs[i], resourcePEs[i] != 1);

	            // record this event into "stat.txt" file
	            //super.recordStatistics("\"Received ResourceCharacteristics " +
	            //        "from " + resourceName[i] + "\"", "");
	        }
	        
	        //total PE number
	        double totalPEs = 0;
	        for(int i = 0; i < totalResource; i++){
	        	totalPEs += resourcePEs[i];
	        }
	        
	        String nodeStatFilename = "output/" + DataUnits.getPrefix() + "PUSHseq_CpuUsage.csv";
		NodeStatRecorder.start(nodeStatFilename);
    }
    

    /**
     * The core method that handles communications among GridSim entities.
     */
    public void body() {
	initPlaner();
	////////////////////////////////////////////////
	// SUBMIT Gridlets
	//init variables
	DPGridlet gl = null;
	//IO_data data;
	boolean success;
        double[] sendTime = new double[totalGridlet];
        double[] receiveTime = new double[totalGridlet];

	//initial populating of PEs
	int i = 0; //what is this????????????????
	int j = 0; //number of gridlet
	int k = 0; // number of PE
	startTime = GridSim.clock(); ///Start time of the submission;
	for(i = 0; i <  totalResource; i++){ 
	    if (resourcePEs[i] == 1){continue;}//skip resources with 1 CPU
	    for(k = 0; k < resourcePEs[i] && j < gridlets.size(); k++){
		gl = (DPGridlet) gridlets.get(j);
		if(j % chunkSize == 0){
		    write("processed: " + j + " / " + totalGridlet );
		    //write(name_ + "Sending Gridlet #" + j + "with id " + gl.getGridletID() + " to PE " + k + " at " + resourceName[i] + " at time " + GridSim.clock());
		}
		//write(gridletToString(gl));
		//success = super.gridletSubmit(gl, resourceID[i]);
		gl.setUserID(myId_);
		gl.setUsedLink(FlowManager.getLinkFlows(storageId, resourceID[i]));
		sim_schedule(super.output, GridSimTags.SCHEDULE_NOW, GridSimTags.GRIDLET_SUBMIT,
			new IO_data(gl, gl.getGridletFileSize() ,resourceID[i]) );
		sendTime[j]=  GridSim.clock(); //remember the time when the gridlet was submited
		j++;
	    }
	}
	write(name_ +"%%%%%%%%%%% " + j + "gridlets submitted,  " +  (gridlets.size() - j) + " gridlets left after initial submision");

	////////////////////////////////////////////////////////
	// RECEIVES Gridlets and submit new

	// hold for few period - few seconds since the Gridlets length are
	// quite huge for a small bandwidth
	//super.gridSimHold(5);

	int resourceFromID = 0;
	String resourceFromName = null;

	// receives the gridlet back
	for (i = 0; i < totalGridlet; i++){ //loop over received gridlets
	    gl = (DPGridlet) super.receiveEventObject();  // gets the Gridlet
	    if( i==0 ) { saturationStart = GridSim.clock();} //first gridlet received            
	    receiveTime[gridlets.indexOf(gl)]=  GridSim.clock(); //remember the time when the gridlet was received
	    receiveList_.add(gl);   // add into the received list            
	    resourceFromID = gl.getResourceID(); //resource which has a free PE
	    resourceFromName = GridSim.getEntityName(resourceFromID);
	    //update network statistics
	    gl.getUsedLink().addOutputTransfer(gl.getGridletOutputSize());
	    //if(j % (list_.size() / 100) == 0){
	    //write(name_ + ": Receiving Gridlet #" +
	    //gl.getGridletID() + "from: " + resourceFromName + " at time = " + GridSim.clock() );
	    //}

	    if(j < totalGridlet){ //if not all gridlets are submitted
		//submit next gridlet
		gl = (DPGridlet) gridlets.get(j);
		//if(j % (list_.size() / 100) == 0){
		//write(name_ + "Sending next Gridlet #" + j + "with id " + gl.getGridletID() + " to " + resourceFromName + " at time " + GridSim.clock());
		//}

		if(j % chunkSize == 0){
		    write("processed: " + j + " / " + totalGridlet );
		}
		//success = super.gridletSubmit(gl, resourceFromID);
		gl.setUserID(myId_);
		gl.setUsedLink(FlowManager.getLinkFlows(storageId, resourceFromID));
		sim_schedule(super.output, GridSimTags.SCHEDULE_NOW, GridSimTags.GRIDLET_SUBMIT,
			new IO_data(gl, gl.getGridletFileSize() ,resourceFromID) );
		sendTime[j]=  GridSim.clock(); //remember the time when the gridlet was submited
		j++;
		if (j == totalGridlet){
		    write(name_ + " ALL GRIDLETS SUBMITTED");
		    saturationFinish = GridSim.clock();
		}
	    }            
	}
	finishTime = GridSim.clock();

	////////////print statistics
	//printGridletList(receiveList_, name_);
	for (i = 0; i < gridlets.size(); i += gridlets.size() / 5){
	    gl = (DPGridlet) gridlets.get(i);
	    printGridletHist(gl);
	}

	////print transfer times 
	write("-------------gridlet log--------------");
	write("getGridletID getResourceID getGridletLength 	getGridletFileSize	 getGridletOutputSize	 	inTransfer	 		outTransfer		 getWallClockTime		totalTime 			slowdown");

	double inTransfer, outTransfer, totalTime, slowdown; 
	String indent = "		";
	for (i = 0; i < gridlets.size(); i += chunkSize / 5){
	    gl = (DPGridlet) gridlets.get(i);
	    inTransfer = gl.getExecStartTime() - sendTime[i];
	    outTransfer = receiveTime[i] - gl.getFinishTime();
	    totalTime = receiveTime[i] - sendTime[i];
	    slowdown = totalTime / gl.getWallClockTime();
	    write(gl.getGridletID() + indent + gl.getResourceID() + indent + gl.getGridletLength() + indent + gl.getGridletFileSize() + indent + gl.getGridletOutputSize() + indent +
		    inTransfer + indent + outTransfer + indent + gl.getWallClockTime() + indent + totalTime + indent + slowdown);
	}


	///calculate computational efficiency per resource
	double[] firstJobSend = new double[totalResource];
	double[] lastJobReceived = new double[totalResource];
	double[] work = new double[totalResource];
	int[] jobs = new int[totalResource];

	double[] outminTransferTime = new double[totalResource];
	double[] outmaxTransferTime = new double[totalResource];
	double[] outtransferTime = new double[totalResource];

	double[] minTransferTime = new double[totalResource];
	double[] maxTransferTime = new double[totalResource];
	double[] transferTime = new double[totalResource];
	//initialize values
	for(i = 0; i <  totalResource; i++){
	    firstJobSend[i] = Double.MAX_VALUE;
	    lastJobReceived[i] = 0.0;
	    work[i] = 0.0;
	    jobs[i] = 0;
	    minTransferTime[i] = Double.MAX_VALUE;
	    maxTransferTime[i] = 0.0;
	    transferTime[i] = 0.0;

	    outminTransferTime[i] = Double.MAX_VALUE;
	    outmaxTransferTime[i] = 0.0;
	    outtransferTime[i] = 0.0;
	}

	double gridletTransferTime;
	double outgridletTransferTime;
	for (j = 0; j < gridlets.size(); j++){ //loop over gridlets
	    gl = (DPGridlet) gridlets.get(j);
	    for(i = 0; i <  totalResource; i++){ //loop over resources
		if(gl.getResourceID() == resourceID[i]){
		    jobs[i]++;
		    work[i] += gl.getActualCPUTime();
		    gridletTransferTime = gl.getSubmissionTime() - sendTime[j];
		    outgridletTransferTime = receiveTime[j] - gl.getFinishTime();
		    transferTime[i] += gridletTransferTime;
		    outtransferTime[i] += outgridletTransferTime;
		    if(firstJobSend[i] > sendTime[j]) { firstJobSend[i] = sendTime[j]; } //serch for the first job submited to the resource 
		    if( lastJobReceived[i] < receiveTime[j] ) { lastJobReceived[i] = receiveTime[j]; } //search for the last job arrived from the resource 
		    if( minTransferTime[i] > gridletTransferTime ) { minTransferTime[i] = gridletTransferTime; }
		    if( maxTransferTime[i]  < gridletTransferTime ) { maxTransferTime[i]  = gridletTransferTime; }
		    if( outminTransferTime[i] > outgridletTransferTime ) { outminTransferTime[i] = outgridletTransferTime; }
		    if( outmaxTransferTime[i]  < outgridletTransferTime ) { outmaxTransferTime[i]  = outgridletTransferTime; }

		    break;

		}
	    }
	}

	///////////////////////////////////////
	//print computational efficiency        
	double cost = 1.0;
	double efficiency = 0.0;
	indent = "	";
	write("#####################Computational efficiency######################");
	write("Name  PEs	jobs	firstJobSend	lastJobReceived	cost		work	efficiency	minTransfer	maxTransfer	"
		+ "averageTransfer	outminTrans	outmaxTrans	averageOutTrans");
	for(i = 0; i <  totalResource; i++){ //loop over resources
	    cost = (lastJobReceived[i] - firstJobSend[i]) * resourcePEs[i] ;
	    efficiency = work[i] / cost;        	

	    System.out.print(String.format("%6s	", resourceName[i]));
	    System.out.print(String.format("%5.0f	",resourcePEs[i]  ));
	    System.out.print(String.format("%d	",jobs[i] ));
	    System.out.print(String.format("%10.3f	",firstJobSend[i]));
	    System.out.print(String.format("%10.3f	",lastJobReceived[i]));
	    System.out.print(String.format("%10.3f	",cost));
	    System.out.print(String.format("%10.3f	",work[i]));        	
	    System.out.print(String.format("%2.3f	",efficiency));

	    System.out.print(String.format("%10.3f	",minTransferTime[i]));
	    System.out.print(String.format("%10.3f	",maxTransferTime[i]));
	    System.out.print(String.format("%10.3f	",(transferTime[i] / jobs[i]) ) );
	    System.out.print(String.format("%10.3f	",outminTransferTime[i]));
	    System.out.print(String.format("%10.3f	",outmaxTransferTime[i]));
	    System.out.println(String.format("%10.3f	",(outtransferTime[i] / jobs[i]) ));
	    System.out.println();

	}

	/////////////////////////////
	//print overall statistics
	write("---------------summary------------------");
	write("Number of gridlets: " + totalGridlet);
	write(" Resources: " + totalResource);
	write(" PEs: " + totalPEs);

	write(" Submission start: " + startTime);
	write(" saturationStart: " + saturationStart);
	write(" saturationFinish: " + saturationFinish);
	write(" Last receive: " + finishTime);

	write(" Makespan: " + (finishTime - startTime));
	write(" Saturated interval: " + (saturationFinish - saturationStart));
	write(" Saturated time ratio: " + (saturationFinish - saturationStart) / (finishTime - startTime));
	write("------------------------------------------");

	
	finish();
    }
    
    private void finish(){	

        //ping resources
        pingAllRes(resourceID);

	
        ////////////////////////////////////////////////////////
        // shut down I/O ports
	
        shutdownUserEntity();
        FlowManager.initialized = false;
        terminateIOEntities();
        //close global CPU monitor
        NodeStatRecorder.close();
        System.out.println(this.name_ + ":%%%% Exiting body() at time " +
            GridSim.clock());
        //fileWriter.close();
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
    private static void printGridletHist(Gridlet gridlet){
        System.out.println( gridlet.getGridletHistory() );

        System.out.print("Gridlet #" + gridlet.getGridletID() );
        System.out.println(", length = " + gridlet.getGridletLength()
                + ", finished so far = " +
                gridlet.getGridletFinishedSoFar() );
        System.out.println("======================================\n");	  	
    	
    }

} // end class


   
