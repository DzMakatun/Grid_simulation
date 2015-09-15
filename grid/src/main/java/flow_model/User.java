package flow_model;

/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * License:      GPL - http://www.gnu.org/copyleft/gpl.html
 */

import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import gridsim.GridletList;
import gridsim.IO_data;
import gridsim.ResourceCharacteristics;
import gridsim.datagrid.DataGridUser;
import gridsim.datagrid.File;
import gridsim.datagrid.FileAttribute;
import gridsim.net.InfoPacket;
import gridsim.net.SimpleLink;
import gridsim.GridUser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import gridsim.net.flow.*;  // To use the new flow network package - GridSim 4.2
import gridsim.util.SimReport;

/**
 * This class defines a user which submits raw data for processing in 
 * network-flow model.
 * @author Uros Cibej and Anthony Sulistio
 * @author Dzmitry Makatun
 */
class User extends GridUser {
    private String name_;
    private int myId_;
    private int totalGridlet; //how many gridlet were red from file
    private GridletList gridlets;          // list of submitted Gridlets
    private GridletList receiveList_;   // list of received Gridlets
    private SimReport report_;  // logs every events
    boolean trace_flag;
    double startTime, saturationStart, saturationFinish, finishTime ;
    int chunkSize; //for monitoring
    private LinkedList <LinkFlows> newPlan;
    

    // constructor
    User(String name, double baud_rate, double delay, int MTU) throws Exception {
    	
    	//super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));

        // NOTE: uncomment this if you want to use the new Flow extension
        super(name, new FlowLink(name + "_link", baud_rate, delay, MTU));
    	
        // creates a report file
        if (trace_flag == true) {
            report_ = new SimReport(name);
        }
    	
    	trace_flag = true;
        this.name_ = name;

        // Gets an ID for this entity
        this.myId_ = super.getEntityId(name);
        write("Creating a grid user entity with name = " +
              name + ", and id = " + this.myId_);
      
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
    
    
    /**
     * The core method that handles communications among GridSim entities.
     */
    public void body() {
    	 // wait for a little while for about 3 seconds.
        // This to give a time for GridResource entities to register their
        // services to GIS (GridInformationService) entity.
        super.gridSimHold(1000.0);
        System.out.println(name_ + ": retrieving GridResourceList");
        LinkedList resList = super.getGridResourceList();
        
        // initialises all the containers
        int totalResource = resList.size();
        write(name_ + ": obtained " + totalResource + " resource IDs in GridResourceList");
        int resourceID[] = new int[totalResource];
        String resourceName[] = new String[totalResource];
        
        //get characteristics of resources
        ResourceCharacteristics resChar;
        double resourcePEs[] = new double[totalResource];
        int i = 0 ;
        for (i = 0; i < totalResource; i++)
        {
            // Resource list contains list of resource IDs not grid resource
            // objects.
            resourceID[i] = ( (Integer)resList.get(i) ).intValue();

            // Requests to resource entity to send its characteristics
            super.send(resourceID[i], GridSimTags.SCHEDULE_NOW,
                       GridSimTags.RESOURCE_CHARACTERISTICS, this.myId_);

            // waiting to get a resource characteristics
            resChar = (ResourceCharacteristics) super.receiveEventObject();
            resourceName[i] = resChar.getResourceName();
            resChar.getNumFreePE();
            resourcePEs[i] = resChar.getNumPE();

            write("Received ResourceCharacteristics from " +
                    resourceName[i] + ", with id = " + resourceID[i] + " with  " + resourcePEs[i] + " PEs");

            // record this event into "stat.txt" file
            super.recordStatistics("\"Received ResourceCharacteristics " +
                    "from " + resourceName[i] + "\"", "");
        }
        
        //total PE number
        double totalPEs = 0;
        for(i = 0; i < totalResource; i++){
        	totalPEs += resourcePEs[i];
        }
        
        /////////////////////////////////////////////////
        //GET resource statuses
        
        //send requests
        for(i = 0; i <  totalResource; i++){ 
                     
            write("sending status request to resource " + resourceID[i]);
            send(super.output, GridSimTags.SCHEDULE_NOW, RiftTags.STATUS_REQUEST,
                 new IO_data(myId_, 0, resourceID[i], 0) 
            );
          } 
        
        //receive responses
        Sim_event ev = new Sim_event();
        Map status;
        for(i = 0; i <  totalResource; i++){ 
            super.sim_get_next(ev);
            status = (HashMap) ev.get_data();
            
            write("received status responce" + statusToString(status));

          } 
        
        //create plan
        newPlan = createNewPlan(resourceID);
        
        
        //send plan
        for(i = 0; i <  totalResource; i++){             
            write("sending new plan to resource " + resourceID[i]);
            send(super.output, GridSimTags.SCHEDULE_NOW, RiftTags.NEW_PLAN,
                 new IO_data(newPlan, 0, resourceID[i], 0) 
            );
          } 


        ////////////////////////////////////////////////
        // SUBMIT Gridlets
        startTime = GridSim.clock(); ///Start time of the submission;
        super.gridSimHold(1.0);
        
        // SUBMIT Gridlets
        DPGridlet gl = null;
        boolean success;
        
        //initial populating of PEs
        
        int j = 0; //number of gridlet
        int k = 0; // number of PE
        startTime = GridSim.clock(); ///Start time of the submission;
        //for(i = 0; i <  totalResource && i < gridlets.size(); i++){ 
          //gl = (DPGridlet) gridlets.get(i);  
          //gl.setUserID(myId_);
          
          //GENERAL FORM:
          // send(output_port,  GridSimTags.SCHEDULE_NOW, TAG, 
          //      new IO_data(your_object, message_size, destination_id,
          //                  netServiceLevel = 0));

          
          //write(" sending gridlet " + gl.getGridletID() + " to resource " + resourceID[i]);
          //send(super.output, GridSimTags.SCHEDULE_NOW, RiftTags.INPUT,
          //     new IO_data(gl, gl.getGridletFileSize(), resourceID[i],
          //                 0) 
          //);

       // }       
       
        
        
        ////////////////////////////////////////////////////////
        // RECEIVES Gridlets and submit new
        //Sim_event ev = new Sim_event();
        //while ( Sim_system.running() )
        //{
          //  super.sim_get_next(ev);

            // if the simulation finishes then exit the loop
            //if (ev.get_tag() == GridSimTags.END_OF_SIMULATION)
            //{
              //  policy_.setEndSimulation();
               // break;
            //}

            // process the received event
            //processEvent(ev);
        //}

        
        
        
        saturationStart = GridSim.clock();
        super.gridSimHold(1.0);
        saturationFinish = GridSim.clock();
        super.gridSimHold(1.0);
        finishTime = GridSim.clock();

        
        
        
             ////////////////////////////////////////////////////////
        //ping resources
        for(i = 0; i <  totalResource; i++){ 
        	pingRes(resourceID[i]);
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

        ////////////////////////////////////////////////////////
        // shut down I/O ports
        shutdownUserEntity();
        terminateIOEntities();

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

   /** generates new transfer plan
    * The planner logic is connected here 
    * @return
    */
   private LinkedList<LinkFlows> createNewPlan(int[] resourceID) {
       double defaultLocalProcessingFlow = 100000;
       double defaultInputFlow = 10000;
       double defaultOutputFlow = 100000;
              
       LinkedList<LinkFlows> plan = new LinkedList();
       LinkFlows tempFlow;
       for (int i = 0; i < resourceID.length; i++ ){
	   plan.add(new LinkFlows(resourceID[i], -1, defaultLocalProcessingFlow, 0)); // add local processing flow
	   plan.add(new LinkFlows(this.myId_, resourceID[i], defaultInputFlow, 0)); // input flow from user to resources
	   plan.add(new LinkFlows(resourceID[i], this.myId_, 0, defaultOutputFlow)); // flow back to user
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
	
	return buf.toString();
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


    
