package flow_model;

/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * License:      GPL - http://www.gnu.org/copyleft/gpl.html
 */

import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import gridsim.GridletList;
import gridsim.ResourceCharacteristics;
import gridsim.datagrid.DataGridUser;
import gridsim.datagrid.File;
import gridsim.datagrid.FileAttribute;
import gridsim.net.InfoPacket;
import gridsim.net.SimpleLink;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import gridsim.net.flow.*;  // To use the new flow network package - GridSim 4.2
import gridsim.util.SimReport;

/**
 * This class defines a user which executes a set of commands.
 * @author Uros Cibej and Anthony Sulistio
 */
class User extends DataGridUser {
    private String name_;
    private int myId_;
    private int totalGridlet; //how many gridlet were red from file
    private GridletList gridlets;          // list of submitted Gridlets
    private GridletList receiveList_;   // list of received Gridlets
    private SimReport report_;  // logs every events
    boolean trace_flag;
    double startTime, saturationStart, saturationFinish, finishTime ;
    int chunkSize; //for monitoring
    

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


        ////////////////////////////////////////////////
        // SUBMIT Gridlets
        startTime = GridSim.clock(); ///Start time of the submission;
        super.gridSimHold(1.0);
        
       
        
        
        ////////////////////////////////////////////////////////
        // RECEIVES Gridlets and submit new
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
        System.out.println(msg);
        if (report_ != null) {
            report_.write(msg);
        }
    }
} // end class


    
