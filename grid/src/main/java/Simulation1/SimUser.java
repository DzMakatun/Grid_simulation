package Simulation1;

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
class SimUser extends DataGridUser {
    private String name_;
    private int myId_;
    private int totalGridlet;
    private GridletList list_;          // list of submitted Gridlets
    private GridletList receiveList_;   // list of received Gridlets
    private SimReport report_;  // logs every events
    boolean trace_flag;
    

    // constructor
    SimUser(String name, double baud_rate, double delay, int MTU) throws Exception {
    	
    	super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));

        // NOTE: uncomment this if you want to use the new Flow extension
        //super(name, new FlowLink(name + "_link", baud_rate, delay, MTU));
    	
    	trace_flag = true;
        this.name_ = name;
        this.receiveList_ = new GridletList();
        GridletReader gridletReader = new GridletReader();
        
        totalGridlet = 100;
        this.list_ = new GridletList();
        
        

        // creates a report file
        if (trace_flag == true) {
            report_ = new SimReport(name);
        }

        // Gets an ID for this entity
        this.myId_ = super.getEntityId(name);
        write("Creating a grid user entity with name = " +
              name + ", and id = " + this.myId_);

        // Creates a list of Gridlets or Tasks for this grid user
        write(name + ":Creating " + totalGridlet +" Gridlets");
        //this.createGridlet(myId_, totalGridlet);
        this.list_ = GridletReader.getGridletList("KISTIlogFilerred.csv", totalGridlet, myId_);
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
        System.out.println(name_ + ": obtained " + totalResource + " resource IDs in GridResourceList");
        int resourceID[] = new int[totalResource];
        String resourceName[] = new String[totalResource];
        
        //get characteristics of resources
        ResourceCharacteristics resChar;
        double resourceCost[] = new double[totalResource];
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
            resourceCost[i] = resChar.getCostPerSec();
            resChar.getNumFreePE();
            resourcePEs[i] = resChar.getNumPE();

            System.out.println("Received ResourceCharacteristics from " +
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
        Gridlet gl = null;
        boolean success;
        
        //initial populating of PEs
        
        int j = 0; //number of gridlet
        int k = 0; // number of PE
        for(i = 0; i <  totalResource; i++){ 
        	for(k = 0; k < resourcePEs[i] && j < list_.size(); k++){
        		gl = (Gridlet) list_.get(j);
                write(name_ + "Sending Gridlet #" + j + "with id " + gl.getGridletID() + " to PE " + k + " at " + resourceName[i] + " at time " + GridSim.clock());
                success = super.gridletSubmit(gl, resourceID[i]);
                j++;
        	}
        }
        
        write(name_ +"%%%%%%%%%%%" + (list_.size() - j) + " gridlets left after initial submision");
        
        ////////////////////////////////////////////////////////
        // RECEIVES Gridlets and submit new

        // hold for few period - few seconds since the Gridlets length are
        // quite huge for a small bandwidth
        //super.gridSimHold(5);
        
        int resourceFromID = 0;
        String resourceFromName = null;
        
        // receives the gridlet back
        for (i = 0; i < list_.size(); i++){ //loop over received gridlets
            gl = (Gridlet) super.receiveEventObject();  // gets the Gridlet
            receiveList_.add(gl);   // add into the received list
            resourceFromID = gl.getResourceID(); //resource which has a free PE
            resourceFromName = GridSim.getEntityName(resourceFromID);
            write(name_ + ": Receiving Gridlet #" +
                  gl.getGridletID() + "from: " + resourceFromName + " at time = " + GridSim.clock() );
            
            
            if(j < list_.size()){ //if not all gridlets are submitted
            	//submit next gridlet
            	gl = (Gridlet) list_.get(j);
                write(name_ + "Sending next Gridlet #" + j + "with id " + gl.getGridletID() + " to " + resourceFromName + " at time " + GridSim.clock());
                success = super.gridletSubmit(gl, resourceFromID);
                j++;
                if (j == list_.size()){
                	write(name_ + " ALL GRIDLETS SUBMITTED");
                }
            }            
        }

        ////////////////////////////////////////////////////////
        //ping resources
        for(i = 0; i <  totalResource; i++){ 
        	pingRes(resourceID[i]);
        }

        ////////////////////////////////////////////////////////
        // shut down I/O ports
        shutdownUserEntity();
        terminateIOEntities();

        // don't forget to close the file
        if (report_ != null) {
            report_.finalWrite();
        }

        write(this.name_ + ": sending and receiving of Gridlets" +
              " complete at " + GridSim.clock() );
    

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
       int size = 500000;

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

    private void createGridlet(int userID, int numGridlet)
    {
        int data = 500000;
        for (int i = 0; i < numGridlet; i++)
        {
            // Creates a Gridlet
            Gridlet gl = new Gridlet(i, data, data, data);
            gl.setUserID(userID);

            // add this gridlet into a list
            this.list_.add(gl);
        }
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
