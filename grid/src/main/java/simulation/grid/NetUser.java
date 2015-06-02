package simulation.grid;


import java.util.*;

import gridsim.*;
import gridsim.net.*;
import gridsim.util.SimReport;


/**
 * This class basically creates Gridlets and submits them to a
 * particular GridResources in a network topology.
 */
class NetUser extends GridSim
{
    private int myId_;      // my entity ID
    private String name_;   // my entity name
    private GridletList list_;          // list of submitted Gridlets
    private GridletList receiveList_;   // list of received Gridlets
    private SimReport report_;  // logs every events


    /**
     * Creates a new NetUser object
     * @param name  this entity name
     * @param totalGridlet  total number of Gridlets to be created
     * @param baud_rate     bandwidth of this entity
     * @param delay         propagation delay
     * @param MTU           Maximum Transmission Unit
     * @param trace_flag    logs every event or not
     * @throws Exception    This happens when name is null or haven't
     *                      initialized GridSim.
     */
    NetUser(String name, int totalGridlet, double baud_rate, double delay,
            int MTU, boolean trace_flag) throws Exception
    {
        super( name, new SimpleLink(name+"_link",baud_rate,delay, MTU) );

        this.name_ = name;
        this.receiveList_ = new GridletList();
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
        this.createGridlet(myId_, totalGridlet);
    }

    /**
     * The core method that handles communications among GridSim entities.
     */
    public void body()
    {
        // wait for a little while for about 3 seconds.
        // This to give a time for GridResource entities to register their
        // services to GIS (GridInformationService) entity.
        super.gridSimHold(3.0);
        LinkedList resList = super.getGridResourceList();

        // initialises all the containers
        int totalResource = resList.size();
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
                write(name_ + "Sending Gridlet #" + j + " to PE " + k + " at " + resourceName[i] + " at time " + GridSim.clock());
                success = super.gridletSubmit(gl, resourceID[i]);
                j++;
        	}
        }
        
        write(name_ +"%%%%%%%%%%%" + (list_.size() - j) + " gridlets left after initial submision");
        
        ////////////////////////////////////////////////////////
        // RECEIVES Gridlets and submit new

        // hold for few period - few seconds since the Gridlets length are
        // quite huge for a small bandwidth
        super.gridSimHold(5);
        
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
                write(name_ + "Sending next Gridlet #" + j + " to " + resourceFromName + " at time " + GridSim.clock());
                success = super.gridletSubmit(gl, resourceFromID);
                j++;
                if (j == list_.size()){
                	write(name_ + " ALL GRIDLETS SUBMITTED");
                }
            }            
        }

        ////////////////////////////////////////////////////////
        // ping functionality
        InfoPacket pkt = null;
        int size = 500;

        // There are 2 ways to ping an entity:
        // a. non-blocking call, i.e.
        //super.ping(resourceID[index], size);    // (i)   ping
        //super.gridSimHold(10);        // (ii)  do something else
        //pkt = super.getPingResult();  // (iii) get the result back

        // b. blocking call, i.e. ping and wait for a result
        pkt = super.pingBlockingCall(resourceID[0], size);

        // print the result
        write("\n-------- " + name_ + " ----------------");
        write(pkt.toString());
        write("-------- " + name_ + " ----------------\n");

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
    }

    /**
     * Gets a list of received Gridlets
     * @return a list of received/completed Gridlets
     */
    public GridletList getGridletList() {
        return receiveList_;
    }

    /**
     * This method will show you how to create Gridlets
     * @param userID        owner ID of a Gridlet
     * @param numGridlet    number of Gridlet to be created
     */
    private void createGridlet(int userID, int numGridlet)
    {
        int data = 5000;
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

