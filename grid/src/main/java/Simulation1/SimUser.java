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
    int maxGridlet; //how many records to read from log
    private int totalGridlet; //how many gridlet were red from file
    private GridletList list_;          // list of submitted Gridlets
    private GridletList receiveList_;   // list of received Gridlets
    private SimReport report_;  // logs every events
    boolean trace_flag;
    double startTime, saturationStart, saturationFinish, finishTime ;
    int chunkSize; //for monitoring
    

    // constructor
    SimUser(String name, double baud_rate, double delay, int MTU) throws Exception {
    	
    	//super(name, new SimpleLink(name + "_link", baud_rate, delay, MTU));

        // NOTE: uncomment this if you want to use the new Flow extension
        super(name, new FlowLink(name + "_link", baud_rate, delay, MTU));

    	
    	trace_flag = true;
        this.name_ = name;
        this.receiveList_ = new GridletList();
        GridletReader gridletReader = new GridletReader();
        
        maxGridlet = 10000;
        String gridletFilename = "F:/git/Grid_simulation/grid/src/main/java/flow_model/input/KISTI_all_filtered.csv";
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
        write(name + ":Reading " + maxGridlet +" Gridlets");
        //this.createGridlet(myId_, totalGridlet);
        this.list_ = GridletReader.getGridletList(gridletFilename, maxGridlet, myId_);
        write(name + ": " + maxGridlet +" Gridlets were red");
        totalGridlet = list_.size();
        chunkSize = totalGridlet / 20;
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
        double[] sendTime = new double[totalGridlet];
        double[] receiveTime = new double[totalGridlet];
        
        // initialises all the containers
        int totalResource = resList.size();
        write(name_ + ": obtained " + totalResource + " resource IDs in GridResourceList");
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
        Gridlet gl = null;
        boolean success;
        
        //initial populating of PEs
        
        int j = 0; //number of gridlet
        int k = 0; // number of PE
        startTime = GridSim.clock(); ///Start time of the submission;
        for(i = 0; i <  totalResource; i++){ 
        	for(k = 0; k < resourcePEs[i] && j < list_.size(); k++){
        		gl = (Gridlet) list_.get(j);
        		if(j % chunkSize == 0){
        			System.out.println("   ." + j + " / " + totalGridlet );
        			//write(name_ + "Sending Gridlet #" + j + "with id " + gl.getGridletID() + " to PE " + k + " at " + resourceName[i] + " at time " + GridSim.clock());
        		}
        			//write(gridletToString(gl));
                success = super.gridletSubmit(gl, resourceID[i]);
                sendTime[j]=  GridSim.clock(); //remember the time when the gridlet was submited
                j++;
        	}
        }
        write(name_ +"%%%%%%%%%%% " + j + "gridlets submitted,  " +  (list_.size() - j) + " gridlets left after initial submision");
        
        ////////////////////////////////////////////////////////
        // RECEIVES Gridlets and submit new

        // hold for few period - few seconds since the Gridlets length are
        // quite huge for a small bandwidth
        //super.gridSimHold(5);
        
        int resourceFromID = 0;
        String resourceFromName = null;
        
        // receives the gridlet back
        for (i = 0; i < totalGridlet; i++){ //loop over received gridlets
            gl = (Gridlet) super.receiveEventObject();  // gets the Gridlet
            if( i==0 ) { saturationStart = GridSim.clock();} //first gridlet received            
            receiveTime[list_.indexOf(gl)]=  GridSim.clock(); //remember the time when the gridlet was received
            receiveList_.add(gl);   // add into the received list            
            resourceFromID = gl.getResourceID(); //resource which has a free PE
            resourceFromName = GridSim.getEntityName(resourceFromID);
            //if(j % (list_.size() / 100) == 0){
            	//write(name_ + ": Receiving Gridlet #" +
                  //gl.getGridletID() + "from: " + resourceFromName + " at time = " + GridSim.clock() );
            //}
            
            if(j < totalGridlet){ //if not all gridlets are submitted
            	//submit next gridlet
            	gl = (Gridlet) list_.get(j);
            	//if(j % (list_.size() / 100) == 0){
            		//write(name_ + "Sending next Gridlet #" + j + "with id " + gl.getGridletID() + " to " + resourceFromName + " at time " + GridSim.clock());
            	//}
            	
        		if(j % chunkSize == 0){
        			System.out.println("   ." + j + " / " + totalGridlet );
        		}
            	success = super.gridletSubmit(gl, resourceFromID);
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
        for (i = 0; i < list_.size(); i += list_.size() / 5){
        	gl = (Gridlet) list_.get(i);
        	printGridletHist(gl);
        }
        
        ////print transfer times 
        System.out.println("-------------gridlet log--------------");
        System.out.println("getGridletID getResourceID getGridletLength 	getGridletFileSize	 getGridletOutputSize	 	inTransfer	 		outTransfer		 getWallClockTime		totalTime 			slowdown");

        double inTransfer, outTransfer, totalTime, slowdown; 
        String indent = "		";
        for (i = 0; i < list_.size(); i += chunkSize / 5){
        	gl = (Gridlet) list_.get(i);
        	inTransfer = gl.getExecStartTime() - sendTime[i];
        	outTransfer = receiveTime[i] - gl.getFinishTime();
        	totalTime = receiveTime[i] - sendTime[i];
        	slowdown = totalTime / gl.getWallClockTime();
        	write(gl.getGridletID() + indent + gl.getResourceID() + indent + gl.getGridletLength() + indent + gl.getGridletFileSize() + indent + gl.getGridletOutputSize() + indent +
        			inTransfer + indent + outTransfer + indent + gl.getWallClockTime() + indent + totalTime + indent + slowdown);
        	
        }


        ////////////////////////////////////////////////////////
        //ping resources
        for(i = 0; i <  totalResource; i++){ 
        	pingRes(resourceID[i]);
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
        for (j = 0; j < list_.size(); j++){ //loop over gridlets
        	gl = (Gridlet) list_.get(j);
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
        System.out.println("#####################Computational efficiency######################");
        System.out.println("Name  PEs	jobs	firstJobSend	lastJobReceived	cost		work	efficiency	minTransfer	maxTransfer	"
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
    

    /**
     * Prints the Gridlet objects
     */
    private static void printGridletList(GridletList list, String name)
    {
        int size = list.size();
        Gridlet gridlet = null;

        String indent = "    ";
        System.out.println();
        System.out.println("============= OUTPUT for " + name + " ==========");
        System.out.println("Gridlet ID" + indent + "getResourceID" + "STATUS" + indent +
                "Resource ID" + " getGridletLength  getGridletFileSize getGridletOutputSize getGridletOutputSize getSubmissionTime getWaitingTime getWallClockTime getExecStartTime");

        // a loop to print the overall result
        int i = 0;
        for (i = 0; i < size; i++)
        {
            gridlet = (Gridlet) list.get(i);
            printGridlet(gridlet);
            

            System.out.println();
        }
    }
    
    private static void printGridlet(Gridlet gridlet)
    {
    	String indent = "    ";
    	System.out.print(indent + gridlet.getGridletID() + indent);
        System.out.print( gridlet.getResourceID() + indent );
        System.out.print( gridlet.getGridletStatusString() + indent );
        System.out.print( gridlet.getGridletLength() + indent);
        System.out.print( gridlet.getGridletFileSize() + indent);            
        System.out.print( gridlet.getGridletOutputSize() + indent);
        System.out.print( gridlet.getSubmissionTime() + indent);
        System.out.print( gridlet.getWaitingTime() + indent);
        System.out.print( gridlet.getWallClockTime() + indent);
        System.out.print( gridlet.getExecStartTime() + indent);
    }
    
    private static String gridletToString(Gridlet gridlet){
    	StringBuffer  br = new StringBuffer();;
    	String indent = " ";
    	
    	br.append("ID=" + gridlet.getGridletID() + indent);
    	br.append( "ResId=" + gridlet.getResourceID() + indent );
    	br.append( "Stat=" + gridlet.getGridletStatusString() + indent );
    	br.append( "Length=" + gridlet.getGridletLength() + indent);
    	br.append( "inSize=" +gridlet.getGridletFileSize() + indent);            
    	br.append( "outSize=" +gridlet.getGridletOutputSize() + indent);
    	br.append( "SubTime=" +gridlet.getSubmissionTime() + indent);
    	br.append( "WaitTime=" +gridlet.getWaitingTime() + indent);
    	br.append( "Wallclock=" +gridlet.getWallClockTime() + indent);
    	br.append( "ExecStart=" +gridlet.getExecStartTime());
    	
        return br.toString();
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


    
