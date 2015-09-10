package flow_model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import eduni.simjava.Sim_event;
import gridsim.*;
import gridsim.datagrid.*;
import gridsim.datagrid.storage.Storage;
import gridsim.net.Link;


/**
 * extends DataGridResource for network-flow model simulations
 * @author Dzmitry Makatun
 */
public class DPResource extends DataGridResource{
    	
    	private int planerId;
    	private boolean planIsSet;
    	
    	//properties to maitain the storage functionality
    	// and manage input/output files
    	private double storageSize; // (MB) total size of storage
    	private double freeStorageSpace; //(MB) available free space
    	private double waitingInputSize; //(MB) input data that can be transferred or processed
    	private double readyOutputSize; //(MB) output data ready to be transferred
    	
    	private GridletList waitingInputFiles = new GridletList(); //Gridlets That has just arrived
    	private GridletList submittedInputFiles = new GridletList(); //Gridlets submited for local processing
    	private GridletList readyOutputFiles = new GridletList(); //output files ready for transfer
    	private GridletList reservedOutputFiles = new GridletList(); //output files of runing jobs
    	
    	//properties to store the plan
    	private ArrayList<Integer> neighborNodesIds = new ArrayList<Integer>(); //IDs of directly connected nodes
    	private ArrayList<Double> neighborNodesInputFlows = new ArrayList<Double>(); //amoount of data to send to each node
    	private ArrayList<Double> neighborNodesOutputFlows = new ArrayList<Double>(); //amoount of data to send to each node
    	private double localProcessingFlow; //input data to be processed locally (counter)
    	private double remoteInputFlow; //total input data to be transferred out (counter)
    	private double remoteOutputFlow; //total output data to be transferred out (counter)
    	
    

	public DPResource(String name, Link link, ResourceCharacteristics resource, double storageSize, ResourceCalendar calendar, ReplicaManager replicaManager) throws Exception {
		super(name, link, resource, calendar, replicaManager);
		this.planIsSet = false;
		this.storageSize = storageSize;
		this.freeStorageSpace = this.storageSize;
		this.waitingInputSize = 0;
		this.readyOutputSize = 0;
		
	}
	
	public DPResource(){
	    super();
	}

	
	/**
	 * provides summary about the resource as a string
	 */
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
	
	/**
	 * Here is the main logic of network flow model execution.
	 * The "handler" (i.e this resource) processes dedicated events
	 * events can be passed to underlying sched. policy.
	 */
	@Override
	protected void processOtherEvent(Sim_event ev)
	  {
	      if (ev == null)
	      {
	          System.out.println(super.get_name() + ".processOtherEvent(): " +
	                  "Error - an event is null.");
	          return;
	      }

	        //pass event to the scheduler
	      	write("Received event: " + verboseEvent(ev) );
	      	
	      	//extract event tag
	      	int tag = ev.get_tag();
	      	
	      	switch (tag)
	        {
	            case RiftTags.STATUS_REQUEST:
	        	write("received status request");
	                processStatusRequest(ev);
	                break;

	            case RiftTags.NEW_PLAN:
	        	write("received new plan");
	                processNewPlan(ev);
	                break;
	                
	            case RiftTags.INPUT:
	        	write("received input file");
	                processIncommingInputFile(ev);
	                break;    

	            case RiftTags.OUTPUT:
	        	write("received input file");
	                processIncommingOutputFile(ev);
	                break;     
	                
	            case GridSimTags.GRIDLET_RETURN:
	        	write("I cached it!!!");
	        	break;
	                
	            default:
	        	//send the unknown event to scheduler;
	        	write("Unknown event received");
	        	policy_.processOtherEvent(ev);
	                break;
	        }        
	    }
	
	private void processIncommingOutputFile(Sim_event ev) {
	    DPGridlet gl = (DPGridlet) ev.get_data();
	    //add new input file to
	    if (addOutputFile(gl) ){
		//send confirmation to sender
	    }else{
		//send failure back to sender 		
		return;
	    }
	    
	    if (planIsSet){
		processFiles();
	    }
	    
	}

	/** Add an incoming output file to storage 
	 * @param gl
	 * @return true if success
	 */
	private boolean addOutputFile(DPGridlet gl) {
	    double size = gl.getInputSizeInMB();
	    if (this.freeStorageSpace >= size ) {
		this.freeStorageSpace -= size;
		this.readyOutputSize += size;
		this.readyOutputFiles.add(gl);
		return true;
		
	    }else{
		//failed to accommodate a new file
		write("WARNING RUNNING OUT OF STORAGE SPACE,"
			+ " can't accomodate new input file, freeSpace: " 
			+ this.freeStorageSpace + " fileSize: "
			+ size);
		return false;
	    }
	}


	private void processIncommingInputFile(Sim_event ev) {
	    DPGridlet gl = (DPGridlet) ev.get_data();
	    //add new input file to
	    if (addInputFile(gl) ){
		//send confirmation to sender
	    }else{
		//send failure back to sender 		
		return;
	    }
	    
	    if (planIsSet){
		processFiles();
	    }
	    
	    
	    
	}
	/**
	 * this method should be called after the new plan is delivered
	 * at the rest of time the files are being processed when a new 
	 * event occurs.
	 */
	private void processFiles(){	    
	  write(" DEBUG: Inside processFiles()");  
	  //PROCESS WAITING INPUT FILES
	  DPGridlet gl;		
	  //first send jobs to free CPUs
	  while (waitingInputFiles.size() > 0 && localProcessingFlow > 0 
	    && resource_.getNumFreePE() > 0 && freeStorageSpace > 0) {
	      write(" DEBUG: Inside processFiles(): first send jobs to free CPUs");   
	    gl = (DPGridlet) waitingInputFiles.poll(); //this removes gl from the list	    
	    if (! submitInputFile(gl) ){ 
	      //failed to submit job (probably not enough space for output)
	      waitingInputFiles.add(gl); //return not submitted gridlet back to list
	      break;			
	    }	    
	  }
	  //then forward the rest for remote processing
	  while (waitingInputFiles.size() > 0 && remoteInputFlow > 0){
	      write(" DEBUG: Inside processFiles(): then forward the rest for remote processing");
	      gl = (DPGridlet) waitingInputFiles.poll(); //this removes gl from the list
	      if ( ! transferInputFile(gl) ){
		      //failed to transfer input file (probably now links with flow > 0)
		      waitingInputFiles.add(gl); //return not submitted gridlet back to list
		      break;	
	      }
	  }	    
	  //PROCESS READY OUTPUT FILES
	  while (readyOutputFiles.size() > 0 && remoteOutputFlow > 0){
	      write(" DEBUG: Inside processFiles(): process ready outputfiles");
	      gl = (DPGridlet) readyOutputFiles.poll(); //this removes gl from the list
	      if ( ! transferOutputFile(gl) ){
		  //failed to transfer output file (probably now links with flow > 0)
		  readyOutputFiles.add(gl); //return not submitted gridlet back to list
		  break;	
	      }
	  }	
	  
	    
	  write(" DEBUG: processFiles() exited");
	    return;
	}

	/**
	 * submit a gridlet for execution, create an output file and update counters.
	 * This changes the gridlet ID in order to receive it from AllocPolicy class
	 *  after it was finished
	 * @param gl
	 * @return
	 */
	private boolean submitInputFile(DPGridlet gl) {
	    write(" DEBUG: Inside submitInputFile()");
	    if (createOutputFile(gl) ){ // if space for outputfile was successfully created
	        waitingInputSize -= gl.getInputSizeInMB(); 
	        this.submittedInputFiles.add(gl);
	        //// THIS IS THE HACK:
	        //we change the gridlet user ID in order to receive it when it's done 
	        //gl.setUserID(super.get_id()); 
	        write("Setting gridlet userID to " +Integer.toString(gl.getUserID() )  );
	        policy_.gridletSubmit(gl, false); // submit to policy for execution 
	        this.localProcessingFlow -= gl.getOutputSizeInMB(); //decrease the counter
	        return true;
	    }else{		
	        write("WARNING failed to submit file");	           
	        return false;    
	    }		    
	}


	/**
	 * send an input file to the remote node over the link with flow > 0
	 * update counters
	 * @param gl
	 * @return true if success
	 */
	private boolean transferInputFile(DPGridlet gl) {
	    //select destination
	    int j = -1; //number of destination in the lists
	    for (int i = 0; i < neighborNodesInputFlows.size(); i++ ){
		//select the first dest node with flow > 0
		if (neighborNodesInputFlows.get(i) > 0){
		    j = i;
		    break;
		}		
	    }
	    if (j == -1){//destination not found
		write("cant find a valid destination for input transfer");
		return false;
	    }
	    
	    //send	    
	    write(" sending input file " + gl.getGridletID() + " to resource " + neighborNodesIds.get(j));
	    send(super.output, GridSimTags.SCHEDULE_NOW, RiftTags.INPUT,
	          new IO_data(gl, gl.getGridletFileSize(), neighborNodesIds.get(j), 0) );
	    //update counters
	    double size = gl.getInputSizeInMB();
	    remoteInputFlow -=  size;
	    neighborNodesInputFlows.set(j, neighborNodesInputFlows.get(j) - size);
	    waitingInputSize -= size;
	    freeStorageSpace +=  size; //this "deletes" the file, 
	    //later file has to be deleted when a confirmation is received    	 
	    return true;
	}
	
	/**
	 * send an OUTPUT file to the remote node over the link with flow > 0
	 * update counters
	 * @param gl
	 * @return true if success
	 */
	private boolean transferOutputFile(DPGridlet gl) {
	    //select destination
	    int j = -1; //number of destination in the lists
	    for (int i = 0; i < neighborNodesOutputFlows.size(); i++ ){
		//select the first dest node with flow > 0
		if (neighborNodesOutputFlows.get(i) > 0){
		    j = i;
		    break;
		}		
	    }
	    if (j == -1){//destination not found
		write("cant find a valid destination for output transfer");
		return false;
	    }
	    
	    //send	    
	    write("sending output file " + gl.getGridletID() + " to resource " + neighborNodesIds.get(j));
	    send(super.output, GridSimTags.SCHEDULE_NOW, RiftTags.CONFIRMATION_OUTPUT,
	          new IO_data(gl, gl.getGridletOutputSize(), neighborNodesIds.get(j), 0) );
	    //update counters
	    double size = gl.getOutputSizeInMB();
	    remoteOutputFlow -=  size;
	    neighborNodesOutputFlows.set(j, neighborNodesOutputFlows.get(j) - size);
	    readyOutputSize -= size;
	    freeStorageSpace +=  size; //this "deletes" the file, 
	    //later file has to be deleted when a confirmation is received    	 
	    return true;
	}


	/**
	 * Store a new incoming input file to storage
	 * @param gl gridlet object, containing info on this file
	 * @return true if success, false if not
	 */
	private boolean addInputFile(DPGridlet gl){
	    double size = gl.getInputSizeInMB();
	    if (this.freeStorageSpace >= size ) {
		this.freeStorageSpace -= size;
		this.waitingInputSize += size;
		this.waitingInputFiles.add(gl);
		return true;
		
	    }else{
		//failed to accommodate a new file
		write("WARNING RUNNING OUT OF STORAGE SPACE,"
			+ " can't accomodate new input file, freeSpace: " 
			+ this.freeStorageSpace + " fileSize: "
			+ size);
		return false;
	    }
	}
	
	
	/** Reserve space for a new output file
	 * @param gl
	 * @return
	 */
	private boolean createOutputFile(DPGridlet gl){
	    double outSize = gl.getOutputSizeInMB();	    
	    if (this.freeStorageSpace >= outSize ) { //if there is a place for output
		this.freeStorageSpace -= outSize; //reserve space for output
		this.reservedOutputFiles.add(gl); //add the file to the future output list		
		return true;		
	    }else{
		//failed to accommodate a new file
		write("WARNING RUNNING OUT OF STORAGE SPACE, can't create an output file");
		return false;
	    }
	}


	/**updates the local part of the plan
	 * reset counters
	 * 
	 * @param ev
	 */
	private void processNewPlan(Sim_event ev) {
	    localProcessingFlow = 100000;// (MB) for testing
	    //link to the central storage
	    neighborNodesIds.add(planerId);
	    neighborNodesInputFlows.add(0.0);// (MB) for testing
	    neighborNodesOutputFlows.add(100000.0);// (MB) for testing
	    
	    //set counters
	    remoteInputFlow = 0;
	    remoteOutputFlow = 0;
	    for (int i = 0; i < neighborNodesIds.size(); i++){
		remoteInputFlow += neighborNodesInputFlows.get(i);
		remoteOutputFlow += neighborNodesOutputFlows.get(i);
	    }
	    this.planIsSet = true;
	    
	    processFiles(); // after the new plan is setup we have to process what was left before
	}

	/**
	 * Sends node status (required for planning)
	 * to the central planer as a response to status request
	 * @param ev
	 */
	private void processStatusRequest(Sim_event ev) {
	    planerId = (Integer) ev.get_data();
	    write("planerId is: " + planerId);
	    
	    //create status message
	    Map status = new HashMap();
	    status.put("nodeId", super.get_id());
	    status.put("nodeName", super.get_name());
	    status.put("freeStorageSpace", this.freeStorageSpace);
	    status.put("readyOutputSize", this.readyOutputSize);
	    status.put("waitingInputFiles", this.waitingInputFiles);
	    
            send(super.output, GridSimTags.SCHEDULE_NOW, RiftTags.STATUS_RESPONSE,
                    new IO_data(status, 0, planerId, 0) 
               );
	    
	    
	    return;
	}
	
	


	/**generate a string with event details
	 * 
	 * @param ev
	 * @return
	 */
	private String verboseEvent(Sim_event ev){
	    StringBuffer buf = new StringBuffer();
	    
	    buf.append("tag:" + ev.get_tag()+ " ");
	    buf.append("src:" + ev.get_src()+ " ");
	    buf.append("dest:" + ev.get_dest()+ " ");

	    return buf.toString();
	}
	
	/**method for displaying output
	 * 
	 * @param message
	 */
	private void write(String message){
	    StringBuffer buf = new StringBuffer();
	    
	    buf.append(GridSim.clock() + " ");
	    buf.append( super.get_name() + ":" );
	    buf.append(super.get_id() + " ");	    
	    buf.append(message);
	    
	    System.out.println(buf.toString());
	    
	    return;
	}
}
