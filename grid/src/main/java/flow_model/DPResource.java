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
    	private ArrayList neighborNodesIds = new ArrayList(); //IDs of directly connected nodes
    	private ArrayList neighborNodesFlows = new ArrayList(); //amoount of data to send to each node
    	private double localProcessingFlow; //input data to be processed locally
    

	public DPResource(String name, Link link, ResourceCharacteristics resource, double storageSize, ResourceCalendar calendar, ReplicaManager replicaManager) throws Exception {
		super(name, link, resource, calendar, replicaManager);
		this.storageSize = storageSize;
		this.freeStorageSpace = this.storageSize;
		this.waitingInputSize = 0;
		this.readyOutputSize = 0;
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
	                //processIncommingOutputFile(ev);
	                break;     
	                
	            default:
	        	//send the unknown event to scheduler;
	        	policy_.processOtherEvent(ev);
	                break;
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
	    
	    
	    
	}
	
	private void processFiles(){
	    
	    //PROCESS WAITING INPUT FILES
	    DPGridlet gl;
	    if (waitingInputFiles.size() > 0){
		
		//local processing
		//if localProcessingFlow > 0 and there is free CPU -> submit and disk not full
		while ( localProcessingFlow > 0 && this.resource_.getNumFreePE() > 0 && this.freeStorageSpace > 0) {
		    gl = (DPGridlet) waitingInputFiles.poll(); //this removes gl from the list
		    if (addOutputFile(gl) ){ // if space for outputfile was successfully created
			waitingInputSize -= gl.getInputSizeInMB(); 
			this.submittedInputFiles.add(gl);
			policy_.gridletSubmit(gl, false); // submit to policy for execution 
			this.localProcessingFlow -= gl.getOutputSizeInMB(); //decrease the counter
		    }else{
			//failed to create output file (not enough place)
			waitingInputFiles.add(gl); //return not submitted gridlet back to list
			break;			
		    }	    
		}
		//forward for remote processing

	    }    
	    
	    //PROCESS READY OUTPUT FILES
	    
	    
	    return;
	}

	/**
	 * Tries to store an input file of a gridlet to the storage,
	 * updates storage space counters
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
		write("WARNING RUNNING OUT OF STORAGE SPACE,"
			+ " can't accomodate new input file, freeSpace: " 
			+ this.freeStorageSpace + " fileSize: "
			+ size);
		//failed to accommodate a new file
		return false;
	    }
	}
	/** Submits a gridlet for local processing,
	 * reserves space for the output, adds to submittedInputFiles 
	 * @param gl
	 * @return
	 */
	private boolean addOutputFile(DPGridlet gl){
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
	 * 
	 * @param ev
	 */
	private void processNewPlan(Sim_event ev) {
	    this.localProcessingFlow = 100000;// (MB) for testing
	    
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
