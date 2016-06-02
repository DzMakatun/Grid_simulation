package pullModel;
import java.util.List;
import java.util.Map;

import eduni.simjava.Sim_event;
import flow_model.*;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import gridsim.GridletList;
import gridsim.IO_data;
import push_model.*;

public class PullSpaceShared extends FastSpaceShared {
    private boolean displayMessages = true;
    //properties to definy the type of the resource
    private boolean isInputSource;
    private boolean isOutputDestination;
    private boolean isInputDestination;
    private boolean isOutputSource;
    
    private double storageSize; // (UNITS) total size of storage
    private double freeStorageSpace; //(UNITS) available free space
    private double waitingInputSize; //(UNITS) input data that can be transferred or processed
    private double readyOutputSize; //(UNITS) output data ready to be transferred
    private int initialNomberOfFiles = 0;
    private double initialSizeOfFiles = 0;
    
    private GridletList waitingInputFiles = new GridletList(); //Gridlets That has just arrived
    private GridletList readyOutputFiles = new GridletList(); //output files ready for transfer
    
    List<Integer> inputSourcesIds, outputDestinationsIds;
    Map<Integer, Double> pingMap;
    

    public PullSpaceShared(String resourceName, String entityName, double storageSize, 
	boolean isInputSource, boolean isOutputDestination,
	boolean isInputDestination, boolean isOutputSource) throws Exception {
	super(resourceName, entityName);
	this.storageSize = storageSize;
	this.freeStorageSpace = this.storageSize;
	this.waitingInputSize = 0;
	this.readyOutputSize = 0;
	this.isInputSource = isInputSource;
	this.isOutputDestination = isOutputDestination;
	this.isInputDestination = isInputDestination;
	this.isOutputSource = isOutputSource;
    }
    
    @Override
    public String getStatFilename(){
	return "output/" + DataUnits.getPrefix() + "_" + this.resName_ + "_PULL_stat.csv";
    }
    
    @Override
    public String getStatusHeader(){
	    String indent = " ";
	    StringBuffer buf = new StringBuffer();	    
	    buf.append("time" + indent);
	    buf.append("busyCPUs" + indent);
	    buf.append("jobQueue" + indent);
	    buf.append("waitingInputSize" + indent);
	    buf.append("readyOutputSize" + indent);
	    //buf.append( + indent);	  
	    return buf.toString();
    }
	
    @Override
    public String getStatusString(){
	    String indent = " ";
	    StringBuffer buf = new StringBuffer();	    
	    buf.append(GridSim.clock() + indent);
	    buf.append(    ( (double) (this.resource_.getNumBusyPE() )
		    / (double) this.resource_.getNumPE() )   + indent);	   
	    buf.append(    ( (double) (super.gridletQueueList_.size() )
		    / (double) this.resource_.getNumPE() )   + indent);	   
	    buf.append( (this.waitingInputSize  / this.storageSize) + indent);
	    buf.append( (this.readyOutputSize  / this.storageSize) + indent);	    
	    //buf.append( + indent);	    
	    return buf.toString();
    }
    
    public boolean addInitialInputFiles(GridletList list){
	DPGridlet gridlet;
	for (int i = 0; i < list.size(); i++){
	    gridlet = (DPGridlet) list.get(i);
	    gridlet.setUserID(this.resId_);
	    if ( ! addInputFile( gridlet ) ){
		//if failed to add input file
		//return false;
		System.exit(13);
	    }else{
		LFNmanager.registerInputFile(gridlet.getGridletID(), this.resName_);
	    }
	}
	this.initialNomberOfFiles = this.waitingInputFiles.size();
	this.initialSizeOfFiles = this.waitingInputSize;
	return true;
    }
    
	/**
	 * Store an input file to storage
	 * @param gl gridlet object, containing info on this file
	 * @return true if success, false if not
	 */
	private boolean addInputFile(Gridlet gl){
	    double size = gl.getGridletFileSize();
	    if (this.freeStorageSpace >= size ) {
		this.freeStorageSpace -= size;
		this.waitingInputSize += size;
		this.waitingInputFiles.add(gl);
		//write("remaining disk space: " + freeStorageSpace);
		if (this.isInputSource){
		    LFNmanager.unCheckFile(gl.getGridletID());
		}
		
		return true;
		
	    }else{
		//failed to accommodate a new file
		//write("WARNING RUNNING OUT OF STORAGE SPACE,"
		//	+ " can't accomodate new input file, freeSpace: " 
		//	+ this.freeStorageSpace + " fileSize: "
		//	+ size);
		return false;
	    }
	}
	
	/** Add an output file to storage 
	 * @param gl
	 * @return true if success
	 */
	private boolean addOutputFile(Gridlet gl) {
	    double size = gl.getGridletOutputSize();
	    if (this.freeStorageSpace >= size ) {
		this.freeStorageSpace -= size;
		this.readyOutputSize += size;
		this.readyOutputFiles.add(gl);
		//if (this.isInputSource){
		//    write("received output file for" + gl.getGridletID());
		//}
		//write("remaining disk space: " + freeStorageSpace);
		return true;
		
	    }else{
		//failed to accommodate a new file
		//write("WARNING RUNNING OUT OF STORAGE SPACE,"
		//	+ " can't accomodate new input file, freeSpace: " 
		//	+ this.freeStorageSpace + " fileSize: "
		//	+ size);
		return false;
	    }
	}
	
	@Override
	public void processOtherEvent(Sim_event ev)
	  {
	      if (ev == null)
	      {
	          System.out.println(super.get_name() + ".processOtherEvent(): " +
	                  "Error - an event is null.");
	          return;
	      }

	      	//write("Received event: " + verboseEvent(ev) );
	      	
	      	//extract event tag
	      	int tag = ev.get_tag();
	      	
	      	switch (tag)
	        {            
	            case RiftTags.INPUT: //submit input file to resource
	        	write("received input file");	        	
	                processIncommingInputFile(ev);
	                break;    

	            case RiftTags.OUTPUT: //save output file to the storage
	        	write("received output file");
	                processIncommingOutputFile(ev);
	                break;              
	                
	            case RiftTags.JOB_REQUEST: //send input files in responce
	        	write("received JOB_REQUEST");
	                processJobRequest(ev);
	                break;              
	                
	            case RiftTags.JOB_REQUEST_DECLINE: //switch to the next source
	        	write("received JOB_REQUEST_DECLINE");
	                processDecline(ev);
	                break;              
	                
	            default:
	        	write("Unknown event received. tag: " + ev.get_tag());
	                break;
	        }        
	    }
	
	    private void processDecline(Sim_event ev) {
		if (this.isInputDestination){
		    JobRequestDecline dec = (JobRequestDecline) ev.get_data();
		 // TODO Auto-generated method stub
		    
		    
		}else{
		    write("error: job_request_decline send to wrong destination");
		}	    
	}

	    private void processJobRequest(Sim_event ev) {
		if (this.isInputSource){
		    JobRequest req = (JobRequest) ev.get_data();
		 // TODO Auto-generated method stub
		    
		}else{
		    write("error: job request send to wrong destination");
		}	    
	}

	    /**
	     * Sends the completed to the output destination
	     * @param gl  a completed Gridlet object
	     * @return <tt>true</tt> if the Gridlet has been sent successfully,
	     *         <tt>false</tt> otherwise
	     * @pre gl != null
	     * @post $none
	     */
	    @Override
	    protected boolean sendFinishGridlet(Gridlet gl)
	    {
		//TODO !!!!!!!!!!!
		int dest = 0; //define destination for output
	        IO_data obj = new IO_data(gl,gl.getGridletOutputSize(),dest);
	        super.sim_schedule(outputPort_, GridSimTags.SCHEDULE_NOW, GridSimTags.GRIDLET_RETURN, obj);
	        return true;
	    }
	
	private void processIncommingInputFile(Sim_event ev) {
	    if (this.isInputDestination){
		Gridlet gl = (Gridlet) ev.get_data();
		super.gridletSubmit(gl, false);
	    }else{
		write("error: input file send to wrong destination");
	    }	    
	}

	/**
	 * store processed output file
	 * @param ev
	 */
	private void processIncommingOutputFile(Sim_event ev) {
	    if (this.isOutputDestination){
		Gridlet gl = (Gridlet) ev.get_data();
		addOutputFile(gl);
		//TODO
		//send next input file
	    }else{
		write("error: ouput file send to wrong destination");
	    }	    
	}

	private void write(String message){
	    String indent = " ";
	    if (displayMessages){
		System.out.println(GridSim.clock() + indent + this.resName_ + indent + message);
	    }
	}

}
