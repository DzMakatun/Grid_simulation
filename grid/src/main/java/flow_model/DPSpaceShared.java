/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * License:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 */

package flow_model;

import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;
import gridsim.AllocPolicy;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import gridsim.GridletList;
import gridsim.IO_data;
import gridsim.Machine;
import gridsim.MachineList;
import gridsim.PE;
import gridsim.PEList;
import gridsim.ResGridlet;
import gridsim.ResGridletList;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.joda.time.DateTime;


/**
 * DPSpaceShared class adopts functionality of SpaceShared for the needs of
 * Data Production simulations. It implements the handler logic for processing
 * of incoming input/output files according to plan, received from the planner.
 * Unfortunately, the SpaceShared is now set to public, thus can't be extended,
 * for that reason part of the code was just copied.
 * The main difference: overrided method gridletFinish which sends finished job to the 
 * Resource entity instead of user.
 * SpaceShared class is an allocation policy for GridResource that behaves
 * exactly like First Come First Serve (FCFS). This is a basic and simple
 * scheduler that runs each Gridlet to one Processing Element (PE).
 * If a Gridlet requires more than one PE, then this scheduler only assign
 * this Gridlet to one PE.
 *
 * @author	 DzmitryMakatun
 * @author       Manzur Murshed and Rajkumar Buyya
 * @author       Anthony Sulistio (re-written this class)
 * @author       Marcos Dias de Assuncao (has made some methods synchronized)
 * @since        GridSim Toolkit 2.2
 * @see gridsim.GridSim
 * @see gridsim.ResourceCharacteristics
 * @invariant $none
 */
public class DPSpaceShared extends AllocPolicy
{
    private ResGridletList gridletQueueList_;     // Queue list
    private ResGridletList gridletInExecList_;    // Execution list
    private ResGridletList gridletPausedList_;    // Pause list
    private double lastUpdateTime_;    // the last time Gridlets updated
    private int[] machineRating_;      // list of machine ratings available
    
    private int planerId;
    private boolean planIsSet;
    
    //properties to definy the type of the resource
    private boolean isInputSource;
    private boolean isOutputDestination;
    private boolean isInputDestination;
    private boolean isOutputSource;
    
    //properties to maitain the storage functionality
    // and manage input/output files
    private double storageSize; // (UNITS) total size of storage
    private double freeStorageSpace; //(UNITS) available free space
    private double waitingInputSize; //(UNITS) input data that can be transferred or processed
    private double readyOutputSize; //(UNITS) output data ready to be transferred
    private double submittedInputSize; //(UNITS) size of input files of submitted jobs
    private double reservedOutputSize; //(UNITS) size of output files of submitted jobs
    private double pendingInputSize; //(UNITS) size of Input files in process of transfer
    private double pendingOutputSize; //(UNITS) size of Output files in process of transfer
    int sendErrorCounter; //how many outgoing transfers were failed 
    int receiveErrorCounter; //how many incoming transfers were failed 
    private double incommingTransferFailureFlag = 0;
    private double outgoingTransferFailureFlag = 0;
    private double jobSubmissionFailureFlag = 0;
    
	
    private GridletList waitingInputFiles = new GridletList(); //Gridlets That has just arrived
    private GridletList submittedInputFiles = new GridletList(); //Gridlets submited for local processing
    private GridletList readyOutputFiles = new GridletList(); //output files ready for transfer
    private GridletList reservedOutputFiles = new GridletList(); //output files of runing jobs
    private GridletList pendingInputFiles = new GridletList(); //Input files in process of transfer
    private GridletList pendingOutputFiles = new GridletList(); //Output files in process of transfer
	
    //properties to store the plan
    private ArrayList<Integer> neighborNodesIds = new ArrayList<Integer>(); //IDs of directly connected nodes
    private ArrayList<Double> neighborNodesInputFlows = new ArrayList<Double>(); //amount of data to send to each node
    private ArrayList<Double> neighborNodesOutputFlows = new ArrayList<Double>(); //amount of data to send to each node
    private ArrayList<LinkFlows> outgoingLinkFlows = new ArrayList<LinkFlows>(); //to keep statistics of links
    private double localProcessingFlow; //(UNITS) input data to be processed locally (counter)
    private double remoteInputFlow; //(UNITS) total input data to be transferred out (counter)
    private double remoteOutputFlow; //(UNITS) total output data to be transferred out (counter)
    
    //for statistics purposes
    private double firstPlanReceived = 0;
    private double lastOutputReceived = 0;
    private double lastOutputSend = 0;
    private int initialNomberOfFiles = 0;
    private double initialSizeOfFiles = 0;
    private int jobsFinished = 0;
    private double inputReceived = 0;
    private double outputSent = 0;
    private int waistedCPUCounter = 0;
    //writing statistics to a file
    private PrintWriter fileWriter; 

    
    


    /**
     * Allocates a new SpaceShared object
     * @param resourceName    the GridResource entity name that will contain
     *                        this allocation policy
     * @param entityName      this object entity name
     * @throws Exception This happens when one of the following scenarios occur:
     *      <ul>
     *          <li> creating this entity before initializing GridSim package
     *          <li> this entity name is <tt>null</tt> or empty
     *          <li> this entity has <tt>zero</tt> number of PEs (Processing
     *              Elements). <br>
     *              No PEs mean the Gridlets can't be processed.
     *              A GridResource must contain one or more Machines.
     *              A Machine must contain one or more PEs.
     *      </ul>
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @pre resourceName != null
     * @pre entityName != null
     * @post $none
     */
    DPSpaceShared(String resourceName, String entityName, double storageSize, 
	    boolean isInputSource, boolean isOutputDestination,
	    boolean isInputDestination, boolean isOutputSource) throws Exception
    {
        super(resourceName, entityName);
        // initialises local data structure
        this.gridletInExecList_ = new ResGridletList();
        this.gridletPausedList_ = new ResGridletList();
        this.gridletQueueList_  = new ResGridletList();
        this.lastUpdateTime_ = 0.0;
        this.machineRating_ = null;
        this.planIsSet = false;
	this.storageSize = storageSize;
	this.freeStorageSpace = this.storageSize;
	this.waitingInputSize = 0;
	this.readyOutputSize = 0;
	this.pendingInputSize = 0;
	this.pendingOutputSize = 0;
	this.submittedInputSize = 0;
	this.reservedOutputSize = 0;
	this.localProcessingFlow = 0.0; 
	this.remoteInputFlow = 0.0; 
	this.remoteOutputFlow = 0.0;
	this.isInputSource = isInputSource;
	this.isOutputDestination = isOutputDestination;
	this.isInputDestination = isInputDestination;
	this.isOutputSource = isOutputSource;
	
	String filename = "output/" + this.resName_ + "_statistics.csv";
	fileWriter = new PrintWriter(filename, "UTF-8");
	fileWriter.println(getStatusHeader() );
	//fileWriter.println(getStatusString() );

    }
    
    /**
     *  
     * @param list
     * @return sum of input files in gridletlist
     */
    private double getInputSize(GridletList list){
	double sum = 0;
	Gridlet gl;
	for (Object obj : list){
	    gl = (Gridlet) obj;
	    sum += gl.getGridletFileSize();
	}
	return sum;
    }
    
    /**
     *  
     * @param list
     * @return sum of input files in gridletlist
     */
    private double getOutputSize(GridletList list){
	double sum = 0;
	Gridlet gl;
	for (Object obj : list){
	    gl = (Gridlet) obj;
	    sum += gl.getGridletOutputSize();
	}
	return sum;
    }
    
    /**
     * Checks the size of gridlet lists
     * @throws Exception 
     */
    private void verifyGridletLists() throws Exception{
	if( this.waitingInputSize == getInputSize(this.waitingInputFiles)
		&& this.submittedInputSize == getInputSize(this.submittedInputFiles)
		&& this.pendingInputSize== getInputSize(this.pendingInputFiles)
		&& this.readyOutputSize == getOutputSize(this.readyOutputFiles)
		&& this.reservedOutputSize == getOutputSize(this.reservedOutputFiles)
		&& this.pendingOutputSize == getOutputSize(this.pendingOutputFiles)
		&& ( waitingInputSize + submittedInputSize + pendingInputSize + readyOutputSize + reservedOutputSize
		       + pendingOutputSize + freeStorageSpace == storageSize )
		){
	    		write("Gridlet lists verification passed ..............OK");
	}else{
	    while(true){
	      write("\n \n \n  \n \n \n  GRIDLET LIST VERIFICATION FAILED \n \n \n \n \n \n ");
	    }
	    //throw new Exception(); 
	}
	
    }
    
    public boolean  isInputSource(){
	return this.isInputSource;
    }
    
    public boolean isOutputDestination(){
	return this.isOutputDestination;
    }
    
    public boolean addInitialInputFiles(GridletList list){
	DPGridlet gridlet;
	for (int i = 0; i < list.size(); i++){
	    gridlet = (DPGridlet) list.get(i);
	    gridlet.setUserID(this.resId_);
	    if ( ! addInputFile( gridlet ) ){
		//if failed to add input file
		return false;
	    }
	}
	this.initialNomberOfFiles = this.waitingInputFiles.size();
	this.initialSizeOfFiles = this.waitingInputSize;
	return true;
    }
    
    /**
     * Returns size of storage in (Units),
     * 
     * @return
     */
    public double getStorageSize(){
	return this.storageSize;
    }
    
    /**
    * provides summary about the resource as a string
    */
    public String paramentersToString(){
	StringBuffer br = new StringBuffer();		
	br.append("name: " + super.resName_ + ", ");
	br.append("id: " + super.resId_ + ", ");
	br.append("PEs: " + resource_.getMachineList().getNumPE() + ", ");
	br.append("storage: " + storageSize + " " + DataUnits.getName() + ", ");		
	br.append("MIPSrating: " + resource_.getMIPSRatingOfOnePE()  + ", ");
	//br.append("isInputSource: " + isInputSource()  + ", ");
	//br.append("isOutputDestination: " +isOutputDestination()  + ", ");	
	//br.append("isInputDestination: " +this.isInputDestination  + ", ");
	//br.append("isOutputSource: " + this.isOutputSource  + ", ");
	br.append("nodeType: ");
	if (this.isInputSource)
	    br.append("1");
	else			
	    br.append("0");
	if (this.isOutputDestination)
	    br.append("1");
	else			
	    br.append("0");
	if (this.isInputDestination)
	    br.append("1");
	else			
	    br.append("0");
	if (this.isOutputSource)
	    br.append("1");
	else			
	    br.append("0");	
	
	br.append(" waitingInputSize: " + waitingInputSize  + ", ");
	//br.append("out_port: " + super.outputPort_.getClass()  + ", ");
	//br.append("inputID: "  + GridSim.getEntityId("Input_" + super.resName_) + ", ");
	//br.append("outputID: "  + GridSim.getEntityId("Output_" + super.resName_) + ", ");
	//br.append("link bandwidth: " + super.outputPort_.  + "(bit/s), ");		
	//br.append("stat: " + super.get_stat().toString());
	return br.toString();
		
    }    
    
    /**
	 * Here is the main logic of network flow model execution.
	 * The "handler" (i.e this resource) processes dedicated events
	 * events can be passed to underlying sched. policy.
	 */
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
	            case RiftTags.STATUS_REQUEST:
	        	//write("received status request");
	                processStatusRequest(ev);
	                break;

	            case RiftTags.NEW_PLAN:
	        	//write("received new plan");
	                processNewPlan(ev);
	                break;
	                
	            case RiftTags.INPUT:
	        	//write("received input file");	        	
	                processIncommingInputFile(ev);
	                break;    

	            case RiftTags.OUTPUT:
	        	//write("received output file");
	                processIncommingOutputFile(ev);
	                break;     
	                
	            case RiftTags.INPUT_TRANSFER_ACK:
	        	write("received INPUT_TRANSFER_ACK");
	        	processInputTransferAck(ev);
	                break;    
	            
	            case RiftTags.OUTPUT_TRANSFER_ACK:
	        	write("received OUTPUT_TRANSFER_ACK");
	        	processOutputTransferAck(ev);
	                break;  
	           

	                
	            case RiftTags.INPUT_TRANSFER_FAIL:
	        	write("received INPUT_TRANSFER_FAIL");
	        	processInputTransferFail(ev);
	                break;  
	                
	            case RiftTags.OUTPUT_TRANSFER_FAIL:
	        	write("received OUTPUT_TRANSFER_FAIL");
	        	processOutputTransferFail(ev);
	                break;  
	                
	                
	                
             
	            default:
	        	write("Unknown event received. tag: " + ev.get_tag());
	                break;
	        }        
	    }
	
	/**
	 * If input transfer to the remote resource failed
	 * return the file to the waiting input queue
	 * @param ev
	 */
	private void processInputTransferFail(Sim_event ev) {
	    this.sendErrorCounter ++;
	    this.outgoingTransferFailureFlag = 1.0;
	    DPGridlet gl = (DPGridlet) ev.get_data();
	    write("INPUT TRANSFER FAILURE, gridlet id: " + gl.getGridletID());
	    if (this.pendingInputFiles.remove(gl)) {
		double size = gl.getGridletFileSize();
		this.pendingInputSize -= size;
		this.waitingInputFiles.add(gl);
		this.waitingInputSize += size;
		
	    }else{
		write("ERROR PENDING INPUT FILE NOT REGISTERED: " + gl.getGridletID());
	    }
	    processFiles();
    }


	/**
	 * If output transfer to the remote resource failed
	 * return the file to the ready output queue
	 * @param ev
	 */
	private void processOutputTransferFail(Sim_event ev) {
	    this.sendErrorCounter ++;
	    this.outgoingTransferFailureFlag = 1.0;
	    DPGridlet gl = (DPGridlet) ev.get_data();
	    if (this.pendingOutputFiles.remove(gl)) {
		double size = gl.getGridletOutputSize();
		this.pendingOutputSize -= size;
		this.readyOutputFiles.add(gl);
		this.readyOutputSize += size;
	    }else{
		write("ERROR PENDING OUTPUT FILE NOT REGISTERED: " + gl.getGridletID());
	    }	
	    //fileWriter.println(getStatusString());
	    processFiles();
    }

	/**
	 * When transfer to remote destination succeed, remove
	 * pending file; 
	 * @param ev
	 */
	private void processOutputTransferAck(Sim_event ev) {
	    DPGridlet gl = (DPGridlet) ev.get_data();
	    if (this.pendingOutputFiles.remove(gl)) {
		double size = gl.getGridletOutputSize();
		this.pendingOutputSize -= size;
		this.freeStorageSpace += size; //remove the file from storage
		gl.getUsedLink().addOutputTransfer(size);//update link statistics
		//statistics
		lastOutputSend = GridSim.clock(); 
		this.outgoingTransferFailureFlag = 0.0;
	    }else{
		write("ERROR PENDING OUTPUT FILE NOT REGISTERED: " + gl.getGridletID());
	    }	
	    processFiles();
    }
	
	/**
	 * When transfer to remote destination succeed, remove
	 * pending file; 
	 * @param ev
	 */
	private void processInputTransferAck(Sim_event ev) {
	    DPGridlet gl = (DPGridlet) ev.get_data();
	    if (this.pendingInputFiles.remove(gl)) {
		double size = gl.getGridletFileSize();
		this.pendingInputSize -= size;
		this.freeStorageSpace += size; //remove the file from storage
		//statistics
		gl.getUsedLink().addInputTransfer(size);//update link statistics
		this.outgoingTransferFailureFlag = 0.0;
	    }else{
		write("ERROR PENDING INPUT FILE NOT REGISTERED: " + gl.getGridletID());
	    }
	    processFiles();
    }

	private void processIncommingOutputFile(Sim_event ev) {
	    DPGridlet gl = (DPGridlet) ev.get_data();
	    write("received output file of a gridlet " + gl.getGridletID());
	    //add new input file to
	    if (addOutputFile(gl) ){
		//send confirmation to sender
		//send without network delay
		super.sim_schedule(gl.getSenderID(), GridSimTags.SCHEDULE_NOW,
			RiftTags.OUTPUT_TRANSFER_ACK, gl); 
		//remember the time when the last output was received. For makespan calculation
		this.lastOutputReceived = GridSim.clock();
		this.incommingTransferFailureFlag = 0.0;
	    }else{
		this.receiveErrorCounter ++;
		this.incommingTransferFailureFlag = 1.0;
		//send failure back to sender 
		//send without network delay		
		super.sim_schedule(gl.getSenderID(), GridSimTags.SCHEDULE_NOW,
			RiftTags.OUTPUT_TRANSFER_FAIL, gl); 
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
	    double size = gl.getOutputSizeInUnits();
	    if (this.freeStorageSpace >= size ) {
		this.freeStorageSpace -= size;
		this.readyOutputSize += size;
		this.readyOutputFiles.add(gl);
		//write("remaining disk space: " + freeStorageSpace);
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
	    write("received input file of gridlet" + gl.getGridletID() 
		    + " of size " + gl.getGridletFileSize() + " " + DataUnits.getName());
	    //add new input file to
	    if (addInputFile(gl) ){
		//send confirmation to sender
		//send without network delay
		
		super.sim_schedule(gl.getSenderID(), GridSimTags.SCHEDULE_NOW,
			RiftTags.INPUT_TRANSFER_ACK, gl); 
		
		//statistics
		this.inputReceived += gl.getGridletFileSize();
		this.incommingTransferFailureFlag = 0.0;
	    }else{
		this.receiveErrorCounter ++;
		this.incommingTransferFailureFlag = 1.0;
		//send failure back to sender 	
		//send without network delay
		super.sim_schedule(gl.getSenderID(), GridSimTags.SCHEDULE_NOW,
			RiftTags.INPUT_TRANSFER_FAIL, gl); 
		return;
	    }
	    
	    if (planIsSet){
		processFiles();
	    }	    
	}
	
	/**
	 * Store a new incoming input file to storage
	 * @param gl gridlet object, containing info on this file
	 * @return true if success, false if not
	 */
	private boolean addInputFile(DPGridlet gl){
	    double size = gl.getInputSizeInUnits();
	    if (this.freeStorageSpace >= size ) {
		this.freeStorageSpace -= size;
		this.waitingInputSize += size;
		this.waitingInputFiles.add(gl);
		//write("remaining disk space: " + freeStorageSpace);
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
	
	/**
	 * When the job is finished we remove an input file
	 * and change the state of output file
	 * @param gl
	 */
	private void processFinishedJob(DPGridlet gl){
	    //remove input file from the disk
	    //and check if it was registered properly
	    write("Gridlet " + gl.getGridletID() + " finished processing. free CPUS + " + this.resource_.getNumFreePE() );
	    if (this.submittedInputFiles.remove(gl) && this.reservedOutputFiles.remove(gl) ){
		this.submittedInputSize -= gl.getGridletFileSize();
		this.reservedOutputSize -= gl.getGridletOutputSize();
		this.freeStorageSpace += gl.getInputSizeInUnits(); //clear disc space
		this.readyOutputFiles.add(gl); //add output file to the ready list
		this.readyOutputSize += gl.getOutputSizeInUnits(); //update counter
		//statisctics
		this.jobsFinished++;
		
	    }else{
		write("Error: finished gridlet was unregisterred");
	    }
	    
	    processFiles(); //this will send next file for processing
	    return;
	}
	
	/**
	 * this method should be called after the new plan is delivered
	 * at the rest of time the files are being processed when a new 
	 * event occurs.
	 */
	private void processFiles(){	    
	  //write(" DEBUG: Inside processFiles()");  
	  DPGridlet gl;	
	  //PROCESS READY OUTPUT FILES
	  while (readyOutputFiles.size() > 0 && remoteOutputFlow > 0){
	      //write(" DEBUG: Inside processFiles(): process ready outputfiles");
	      gl = (DPGridlet) readyOutputFiles.poll(); //this removes gl from the list
	      if ( ! transferOutputFile(gl) ){
		  //failed to transfer output file (probably now links with flow > 0)
		  readyOutputFiles.add(gl); //return not submitted gridlet back to list
		  break;	
	      }
	  }	  
	  //PROCESS WAITING INPUT FILES
	  	
	  //first send jobs to free CPUs // && localProcessingFlow > 0  && freeStorageSpace > 0
	  while (isInputDestination == true && waitingInputFiles.size() > 0  
	    && resource_.getNumFreePE() > 0) {
	      //write(" DEBUG: Inside processFiles(): first send jobs to free CPUs");   
	    gl = (DPGridlet) waitingInputFiles.poll(); //this removes gl from the list	    
	    if (! submitInputFile(gl) ){ 
	      //failed to submit job (probably not enough space for output)
	      waitingInputFiles.add(gl); //return not submitted gridlet back to list
	      break;			
	    }	    
	  }
	  
	  //then forward the rest for remote processing
	  while (waitingInputFiles.size() > 0 && remoteInputFlow > 0){
	      //write(" DEBUG: Inside processFiles(): then forward the rest for remote processing");
	      gl = (DPGridlet) waitingInputFiles.poll(); //this removes gl from the list
	      if ( ! transferInputFile(gl) ){
		      //failed to transfer input file (probably no links with flow > 0)
		      waitingInputFiles.add(gl); //return not submitted gridlet back to list
		      break;	
	      }
	  }	    
	  
	  //check if we have waiting files and free cpus.
	  if (this.waitingInputFiles.size() > 0 && this.resource_.getNumFreePE() > 0){
	      this.waistedCPUCounter++;
	  }
	  
	  //write statistics to file
	  fileWriter.println(getStatusString() );
	  //write(" DEBUG: processFiles() exited");
	  
	  //DEBUG
	  //try {
	    //verifyGridletLists();
	//} catch (Exception e) {
	    // TODO Auto-generated catch block
	   // e.printStackTrace();
	    
	//}
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
	   
	    if (createOutputFile(gl) ){ // if space for outputfile was successfully created
	        waitingInputSize -= gl.getInputSizeInUnits(); 
	        submittedInputFiles.add(gl);
	        submittedInputSize += gl.getGridletFileSize();
	        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	        //BUGFIX (otherwise job times are not calculated)
	        gl.setResourceParameter(this.resId_,this.resource_.getCostPerSec());
	        /////////////////////////////////
	        gridletSubmit(gl, false); // submit to policy for execution 
	        localProcessingFlow -= gl.getInputSizeInUnits(); //decrease the counter
	        write(" Submitted gridlet " + gl.getGridletID() +" for processing, free CPUS: " 
	        + this.resource_.getNumFreePE());
	        jobSubmissionFailureFlag = 0.0;
	        return true;
	    }else{		
	        write("WARNING failed to submit input file for processing: "+ gl.getGridletID()
	        	+ " freeStorageSpace: " + this.freeStorageSpace
	        	+ " free CPUS: " + this.resource_.getNumFreePE());
	        jobSubmissionFailureFlag = 1.0;
	        return false;    
	    }		    
	}
	
	
	/** Reserve space for a new output file
	 * @param gl
	 * @return
	 */
	private boolean createOutputFile(DPGridlet gl){
	    double outSize = gl.getOutputSizeInUnits();	    
	    if (this.freeStorageSpace >= outSize ) { //if there is a place for output
		this.freeStorageSpace -= outSize; //reserve space for output
		this.reservedOutputFiles.add(gl); //add the file to the future output list
		this.reservedOutputSize += outSize;
		return true;		
	    }else{
		//failed to accommodate a new file
		write("WARNING RUNNING OUT OF STORAGE SPACE, can't create an output file: " + gl.getGridletID()
			+ " freeStorageSpace: " + this.freeStorageSpace);
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
	    write(" sending input file " + gl.getGridletID() + " of size " + gl.getGridletFileSize() 
		    + "(bytes) to resource " + neighborNodesIds.get(j));
	    gl.setSenderID(this.resId_);
	    gl.setUsedLink(this.outgoingLinkFlows.get(j)); //for link statistics
	    IO_data data = new IO_data(gl, gl.getGridletFileSize(), neighborNodesIds.get(j));
	    //DEBUG	   
	    //write("sending IO_data: " + data.toString());
	    super.sim_schedule(super.outputPort_, GridSimTags.SCHEDULE_NOW, RiftTags.INPUT, data);
	    
	    //update counters
	    double size = gl.getInputSizeInUnits();
	    remoteInputFlow -=  size;
	    neighborNodesInputFlows.set(j, neighborNodesInputFlows.get(j) - size);
	    waitingInputSize -= size;
	    this.pendingInputFiles.add(gl);
	    this.pendingInputSize += size;
	    //freeStorageSpace +=  size; //this "deletes" the file, 
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
	    gl.setSenderID(this.resId_);
	    gl.setUsedLink(this.outgoingLinkFlows.get(j)); //for link statistics
	    IO_data data =  new IO_data(gl, gl.getGridletOutputSize(), neighborNodesIds.get(j) );
	    //DEBUG
	    // write("sending IO_data: " + data.toString());
	    super.sim_schedule(super.outputPort_, GridSimTags.SCHEDULE_NOW, RiftTags.OUTPUT, data);
	    //GridSim.send(super.outputPort_, GridSimTags.SCHEDULE_NOW,  RiftTags.OUTPUT, data);
	    
	    //update counters
	    double size = gl.getOutputSizeInUnits();
	    remoteOutputFlow -=  size;
	    neighborNodesOutputFlows.set(j, neighborNodesOutputFlows.get(j) - size);
	    readyOutputSize -= size;
	    this.pendingOutputFiles.add(gl);
	    this.pendingOutputSize += size;
	    //freeStorageSpace +=  size; //this "deletes" the file, 	    
	    //later file has to be deleted when a confirmation is received    
	    write("send output file " + gl.getGridletID()  +" of size "  + gl.getGridletOutputSize() 
		    + "to resource " + neighborNodesIds.get(j)
		    + " remaining flow" + neighborNodesOutputFlows.get(j));	    
		
	    //statistics
	    this.outputSent += size;
	    return true;
	}



	/**updates the local part of the plan
	 * reset counters
	 * 
	 * @param ev
	 */
	private void processNewPlan(Sim_event ev) {
	    LinkedList <LinkFlows> newPlan = (LinkedList <LinkFlows>) ev.get_data();	    
	    LinkFlows tempData;
	    
	    //if this is the first plan, remember the time as the start of simulation
	    if (neighborNodesIds.isEmpty()){
		this.firstPlanReceived = GridSim.clock();
	    }
	    
	    //clear old plan
	    neighborNodesIds.clear();
	    neighborNodesInputFlows.clear();
	    neighborNodesOutputFlows.clear();
	    outgoingLinkFlows.clear();
	    for (int i = 0; i < newPlan.size(); i++) {
		tempData = newPlan.get(i);
		if (tempData.fromID == resId_) {
		    if (tempData.toID == -1){ //dummy edge
			localProcessingFlow = tempData.inputFlow;
			
		    }else{ //links to other nodes
			neighborNodesIds.add(tempData.toID);
			neighborNodesInputFlows.add(tempData.inputFlow);
			neighborNodesOutputFlows.add(tempData.outputFlow);
			outgoingLinkFlows.add(tempData);
		    }
		}
	    }    
	    
	    //set counters
	    remoteInputFlow = 0;
	    remoteOutputFlow = 0;
	    for (int i = 0; i < neighborNodesIds.size(); i++){
		remoteInputFlow += neighborNodesInputFlows.get(i);
		remoteOutputFlow += neighborNodesOutputFlows.get(i);
	    }
	    this.planIsSet = true;
	    write(planToString());
	    processFiles(); // after the new plan is setup we have to process what was left before
	}
	
	/**
	 * prints the current plan and state of the resource
	 */
	private String planToString(){
	    StringBuffer buf = new StringBuffer();
	    
	    //buf.append("\n-------------PLAN----------------\n");
	    //buf.append("all values in "+ DataUnits.getName() + "\n");
	    buf.append("localProcessingFlow: " + localProcessingFlow + "\n");
	    buf.append("remoteInputFlow: " + remoteInputFlow + " remoteOutputFlow: " + remoteOutputFlow + "\n");
	    buf.append("IDS: " + neighborNodesIds.toString() + "\n");
	    buf.append("IN:  " + neighborNodesInputFlows.toString() + "\n");
	    buf.append("OUT: " + neighborNodesOutputFlows.toString() + "\n");
	    
	    buf.append("STATUS: -------");
	    buf.append("\nfreeStorageSpace: " +freeStorageSpace + " waitingInputSize: " + waitingInputSize 
		    + " readyOutputSize: " + readyOutputSize + "\n");
	    buf.append("pending files: " + (this.pendingInputSize + this.pendingOutputSize) 
		    + " sendErrors: " + this.sendErrorCounter + " receiveErrors " + this.receiveErrorCounter);
	    buf.append("\n---------------------------------");
	    
	    return buf.toString();
	}

	/**
	 * Sends node status (required for planning)
	 * to the central planer as a response to status request
	 * @param ev
	 */
	private void processStatusRequest(Sim_event ev) {
	    planerId = (Integer) ev.get_data();
	    write("Procesing status request. planerId is: " + planerId);
	    	    
	    //create status message
	    Map status = new HashMap();
	    status.put("nodeId", super.resId_);
	    status.put("nodeName", super.resName_);
	    
	    //node role
	    status.put("isInputSource", isInputSource());
	    status.put("isOutputDestination", isOutputDestination);
	    status.put("isInputDestination", isInputDestination);
	    status.put("isOutputSource", isOutputSource);
	    
	    status.put("waitingInputSize", waitingInputSize);
	    status.put("freeStorageSpace", freeStorageSpace);
	    status.put("readyOutputSize", readyOutputSize);
	    status.put("submittedInputSize", submittedInputSize);
	    status.put("reservedOutputSize", reservedOutputSize);
	    
	    status.put("busyCPUS", this.resource_.getNumBusyPE());

	    //status.put("waitingInputFiles", waitingInputFiles);
	    
	    //send without network delay
	    super.sim_schedule(planerId, GridSimTags.SCHEDULE_NOW, RiftTags.STATUS_RESPONSE,
		    status 		    
           );
	    
	    //print statistics
	    write("~~~~~~~~~~~~~~~~~~~ REMAINING FLOWS OF OLD PLAN ~~~~~~~~~~~~~~~~~\n" 
		    + planToString() + "\n"
		    + "STATISTICS : -------------\n"
		    + getStatusHeader() + "\n"
		    + getStatusString() + "\n"
		    + "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
		    );    	    
	    
	    fileWriter.println(getStatusString() );
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
	    buf.append(" resId:" + super.resId_ + " ");	
	    buf.append(message);
	    
	    System.out.println(buf.toString());
	    //write to file
	    Logger.write(buf.toString());
	    
	    return;
	}
	
	public String getStatusHeader(){
	    String indent = " ";
	    StringBuffer buf = new StringBuffer();	    
	    buf.append("time" + indent);
	    buf.append("busyCPUs" + indent);
	    buf.append("jobSubmissionFailureFlag" + indent);
	    buf.append("incommingTransferFailureFlag" + indent);
	    buf.append("outgoingTransferFailureFlag" + indent);
	    
	    buf.append("submittedInputSize" + indent);
	    buf.append("reservedOutputSize" + indent);
	    buf.append("waitingInputSize" + indent);
	    buf.append("pendingInputSize" + indent);
	    buf.append("readyOutputSize" + indent);
	    buf.append("pendingOutputSize" + indent);	    
	    //buf.append( + indent);	    
	    return buf.toString();
	}
	
	public String getStatusString(){
	    String indent = " ";
	    StringBuffer buf = new StringBuffer();	    
	    buf.append(GridSim.clock() + indent);
	    buf.append(    ( (double) (this.resource_.getNumBusyPE())
		    / (double) this.resource_.getNumPE() )   + indent);	
	    buf.append( this.jobSubmissionFailureFlag + indent);
	    buf.append( this.incommingTransferFailureFlag + indent);
	    buf.append( this.outgoingTransferFailureFlag + indent);
	    
	    buf.append( (this.submittedInputSize  / this.storageSize) + indent);
	    buf.append( (this.reservedOutputSize  / this.storageSize) + indent);
	    buf.append( (this.waitingInputSize  / this.storageSize) + indent);
	    buf.append( (this.pendingInputSize  / this.storageSize) + indent);
	    buf.append( (this.readyOutputSize  / this.storageSize) + indent);
	    buf.append( (this.pendingOutputSize  / this.storageSize) + indent);	 
	    //buf.append( + indent);	    
	    return buf.toString();
	}
	
	private void finilize(){
	  //print statistics
	    double makespanSeconds = lastOutputReceived - firstPlanReceived; 
	    Date makespan = new Date((long) (makespanSeconds * 1000));
	    SimpleDateFormat myFormat = 
	            new SimpleDateFormat("DD 'days' HH:mm:ss");
	    
	    write( "\n##########################FINAL STATISTICS for " + this.resName_+ " ##########################3\n"
		    + " initial number of input files: " + this.initialNomberOfFiles
		          + " size: " + this.initialSizeOfFiles + " " + DataUnits.getName()
		    + "\n output files received: " + this.readyOutputFiles.size()
		          +" size: " + this.readyOutputSize + " " + DataUnits.getName()
		    		    +"\n jobsFinished: " + jobsFinished
		    + "\n inputReceived: " + inputReceived + " " + DataUnits.getName()
		    + "\n outputSent: " + outputSent + " " + DataUnits.getName()
		    + "\n waistedCPUCounter: " + waistedCPUCounter 
		       + " sending errors: " + this.sendErrorCounter + " receiving errors: " + this.receiveErrorCounter      
		    + "\n firstPlanReceived: " + firstPlanReceived
		    + "\n lastOutputSend: " + lastOutputSend		    
		    + "\n lastOutputReceived: " + lastOutputReceived
		    + "\n duration: " + makespanSeconds + " seconds or: " + myFormat.format(makespan)


		    + "\n##########################################################################################\n"
		    );    	    
	    write("exitting");
	    fileWriter.println(getStatusString());
	    fileWriter.close();
	    return;
	}
    
    ///////////////////////////////////// METHODS FROM SpaceShared CLASS ///////////////////////// 

    /**
     * Handles internal events that are coming to this entity.
     * @pre $none
     * @post $none
     */
    public void body()
    {
        // Gets the PE's rating for each Machine in the list.
        // Assumed one Machine has same PE rating.
        MachineList list = super.resource_.getMachineList();
        int size = list.size();
        machineRating_ = new int[size];
        for (int i = 0; i < size; i++) {
            machineRating_[i] = super.resource_.getMIPSRatingOfOnePE(i, 0);
        }

        // a loop that is looking for internal events only
        Sim_event ev = new Sim_event();
        while ( Sim_system.running() )
        {
            super.sim_get_next(ev);

            // if the simulation finishes then exit the loop
            if (ev.get_tag() == GridSimTags.END_OF_SIMULATION ||
                super.isEndSimulation())
            {
        	finilize();
        	
                break;
            }

            // Internal Event if the event source is this entity
            if (ev.get_src() == super.myId_ && gridletInExecList_.size() > 0)
            {
                updateGridletProcessing();   // update Gridlets
                checkGridletCompletion();    // check for finished Gridlets
            }
        }

        // CHECK for ANY INTERNAL EVENTS WAITING TO BE PROCESSED
        while (super.sim_waiting() > 0)
        {
            // wait for event and ignore since it is likely to be related to
            // internal event scheduled to update Gridlets processing
            super.sim_get_next(ev);
            System.out.println(super.resName_ +
                               ".SpaceShared.body(): ignore internal events");
        }
    }

    /**
     * Schedules a new Gridlet that has been received by the GridResource
     * entity.
     * @param   gl    a Gridlet object that is going to be executed
     * @param   ack   an acknowledgement, i.e. <tt>true</tt> if wanted to know
     *        whether this operation is success or not, <tt>false</tt>
     *        otherwise (don't care)
     * @pre gl != null
     * @post $none
     */
    public synchronized void gridletSubmit(Gridlet gl, boolean ack)
    {
        // update the current Gridlets in exec list up to this point in time
        updateGridletProcessing();

        // reset number of PE since at the moment, it is not supported
        if (gl.getNumPE() > 1)
        {
            String userName = GridSim.getEntityName( gl.getUserID() );
            System.out.println();
            System.out.println(super.get_name() + ".gridletSubmit(): " +
                " Gridlet #" + gl.getGridletID() + " from " + userName +
                " user requires " + gl.getNumPE() + " PEs.");
            System.out.println("--> Process this Gridlet to 1 PE only.");
            System.out.println();

            // also adjusted the length because the number of PEs are reduced
            int numPE = gl.getNumPE();
            double len = gl.getGridletLength();
            gl.setGridletLength(len*numPE);
            gl.setNumPE(1);
        }


        ResGridlet rgl = new ResGridlet(gl);
        //DEBUG
        //System.out.println("gridlet length: " + gl.getGridletLength() + "gridlet getGridletFinishedSoFar(): " +gl.getGridletFinishedSoFar()  + " ResGridlet getGridletLength: " + rgl.getGridletLength()
        //	+ " ResGridlet getRemainingGridletLength: " + rgl.getRemainingGridletLength());
        

        boolean success = false;

        // if there is an available PE slot, then allocate immediately
        if (gridletInExecList_.size() < super.totalPE_) {
            success = allocatePEtoGridlet(rgl);
        }

        // if no available PE then put the ResGridlet into a Queue list
        if (!success)
        {
            rgl.setGridletStatus(Gridlet.QUEUED);
            gridletQueueList_.add(rgl);
        }

        // sends back an ack if required
        if (ack)
        {
            super.sendAck(GridSimTags.GRIDLET_SUBMIT_ACK, true,
                          gl.getGridletID(), gl.getUserID()
            );
        }
    }

    /**
     * Finds the status of a specified Gridlet ID.
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @return the Gridlet status or <tt>-1</tt> if not found
     * @see gridsim.Gridlet
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    public synchronized int gridletStatus(int gridletId,int userId)
    {
        ResGridlet rgl = null;

        // Find in EXEC List first
        int found = gridletInExecList_.indexOf(gridletId, userId);
        if (found >= 0)
        {
            // Get the Gridlet from the execution list
            rgl = (ResGridlet) gridletInExecList_.get(found);
            return rgl.getGridletStatus();
        }

        // Find in Paused List
        found = gridletPausedList_.indexOf(gridletId, userId);
        if (found >= 0)
        {
            // Get the Gridlet from the execution list
            rgl = (ResGridlet) gridletPausedList_.get(found);
            return rgl.getGridletStatus();
        }

        // Find in Queue List
        found = gridletQueueList_.indexOf(gridletId, userId);
        if (found >= 0)
        {
            // Get the Gridlet from the execution list
            rgl = (ResGridlet) gridletQueueList_.get(found);
            return rgl.getGridletStatus();
        }

        // if not found in all 3 lists then no found
        return -1;
    }

    /**
     * Cancels a Gridlet running in this entity.
     * This method will search the execution, queued and paused list.
     * The User ID is
     * important as many users might have the same Gridlet ID in the lists.
     * <b>NOTE:</b>
     * <ul>
     *    <li> Before canceling a Gridlet, this method updates all the
     *         Gridlets in the execution list. If the Gridlet has no more MIs
     *         to be executed, then it is considered to be <tt>finished</tt>.
     *         Hence, the Gridlet can't be canceled.
     *
     *    <li> Once a Gridlet has been canceled, it can't be resumed to
     *         execute again since this method will pass the Gridlet back to
     *         sender, i.e. the <tt>userId</tt>.
     *
     *    <li> If a Gridlet can't be found in both execution and paused list,
     *         then a <tt>null</tt> Gridlet will be send back to sender,
     *         i.e. the <tt>userId</tt>.
     * </ul>
     *
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    public synchronized void gridletCancel(int gridletId, int userId)
    {
        // cancels a Gridlet
        ResGridlet rgl = cancel(gridletId, userId);

        // if the Gridlet is not found
        if (rgl == null)
        {
            System.out.println(super.resName_ +
                    ".SpaceShared.gridletCancel(): Cannot find " +
                    "Gridlet #" + gridletId + " for User #" + userId);

            super.sendCancelGridlet(GridSimTags.GRIDLET_CANCEL, null,
                                    gridletId, userId);
            return;
        }

        // if the Gridlet has finished beforehand then prints an error msg
        if (rgl.getGridletStatus() == Gridlet.SUCCESS)
        {
            System.out.println(super.resName_
                    + ".SpaceShared.gridletCancel(): Cannot cancel"
                    + " Gridlet #" + gridletId + " for User #" + userId
                    + " since it has FINISHED.");
        }

        // sends the Gridlet back to sender
        rgl.finalizeGridlet();
        super.sendCancelGridlet(GridSimTags.GRIDLET_CANCEL, rgl.getGridlet(),
                                gridletId, userId);
    }

    /**
     * Pauses a Gridlet only if it is currently executing.
     * This method will search in the execution list. The User ID is
     * important as many users might have the same Gridlet ID in the lists.
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @param   ack   an acknowledgement, i.e. <tt>true</tt> if wanted to know
     *        whether this operation is success or not, <tt>false</tt>
     *        otherwise (don't care)
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    public synchronized void gridletPause(int gridletId, int userId, boolean ack)
    {
        boolean status = false;

        // Find in EXEC List first
        int found = gridletInExecList_.indexOf(gridletId, userId);
        if (found >= 0)
        {
            // updates all the Gridlets first before pausing
            updateGridletProcessing();

            // Removes the Gridlet from the execution list
            ResGridlet rgl = (ResGridlet) gridletInExecList_.remove(found);

            // if a Gridlet is finished upon cancelling, then set it to success
            // instead.
            if (rgl.getRemainingGridletLength() == 0.0)
            {
                found = -1;  // meaning not found in Queue List
                gridletFinish(rgl, Gridlet.SUCCESS);
                System.out.println(super.resName_
                        + ".SpaceShared.gridletPause(): Cannot pause"
                        + " Gridlet #" + gridletId + " for User #" + userId
                        + " since it has FINISHED.");
            }
            else
            {
                status = true;
                rgl.setGridletStatus(Gridlet.PAUSED);  // change the status
                gridletPausedList_.add(rgl);   // add into the paused list

                // Set the PE on which Gridlet finished to FREE
                super.resource_.setStatusPE( PE.FREE, rgl.getMachineID(),
                                             rgl.getPEID() );

                // empty slot is available, hence process a new Gridlet
                allocateQueueGridlet();
            }
        }
        else {      // Find in QUEUE list
            found = gridletQueueList_.indexOf(gridletId, userId);
        }

        // if found in the Queue List
        if (status == false && found >= 0)
        {
            status = true;

            // removes the Gridlet from the Queue list
            ResGridlet rgl = (ResGridlet) gridletQueueList_.remove(found);
            rgl.setGridletStatus(Gridlet.PAUSED);   // change the status
            gridletPausedList_.add(rgl);            // add into the paused list
        }
        // if not found anywhere in both exec and paused lists
        else if (found == -1)
        {
            System.out.println(super.resName_ +
                    ".SpaceShared.gridletPause(): Error - cannot " +
                    "find Gridlet #" + gridletId + " for User #" + userId);
        }

        // sends back an ack if required
        if (ack)
        {
            super.sendAck(GridSimTags.GRIDLET_PAUSE_ACK, status,
                          gridletId, userId);
        }
    }

    /**
     * Moves a Gridlet from this GridResource entity to a different one.
     * This method will search in both the execution and paused list.
     * The User ID is important as many Users might have the same Gridlet ID
     * in the lists.
     * <p>
     * If a Gridlet has finished beforehand, then this method will send back
     * the Gridlet to sender, i.e. the <tt>userId</tt> and sets the
     * acknowledgment to false (if required).
     *
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @param destId       a new destination GridResource ID for this Gridlet
     * @param   ack   an acknowledgement, i.e. <tt>true</tt> if wanted to know
     *        whether this operation is success or not, <tt>false</tt>
     *        otherwise (don't care)
     * @pre gridletId > 0
     * @pre userId > 0
     * @pre destId > 0
     * @post $none
     */
    public synchronized void gridletMove(int gridletId, int userId, int destId, boolean ack)
    {
        // cancels the Gridlet
        ResGridlet rgl = cancel(gridletId, userId);

        // if the Gridlet is not found
        if (rgl == null)
        {
            System.out.println(super.resName_ +
                       ".SpaceShared.gridletMove(): Cannot find " +
                       "Gridlet #" + gridletId + " for User #" + userId);

            if (ack)   // sends back an ack if required
            {
                super.sendAck(GridSimTags.GRIDLET_SUBMIT_ACK, false,
                              gridletId, userId);
            }

            return;
        }

        // if the Gridlet has finished beforehand
        if (rgl.getGridletStatus() == Gridlet.SUCCESS)
        {
            System.out.println(super.resName_
                    + ".SpaceShared.gridletMove(): Cannot move Gridlet #"
                    + gridletId + " for User #" + userId
                    + " since it has FINISHED.");

            if (ack) // sends back an ack if required
            {
                super.sendAck(GridSimTags.GRIDLET_SUBMIT_ACK, false,
                              gridletId, userId);
            }

            gridletFinish(rgl, Gridlet.SUCCESS);
        }
        else   // otherwise moves this Gridlet to a different GridResource
        {
            rgl.finalizeGridlet();

            // Set PE on which Gridlet finished to FREE
            super.resource_.setStatusPE( PE.FREE, rgl.getMachineID(),
                                         rgl.getPEID() );

            super.gridletMigrate(rgl.getGridlet(), destId, ack);
            allocateQueueGridlet();
        }
    }

    /**
     * Resumes a Gridlet only in the paused list.
     * The User ID is important as many Users might have the same Gridlet ID
     * in the lists.
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @param   ack   an acknowledgement, i.e. <tt>true</tt> if wanted to know
     *        whether this operation is success or not, <tt>false</tt>
     *        otherwise (don't care)
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    public synchronized void gridletResume(int gridletId, int userId, boolean ack)
    {
        boolean status = false;

        // finds the Gridlet in the execution list first
        int found = gridletPausedList_.indexOf(gridletId, userId);
        if (found >= 0)
        {
            // removes the Gridlet
            ResGridlet rgl = (ResGridlet) gridletPausedList_.remove(found);
            rgl.setGridletStatus(Gridlet.RESUMED);

            // update the Gridlets up to this point in time
            updateGridletProcessing();
            status = true;

            // if there is an available PE slot, then allocate immediately
            boolean success = false;
            if ( gridletInExecList_.size() < super.totalPE_ ) {
                success = allocatePEtoGridlet(rgl);
            }

            // otherwise put into Queue list
            if (!success)
            {
                rgl.setGridletStatus(Gridlet.QUEUED);
                gridletQueueList_.add(rgl);
            }

            System.out.println(super.resName_ + "TimeShared.gridletResume():" +
                    " Gridlet #" + gridletId + " with User ID #" +
                    userId + " has been sucessfully RESUMED.");
        }
        else
        {
            System.out.println(super.resName_ +
                    "TimeShared.gridletResume(): Cannot find " +
                    "Gridlet #" + gridletId + " for User #" + userId);
        }

        // sends back an ack if required
        if (ack)
        {
            super.sendAck(GridSimTags.GRIDLET_RESUME_ACK, status,
                          gridletId, userId);
        }
    }

    ///////////////////////////// PRIVATE METHODS /////////////////////

    /**
     * Allocates the first Gridlet in the Queue list (if any) to execution list
     * @pre $none
     * @post $none
     */
    private void allocateQueueGridlet()
    {
        // if there are many Gridlets in the QUEUE, then allocate a
        // PE to the first Gridlet in the list since it follows FCFS
        // (First Come First Serve) approach. Then removes the Gridlet from
        // the Queue list
        if (gridletQueueList_.size() > 0 &&
            gridletInExecList_.size() < super.totalPE_)
        {
            ResGridlet obj = (ResGridlet) gridletQueueList_.get(0);

            // allocate the Gridlet into an empty PE slot and remove it from
            // the queue list
            boolean success = allocatePEtoGridlet(obj);
            if (success) {
                gridletQueueList_.remove(obj);
            }
        }
    }

    /**
     * Updates the execution of all Gridlets for a period of time.
     * The time period is determined from the last update time up to the
     * current time. Once this operation is successfull, then the last update
     * time refers to the current time.
     * @pre $none
     * @post $none
     */
    private synchronized void updateGridletProcessing()
    {
        // Identify MI share for the duration (from last event time)
        double time = GridSim.clock();
        double timeSpan = time - lastUpdateTime_;

        // if current time is the same or less than the last update time,
        // then ignore
        if (timeSpan <= 0.0) {
            return;
        }

        // Update Current Time as Last Update
        lastUpdateTime_ = time;

        // update the GridResource load
        int size = gridletInExecList_.size();
        double load = super.calculateTotalLoad(size);
        super.addTotalLoad(load);

        // if no Gridlets in execution then ignore the rest
        if (size == 0) {
            return;
        }

        ResGridlet obj = null;

        // a loop that allocates MI share for each Gridlet accordingly
        Iterator iter = gridletInExecList_.iterator();
        while ( iter.hasNext() )
        {
            obj = (ResGridlet) iter.next();

            // Updates the Gridlet length that is currently being executed
            load = getMIShare( timeSpan, obj.getMachineID() );
            obj.updateGridletFinishedSoFar(load);
        }
    }

    /**
     * Identifies MI share (max and min) each Gridlet gets for
     * a given timeSpan
     * @param timeSpan     duration
     * @param machineId    machine ID that executes this Gridlet
     * @return  the total MI share that a Gridlet gets for a given
     *          <tt>timeSpan</tt>
     * @pre timeSpan >= 0.0
     * @pre machineId > 0
     * @post $result >= 0.0
     */
    private double getMIShare(double timeSpan, int machineId)
    {
        // 1 - localLoad_ = available MI share percentage
        double localLoad = super.resCalendar_.getCurrentLoad();

        // each Machine might have different PE Rating compare to another
        // so much look at which Machine this PE belongs to
        double totalMI = machineRating_[machineId] * timeSpan * (1 - localLoad);
        return totalMI;
    }

    /**
     * Allocates a Gridlet into a free PE and sets the Gridlet status into
     * INEXEC and PE status into busy afterwards
     * @param rgl  a ResGridlet object
     * @return <tt>true</tt> if there is an empty PE to process this Gridlet,
     *         <tt>false</tt> otherwise
     * @pre rgl != null
     * @post $none
     */
    private boolean allocatePEtoGridlet(ResGridlet rgl)
    {
        // IDENTIFY MACHINE which has a free PE and add this Gridlet to it.
        Machine myMachine = resource_.getMachineWithFreePE();

        // If a Machine is empty then ignore the rest
        if (myMachine == null) {
            return false;
        }

        // gets the list of PEs and find one empty PE
        PEList MyPEList = myMachine.getPEList();
        int freePE = MyPEList.getFreePEID();

        // ALLOCATE IMMEDIATELY
        rgl.setGridletStatus(Gridlet.INEXEC);   // change Gridlet status
        rgl.setMachineAndPEID(myMachine.getMachineID(), freePE);
        

        // add this Gridlet into execution list
        gridletInExecList_.add(rgl);

        // Set allocated PE to BUSY status
        super.resource_.setStatusPE(PE.BUSY, rgl.getMachineID(), freePE);

        // Identify Completion Time and Set Interrupt
        int rating = machineRating_[ rgl.getMachineID() ];
        //DEBUG
        //System.out.println("allocatePEtoGridlet: rgl.getRemainingGridletLength(): " + rgl.getRemainingGridletLength());
        double time = forecastFinishTime( rating ,
                                          rgl.getRemainingGridletLength() );

        int roundUpTime = (int) (time+1);   // rounding up
        rgl.setFinishTime(roundUpTime);

        // then send this into itself
        super.sendInternalEvent(roundUpTime);
        return true;
    }

    /**
     * Forecast finish time of a Gridlet.
     * <tt>Finish time = length / available rating</tt>
     * @param availableRating   the shared MIPS rating for all Gridlets
     * @param length   remaining Gridlet length
     * @return Gridlet's finish time.
     * @pre availableRating >= 0.0
     * @pre length >= 0.0
     * @post $none
     */
    private static double forecastFinishTime(double availableRating, double length)
    {
        double finishTime = (length / availableRating);

        // This is as a safeguard since the finish time can be extremely
        // small close to 0.0, such as 4.5474735088646414E-14. Hence causing
        // some Gridlets never to be finished and consequently hang the program
        if (finishTime < 1.0) {
            finishTime = 1.0;
        }
        //DEBUG
        //System.out.println("availableRating: " + availableRating + " length: " + length + " forecastFinishTime " + finishTime);
        return finishTime;
    }

    /**
     * Checks all Gridlets in the execution list whether they are finished or
     * not.
     * @pre $none
     * @post $none
     */
    private synchronized void checkGridletCompletion()
    {
        ResGridlet obj = null;
        int i = 0;

        // NOTE: This one should stay as it is since gridletFinish()
        // will modify the content of this list if a Gridlet has finished.
        // Can't use iterator since it will cause an exception
        while ( i < gridletInExecList_.size() )
        {
            obj = (ResGridlet) gridletInExecList_.get(i);

            if (obj.getRemainingGridletLength() == 0.0)
            {
                gridletInExecList_.remove(obj);
                gridletFinish(obj, Gridlet.SUCCESS);
                continue;
            }

            i++;
        }

        // if there are still Gridlets left in the execution
        // then send this into itself for an hourly interrupt
        // NOTE: Setting the internal event time too low will make the
        //       simulation more realistic, BUT will take longer time to
        //       run this simulation. Also, size of sim_trace will be HUGE!
        /*if (gridletInExecList_.size() > 0) {
            super.sendInternalEvent(60.0*60.0);
        }*/ //performance upgrade
    }

    /**
     * The initial method was changed for Data production simulations.
     * Now when the gridlet is finished, it is not send back to user,
     *  but processed by processFinishedJob(gl)
     * 
     * Updates the Gridlet's properties, such as status once a
     * Gridlet is considered finished.
     * @param rgl   a ResGridlet object
     * @param status   the Gridlet status
     * @pre rgl != null
     * @pre status >= 0
     * @post $none
     */
    private void gridletFinish(ResGridlet rgl, int status)
    {
        // Set PE on which Gridlet finished to FREE
        super.resource_.setStatusPE(PE.FREE, rgl.getMachineID(), rgl.getPEID());

        // the order is important! Set the status first then finalize
        // due to timing issues in ResGridlet class
        rgl.setGridletStatus(status);
        rgl.finalizeGridlet();
        
        //DP METHOD!!!!!!
        processFinishedJob(  (DPGridlet) rgl.getGridlet() );
        //super.sendFinishGridlet( rgl.getGridlet() );

        allocateQueueGridlet();   // move Queued Gridlet into exec list
    }

    /**
     * Handles an operation of canceling a Gridlet in either execution list
     * or paused list.
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @return an ResGridlet object <tt>null</tt> if this Gridlet is not found
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    private ResGridlet cancel(int gridletId, int userId)
    {
        ResGridlet rgl = null;

        // Find in EXEC List first
        int found = gridletInExecList_.indexOf(gridletId, userId);
        if (found >= 0)
        {
            // update the gridlets in execution list up to this point in time
            updateGridletProcessing();

            // Get the Gridlet from the execution list
            rgl = (ResGridlet) gridletInExecList_.remove(found);

            // if a Gridlet is finished upon cancelling, then set it to success
            // instead.
            if (rgl.getRemainingGridletLength() == 0.0) {
                rgl.setGridletStatus(Gridlet.SUCCESS);
            }
            else {
                rgl.setGridletStatus(Gridlet.CANCELED);
            }

            // Set PE on which Gridlet finished to FREE
            super.resource_.setStatusPE( PE.FREE, rgl.getMachineID(),
                                        rgl.getPEID() );
            allocateQueueGridlet();
            return rgl;
        }

        // Find in QUEUE list
        found = gridletQueueList_.indexOf(gridletId, userId);
        if (found >= 0)
        {
            rgl = (ResGridlet) gridletQueueList_.remove(found);
            rgl.setGridletStatus(Gridlet.CANCELED);
        }

        // if not, then find in the Paused list
        else
        {
            found = gridletPausedList_.indexOf(gridletId, userId);

            // if found in Paused list
            if (found >= 0)
            {
                rgl = (ResGridlet) gridletPausedList_.remove(found);
                rgl.setGridletStatus(Gridlet.CANCELED);
            }

        }
        return rgl;
    }
} 

