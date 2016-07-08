package pullModel;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import networkflows.planner.CompNode;
import eduni.simjava.Sim_event;
import flow_model.*;
import gridsim.GridSim;
import gridsim.GridSimCore;
import gridsim.GridSimTags;
import gridsim.Gridlet;
import gridsim.GridletList;
import gridsim.IO_data;
import gridsim.ResGridlet;
import gridsim.ResGridletList;
import gridsim.net.InfoPacket;
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
    private double nomberOfPendingOutputFiles;
    private int initialNomberOfFiles = 0;
    private double initialSizeOfFiles = 0;
    
    private GridletList waitingInputFiles = new GridletList(); //Gridlets That has just arrived
    private GridletList readyOutputFiles = new GridletList(); //output files ready for transfer
    
    List<Integer> inputSourcesIds, outputDestinationsIds;
    Map<Integer, Double> pingMap;
    private int userId;
    private double startTime, lastActivityTime;
    private boolean noWorkLeftSend;
    private double submittedInputSize;
    private double reservedOutputSize;
    private double pendingInputSize;
    private double pendingOutputSize;
    private int inputFilesSkipped;
    private int jobsFinished;

    public PullSpaceShared(String resourceName, String entityName, double storageSize, 
	boolean isInputSource, boolean isOutputDestination,
	boolean isInputDestination, boolean isOutputSource) throws Exception {
	super(resourceName, entityName);
	this.storageSize = storageSize;
	this.freeStorageSpace = this.storageSize;
	this.waitingInputSize = 0;
	this.readyOutputSize = 0;
        submittedInputSize = 0;
        reservedOutputSize = 0;
        pendingInputSize = 0;
        pendingOutputSize = 0;
	this.nomberOfPendingOutputFiles = 0;
	this.isInputSource = isInputSource;
	this.isOutputDestination = isOutputDestination;
	this.isInputDestination = isInputDestination;
	this.isOutputSource = isOutputSource;
	this.startTime = 0;
	this.lastActivityTime = 0;	
	inputFilesSkipped = 0;
	jobsFinished = 0;
	userId = -1;
	noWorkLeftSend = false;
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
	    buf.append("jobQueue" + indent);
	    buf.append("busyCPUs" + indent);
	    buf.append("jobSubmissionFailureFlag" + indent);
	    buf.append("incommingTransferFailureFlag" + indent);
	    buf.append("outgoingTransferFailureFlag" + indent);
	    
	    buf.append("submittedInputSize" + indent);
	    buf.append("reservedOutputSize" + indent);
	    buf.append("waitingInputSize" + indent);	    
	    buf.append("readyOutputSize" + indent);
	    buf.append("pendingInputSize" + indent);
	    buf.append("pendingOutputSize" + indent);	    
	    //buf.append( + indent);	    
	    return buf.toString();
    }
    
    /**
     * Checks the size of gridlet lists
     * @throws Exception 
     */
    private void verifyGridletLists(){
	if( this.waitingInputSize == getInputSize(this.waitingInputFiles)
		&& this.readyOutputSize == getOutputSize(this.readyOutputFiles)
	        && this.submittedInputSize == getInputSize(super.gridletInExecList_)
	        && this.reservedOutputSize == getOuputSize(super.gridletInExecList_)
		&& ( waitingInputSize + submittedInputSize + pendingInputSize + readyOutputSize + reservedOutputSize
		       + pendingOutputSize + freeStorageSpace == storageSize )
		){
	    		write("Gridlet lists verification passed ..............OK");
	}else{
	    while(true){
	      System.out.println(this.get_name() + "\n \n \n  \n \n \n  GRIDLET LIST VERIFICATION FAILED \n \n \n \n \n \n ");
	    }
	}	
    }
    
    private double getInputSize(ResGridletList resGridletList) {
	double sum = 0;
	ResGridlet gl;
	for (Object obj : resGridletList){
	    gl = (ResGridlet) obj;
	    sum += gl.getGridlet().getGridletFileSize();
	}
	return sum;
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
    
    private double getOuputSize(ResGridletList resGridletList) {
	double sum = 0;
	ResGridlet gl;
	for (Object obj : resGridletList){
	    gl = (ResGridlet) obj;
	    sum += gl.getGridlet().getGridletOutputSize();
	}
	return sum;
    }
    
    /**
     *  
     * @param list
     * @return sum of output files in gridletlist
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
	
    @Override
    public String getStatusString(){
	    String indent = " ";
	    StringBuffer buf = new StringBuffer();	    
	    buf.append(GridSim.clock() + indent);	    
	    buf.append( ( (double) this.gridletQueueList_.size()  ) / (double) this.resource_.getNumPE()   + indent); 
	    buf.append(    ( (double) (this.resource_.getNumBusyPE())
		    / (double) this.resource_.getNumPE() )   + indent);	
	    buf.append( 0 + indent);
	    buf.append( 0 + indent);
	    buf.append( 0 + indent);
	    
	    buf.append( (this.submittedInputSize  / this.storageSize) + indent);
	    buf.append( (this.reservedOutputSize  / this.storageSize) + indent);
	    buf.append( (this.waitingInputSize  / this.storageSize) + indent);
	    buf.append( (this.readyOutputSize  / this.storageSize) + indent);
	    buf.append( (this.pendingInputSize  / this.storageSize) + indent);	    
	    buf.append( (this.pendingOutputSize  / this.storageSize) + indent);	 
	    //buf.append( + indent);	    
	    return buf.toString();
    }
    
    public boolean addInitialInputFiles(GridletList list){
	DPGridlet gridlet;
	for (int i = 0; i < list.size(); i++){
	    gridlet = (DPGridlet) list.get(i);
	    //write(gridlet.getGridletID() + " job duration " + gridlet.getGridletLength() + " job finishedSoFAr " + gridlet.getGridletFinishedSoFar());
	    gridlet.setUserID(this.resId_);
	    //write(gridlet.getGridletID() + " job duration " + gridlet.getGridletLength() + " job finishedSoFAr " + gridlet.getGridletFinishedSoFar());
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
	
        private void sortInputQueue(){
		Collections.sort(this.waitingInputFiles, 
			new Comparator<Gridlet>() {
		            public int compare(Gridlet gl1, Gridlet gl2) {
		                return ( LFNmanager.getNumberOfReplicas( gl1.getGridletID() )
		        	    .compareTo( LFNmanager.getNumberOfReplicas( gl2.getGridletID() ) ));
		                }
	        });
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
	        case RiftTags.INIT: //ask for a next input file
	        	write("received INIT");
	        	initProduction(ev);
	                break; 
	      	
	        case RiftTags.START: //ask for a next input file
	        	write("received START");
	        	startProduction();
	                break; 
	                
	        case GridSimTags.INFOPKT_RETURN: //ask for a next input file
	        	//write("received ping response");
	        	processPingResponce(ev);
	                break; 
	                
	                
	      	    /**
	            case RiftTags.INPUT: //submit input file to resource
	        	//write("received input file");	        	
	                processIncommingInputFile(ev);
	                break;    
		    **/
	            case RiftTags.OUTPUT: //save output file to the storage, send ack
	        	//write("received output file");
	                processIncommingOutputFile(ev);
	                break;              
	                
	            case RiftTags.JOB_REQUEST: //send input files in responce
	        	//write("received JOB_REQUEST");
	                processJobRequest(ev);
	                break;              
	                
	            case RiftTags.JOB_REQUEST_DECLINE: //switch to the next source
	        	//write("received JOB_REQUEST_DECLINE");
	                processDecline(ev);
	                break;   
	                
	            case RiftTags.OUTPUT_TRANSFER_ACK: //ask for a next input file
	        	//write("received OUTPUT_TRANSFER_ACK");
	        	processOutputTransferAck(ev);
	                break;    
	                
	            case RiftTags.INPUT_TRANSFER_ACK: //ask for a next input file
	        	//write("received INPUT_TRANSFER_ACK");
	        	processInputTransferAck(ev);
	                break;    
	                
	            default:
	        	write("Unknown event received. tag: " + ev.get_tag());
	                break;
	        }        
	    }	



	private void initProduction(Sim_event ev) {
	        userId = (Integer) ev.get_data();
	        sortInputQueue();
		if (this.isInputDestination){
		    inputSourcesIds = new LinkedList<Integer>();
		    outputDestinationsIds = new LinkedList<Integer>();
		   
		    for(CompNode node : ResourceReader.planerNodes){
			if (node.isInputSource()){
			    inputSourcesIds.add(node.getId());
			}
			if (node.isOutputDestination()){
			    outputDestinationsIds.add(node.getId());
			}
		     }
		  pingMap = new HashMap<Integer,Double>();  
		  pingResources(inputSourcesIds);
		  pingResources(outputDestinationsIds);		  
		  //write("inputSourcesIds size: " + inputSourcesIds.size() );
		  //write("outputDestinationsIds size: " + outputDestinationsIds.size() );
		}
	}

	private void pingResources(List<Integer> resources) {
	    int size = 1; //packet size
	    for (int res: resources){
		if (!this.pingMap.containsKey(res)){ //if the resource was not pinged before
		    //send ping request
		    super.sim_schedule(this.outputPort_, GridSimTags.SCHEDULE_NOW, GridSimTags.INFOPKT_SUBMIT,
	              new IO_data(null, size, res) );
		    pingMap.put(res, 0.0); //put 0s not to ping resources twice 
		}
	    }	    
	}
	
	private void processPingResponce(Sim_event ev) {	    
	      if (ev.get_tag() == GridSimTags.INFOPKT_RETURN){
		  InfoPacket pkt = (InfoPacket) ev.get_data();		  
		  int destIndex = pkt.getDetailHops().length / 2;//pkt.getNumHop();
		  int destId = (Integer) pkt.getDetailHops()[destIndex];
		  pingMap.put(destId, pkt.getBaudRate());
		  write("Ping response from " + destId + " bandwidth :" + pkt.getBaudRate());
	      }	    
	}

	/**
	 * Initialize and start sending request to sources
	 */
	private void startProduction() {
		//write("Starting data production");	
	    this.startTime = GridSim.clock();
	    super.fileWriter.println(getStatusString() );   
	    if (this.isInputDestination){	
		sortResources(inputSourcesIds);
		sortResources(outputDestinationsIds);
		write(getNetworkInfoString());
		requestInputFiles(this.resource_.getNumFreePE()); //request input files from currently selected source
	    }
        }
	 
	private String getNetworkInfoString() {
	    StringBuffer buf = new StringBuffer();
	    buf.append("Discovered network setup");
	    buf.append("\n pingMap: ");
	    for (int key: pingMap.keySet()){
		buf.append(" ( " + key + " , " + pingMap.get(key) + " )");
	    }
	    buf.append("\n inputSources:");
	    for(int id: this.inputSourcesIds){
		buf.append(id + " ");
	    }
	    buf.append("\n outputDestinations:");
	    for(int id: this.outputDestinationsIds){
		buf.append(id + " ");
	    }	    
	    return buf.toString();
	}

	private void sortResources(List<Integer> idList) {
	    Collections.sort(idList, new Comparator<Integer>() {
	        public int compare(Integer res1,Integer res2) {
	            return -(pingMap.get(res1)).compareTo( pingMap.get(res2));
	        }
	    });    
	    
	}

	private void checkNoWorkLeft(){
	    if (noWorkLeftSend){// check if notification was send before
		return;
	    }
	    if( this.waitingInputSize == 0 //no input files left (important for sources)
		    &&( !this.isInputDestination //this is not a processing node
			    ||( //OR
				    this.inputSourcesIds.isEmpty() //no input sources left
				    &&this.resource_.getNumBusyPE() == 0 //no jobs running
				    && this.nomberOfPendingOutputFiles == 0 //all output files were transferred
			      )              
		      )
	       ){		
		write("sending NO_WORK_LEFT");
		//send with no network delay
		super.sim_schedule(userId, GridSimTags.SCHEDULE_NOW, RiftTags.NO_WORK_LEFT, this.resId_);
		noWorkLeftSend = true;
	    }	    
	}
	 
	 
	private void requestInputFiles(int n){
	    if (this.inputSourcesIds.isEmpty()){
		//write("No more sources available");
		checkNoWorkLeft();
		return;
	    }
	    JobRequest req = new JobRequest(this.resId_, n);
	    int source = this.inputSourcesIds.get(0);
	    //send with no network delay
	    super.sim_schedule(source, GridSimTags.SCHEDULE_NOW, RiftTags.JOB_REQUEST, req);
	    if (n >1){
		write("requesting " + n + " jobs from "+ source);
	    }
	}
	 


	private void processInputTransferAck(Sim_event ev) {
	    lastActivityTime = GridSim.clock();
	    long size = (Long) ev.get_data();
	    this.pendingInputSize -= size;
	    this.freeStorageSpace += size;	    
	}
	
	private void processOutputTransferAck(Sim_event ev) {
	    lastActivityTime = GridSim.clock();
	    nomberOfPendingOutputFiles--;
	    long size = (Long) ev.get_data();
	    this.pendingOutputSize -= size;
	    this.freeStorageSpace += size;
	    this.requestInputFiles(1);//ask for next jo	    
	}

	private void processDecline(Sim_event ev) {
		if (this.isInputDestination){
		    JobRequestDecline dec = (JobRequestDecline) ev.get_data();
		    if (!this.inputSourcesIds.isEmpty() &&dec.sourceId == this.inputSourcesIds.get(0)){//if current source declined - move to the next
			this.inputSourcesIds.remove(0);
		    }
		    if (this.inputSourcesIds.isEmpty() ){
			write("all sources depleted");
			checkNoWorkLeft();
		    }else{
			write("Switched to source " +  inputSourcesIds.get(0));
			requestInputFiles(dec.numberOfJobs);
		    }		    
		}else{
		    write("error: job_request_decline send to wrong destination");
		}
		//verifyGridletLists();
	}

        private void processJobRequest(Sim_event ev) {
	    if (this.isInputSource){
                JobRequest req = (JobRequest) ev.get_data();
		//write("node " + req.requesterId + " requested " + req.numberOfJobs + " jobs");
		int i = req.numberOfJobs;
		DPGridlet gl;
		long size;
		while (!this.waitingInputFiles.isEmpty() && i > 0){		    
		    gl = (DPGridlet) waitingInputFiles.poll();
		    size = gl.getGridletFileSize();
			  if ( !LFNmanager.checkAndChange( gl.getGridletID() )  ){//if file is send by some other source
			      //write("file " +gl.getGridletID() + " already send by other source");
			      //delete file
			      this.freeStorageSpace += size;
			      waitingInputSize -= size;
			      this.inputFilesSkipped +=1;
			      continue; //poll next file
			  }		    
		    gl.setSenderID(this.resId_);
		    IO_data data = new IO_data(gl, size, req.requesterId);
		    //DEBUG	   
		    //write("sending IO_data: " + data.toString());
		    //send with network delay
		    super.sim_schedule(super.outputPort_, GridSimTags.SCHEDULE_NOW, GridSimTags.GRIDLET_SUBMIT, data);
		    lastActivityTime = GridSim.clock();
		    //update counters		    
		    this.waitingInputSize -= size;
		    this.pendingInputSize +=size;
		    i--;
		    if (this.waitingInputFiles.size() % (this.initialNomberOfFiles / 20) == 0){
			write( (initialNomberOfFiles - waitingInputFiles.size()) + " / " + initialNomberOfFiles);
			//verifyGridletLists() ;
		    }
		}
		//if not enough input files send decline
		if (i > 0){
		    JobRequestDecline dec = new JobRequestDecline(this.resId_, i);
		    //send without network delay
		    super.sim_schedule(req.requesterId, GridSimTags.SCHEDULE_NOW, RiftTags.JOB_REQUEST_DECLINE, dec);
		    checkNoWorkLeft();
		}
	     super.fileWriter.println(getStatusString() );    
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
		//write("gridlet finished");
		//write(gl.getGridletID() + " job finished ");
		int dest = this.outputDestinationsIds.get(0); //define destination for output
		((DPGridlet) gl).setSenderID(this.resId_); //send the address where to send ack
		long size = gl.getGridletOutputSize();
	        IO_data obj = new IO_data(gl,size,dest);
	        super.sim_schedule(outputPort_, GridSimTags.SCHEDULE_NOW, RiftTags.OUTPUT, obj);
	        nomberOfPendingOutputFiles++;
	        this.pendingOutputSize += size;
	        this.reservedOutputSize -= size;
	        this.submittedInputSize -= gl.getGridletFileSize();
	        this.freeStorageSpace += gl.getGridletFileSize();
	        jobsFinished++;
	        super.fileWriter.println(getStatusString() );   
	        return true;
	    }
	
	    /** GRIDLETS MUST BE SUBMITTED THROUGH RESOURCES !!!!!
	private void processIncommingInputFile(Sim_event ev) {
	    if (this.isInputDestination){
		Gridlet gl = (Gridlet) ev.get_data();
		write(gl.getGridletID() + " job duration " + gl.getGridletLength() + " job finishedSoFAr " + gl.getGridletFinishedSoFar());
		super.gridletSubmit(gl, false);		
		//write("used CPUs: " + this.resource_.getNumBusyPE() + " of " + this.resource_.getNumPE());
	    }else{
		write("error: input file send to wrong destination");
	    }	    
	}
	**/

	@Override
	 public void gridletSubmit(Gridlet gl, boolean ack){
	    super.sim_schedule(gl.getUserID(), GridSimTags.SCHEDULE_NOW, RiftTags.INPUT_TRANSFER_ACK, gl.getGridletFileSize());
	    long sizeIn = gl.getGridletFileSize();
	    long sizeOut = gl.getGridletOutputSize();
	    this.reservedOutputSize += sizeOut;
	    this.submittedInputSize +=sizeIn;
	    this.freeStorageSpace -= (sizeIn + sizeOut);
	    super.gridletSubmit(gl, ack);	    
	}
	    
	


	/**
	 * store processed output file
	 * @param ev
	 */
	private void processIncommingOutputFile(Sim_event ev) {
	    if (this.isOutputDestination){
		Gridlet gl = (Gridlet) ev.get_data();
		addOutputFile(gl);
		//send ack with no network delay
		int dest  = ((DPGridlet) gl).getSenderID();
		super.sim_schedule(dest, GridSimTags.SCHEDULE_NOW, RiftTags.OUTPUT_TRANSFER_ACK, gl.getGridletOutputSize());
		super.fileWriter.println(getStatusString() ); 
		lastActivityTime = GridSim.clock();
	    }else{
		write("error: ouput file send to wrong destination");
	    }	    
	}
	
	/**
	 * To catch the end of simulation
	 */
	@Override
        public void setEndSimulation() {
	    	finish();
	        super.setEndSimulation();
	}
	
	private void finish() {
	    StringBuffer buf = new StringBuffer();
	    buf.append("Final report");
	    buf.append("\n*******************************");
	    buf.append("\n Makespan: " + (this.lastActivityTime - this.startTime));
	    buf.append("\n " + this.readyOutputFiles.size() + " output files received");
	    buf.append("\n Input files skipped: " + this.inputFilesSkipped);
	    buf.append("\n jobsFinished: " + jobsFinished);
	    buf.append("\n*******************************");
	    write(buf.toString());
	}

	private void write(String message){
	    String indent = " ";	    
            StringBuffer buf = new StringBuffer();
            buf.append(GridSim.clock() + " ");
            buf.append( this.resName_ + ":" );
            buf.append(this.resId_ + "(handler) ");	    
            buf.append(message);
	    if (displayMessages){
		System.out.println(buf.toString());
	    }
	    Logger.write(buf.toString());
	}

}
