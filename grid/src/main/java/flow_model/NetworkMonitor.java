package flow_model;
import java.io.PrintWriter;

import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;
import gridsim.*;
import gridsim.GridSimTags;
import gridsim.net.SimpleLink;

public class NetworkMonitor extends GridSim{
    private int myId_;
    private String myName;
    private PrintWriter fileWriter; 
    private double updateInterval;
    private String indent = " ";
    
    
    NetworkMonitor(String name) throws Exception{
	super(name, new SimpleLink("NetMonLink",Double.MAX_VALUE, 0.001, Integer.MAX_VALUE) );
	this.myName = name;
	this.myId_ = super.getEntityId(name);
        String filename = "output/" + "network_usage.csv";
	fileWriter = new PrintWriter(filename, "UTF-8");
	this.updateInterval = ParameterReader.deltaT / 10;
    }
    
    public void body(){
	//REGISTER TO GIS
	
	
	
	 //write statistics for planned link bandwith consumprion	
	while (! FlowManager.initialized){
	    super.gridSimHold(100.0);
	}
	fileWriter.println("time" + indent + FlowManager.getNamesLine());
	FlowManager.resetCounters();
	super.sim_schedule(myId_, updateInterval, GridSimTags.INSIGNIFICANT);
	Sim_event ev = new Sim_event();
	while (FlowManager.initialized && Sim_system.running()){	    
	    super.sim_get_next(ev);
            // if the simulation finishes then exit the loop
            if (ev.get_tag() == GridSimTags.END_OF_SIMULATION){        	
                break;
            }
            
            if (ev.get_src() == myId_){
        	fileWriter.println(GridSim.clock() + indent + FlowManager.getConsumptionLine(updateInterval));
    	        FlowManager.resetCounters();
    	        //send event to itself
    	        super.sim_schedule(myId_, updateInterval, GridSimTags.INSIGNIFICANT);
            }
            
	}
	
        while (super.sim_waiting() > 0)
        {
            // wait for event and ignore since it is likely to be related to
            // internal event scheduled to update Gridlets processing
            super.sim_get_next(ev);
            System.out.println(myName +
                               ".NetworkMonitor.body(): ignore internal events");
        }
	
	terminateIOEntities(); 
	fileWriter.close();
	
    }

}
