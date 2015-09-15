package flow_model;


import eduni.simjava.Sim_system;
import gridsim.*;
import gridsim.datagrid.index.*;
import gridsim.net.FIFOScheduler;
import gridsim.net.Link;
import gridsim.net.Router;
import gridsim.net.SimpleLink;
import gridsim.util.NetworkReader;
import gridsim.util.SimReport;

import java.util.Calendar;
import java.util.LinkedList;

import gridsim.net.flow.*;  // To use the new flow network package - GridSim 4.2

/**
 * This is the main class of the simulation package. It reads all the parameters
 * from a file, constructs the simulation defined in the configuration files,
 * and runs the simulation.
 * @author Uros Cibej and Anthony Sulistio
 */
public class Simulation {

    private static SimReport report_;  // logs every events
	
    public static void main(String[] args) {
        System.out.println("Starting simulation ....");

        try {
            if (args.length != 1) {
                System.out.println("Usage: java Main parameter_file");
                return;
            }
            
            report_ = new SimReport("Simulation_report");

            //reads parameters
            write( "Parameters file: " + args[0]);
            ParameterReader.read(args[0]);

            int num_user = ParameterReader.numUsers; // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = true; // means trace GridSim events
            boolean gisFlag = false; // means using DataGIS instead
            
            //set data units for the simulation
            DataUnits.setUnits(ParameterReader.dataUnitsName, ParameterReader.dataUnitsSize);

           
            //Initializes the GridSim package
            System.out.println("Initializing GridSim package");
            GridSim.init(num_user, calendar, trace_flag, gisFlag);

            //uses flow extension
            GridSim.initNetworkType(GridSimTags.NET_FLOW_LEVEL);

            // sets the GIS into DataGIS that handles specifically for data grid
            // scenarios
            DataGIS gis = new DataGIS(); 
            GridSim.setGIS(gis);

            
            //SETUP NETWORK DEFAULTS
            double baud_rate = Double.MAX_VALUE;//10000000000.0; // bits/s (throughput of the
            					//links that we do not consider has to be as 
            					//large as possible, so that they are not a bottleneck)
            double propDelay = 1; // propagation delay in millisecond
            int mtu = Integer.MAX_VALUE; // max. transmission unit in bytes

            //-------------------------------------------
            //reads topology
            //uses flow extension
            LinkedList routerList = NetworkReader.createFlow(ParameterReader.networkFilename);
            
            
            //Regional RC
            Link l = new FlowLink("rc_link", baud_rate, propDelay, mtu);
            TopRegionalRC rc = new TopRegionalRC(l);

            //connect the TopRC to a router specified in the parameters file
            Router r1 = NetworkReader.getRouter(ParameterReader.topRCrouter,
                    routerList);
            FIFOScheduler gisSched = new FIFOScheduler();
            r1.attachHost(rc, gisSched); // attach RC

            //READ GRIDLETS
            GridletList gridletList = GridletReader.getGridletList(ParameterReader.gridletsFilename, 
        	    ParameterReader.maxGridlets);
            //print gridletlist
            DPGridlet gl = null;
            write("GRIDLETS: ");
            for(Gridlet g: gridletList){
            	gl = (DPGridlet) g;
            	write(gl.toStringShort());
            }
            
            ///////////
            //CREATEs RESOURCES
            LinkedList resList = ResourceReader.read(ParameterReader.resourceFilename,
                    routerList);
            GridResource res;
            
            //print resource list
            DPSpaceShared policy = null;
            write("RESOURCES: ");
            for(Object obj: resList){
            	res = (GridResource) obj;
            	policy = (DPSpaceShared) res.getAllocationPolicy();
            	if ( policy.isInputSource() ){
            	    policy.addInitialInputFiles(gridletList);
            	}
            	write( policy.paramentersToString());
            	
            }

            

            
            
            //CREATE USERS
            LinkedList users = UserReader.read(ParameterReader.usersFilename,
                    routerList, resList);
            
            write("USERS:");
            User usr;
            for(Object obj: users){
            	usr = (User) obj;
            	usr.setGridletList(gridletList);
            	write(usr.toStringShort());
            	
            }
           
            

            GridSim.startGridSimulation();
            
            write("\nFinish data grid simulation ...");
            report_.finalWrite();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unwanted errors happen");
        }
    }
    
    private static void write(String msg)
    {
        System.out.println(msg);
        if (report_ != null) {
            report_.write(msg);
        }
    }
    
    private static String gridResourceToString(GridResource gr){
	StringBuffer br = new StringBuffer();
	ResourceCharacteristics characteristics = gr.getResourceCharacteristics();
	
	
	br.append("name: " + gr.get_name() + ", ");
	br.append("id: " + gr.get_id() + ", ");
	br.append("PEs: " + characteristics.getMachineList().getNumPE() + ", ");
	br.append("storage: " + ( (DPSpaceShared) gr.getAllocationPolicy() ).getStorageSize() + "(MB), ");
	
	br.append("processingRate: " +characteristics.getMIPSRatingOfOnePE()  + ", ");
	//br.append("processingRate: " +characteristics.  + ", ");
	
	br.append("link bandwidth: " + gr.getLink().getBaudRate()  + "(bit/s), ");
	//br.append("stat: " + gr.get_stat().toString());
	
	return br.toString();
	
    }
    

}

