package no_network;

import flow_model.*;
import gridsim.GridResource;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.ResourceCharacteristics;
import gridsim.net.FIFOScheduler;
import gridsim.net.RIPRouter;  // To use the new flow network package - GridSim 4.2
import gridsim.util.SimReport;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedList;


/**
 * This is the main class of the simulation package. It reads all the parameters
 * from a file, constructs the simulation defined in the configuration files,
 * and runs the simulation.
 * @author Uros Cibej and Anthony Sulistio
 */
public class NoNetworkSimulation {
    public static void main(String[] args) {
	String path = "F:/git/Grid_simulation/grid/src/main/java/flow_model/";
	String gridletFileName = path + "input/KISTI_7000_filtered.csv";
	int gridletNumber = 7000;
	
        System.out.println("Starting PUSH simulation ....");

        try {
            if (args.length != 1) {
                System.out.println("Usage: java Main parameter_file");
                return;
            }
                       
            

            //reads parameters
            System.out.println( "Parameters file: " + args[0]);
            ParameterReader.read(args[0]);
            
            
            Logger.openFile(ParameterReader.simulationLogFilename);
            write( "Parameters file: " + args[0]);
            int num_user = 1; // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = true; // means trace GridSim events
           // boolean gisFlag = false; // means using custom gis instead
            
            //set data units for the simulation
            DataUnits.setUnits(ParameterReader.dataUnitsName, ParameterReader.dataUnitsSize);

           
            //uses flow extension
            GridSim.initNetworkType(GridSimTags.NET_PACKET_LEVEL);
            //Initializes the GridSim package
            System.out.println("Initializing GridSim package");            
            GridSim.init(num_user, calendar, trace_flag);

            

            // sets the GIS into DataGIS that handles specifically for data grid
            // scenarios
            //GridInformationService gis = new GridInformationService("GIS",Double.MAX_VALUE);
            //DataGIS gis = new DataGIS(); 
            //GridSim.setGIS(gis);
            
            ///////////
            //CREATEs RESOURCES
            LinkedList<GridResource> resList = NoNetworkResReader.read(ParameterReader.resourceFilename);  
            //adding routers
            LinkedList<RIPRouter> routerList = new LinkedList<RIPRouter>();
            RIPRouter router, plannerRouter = null;
            
            //DPSpaceShared policy = null;
            ResourceCharacteristics character = null;
            write("RESOURCES: ");
            for(GridResource res: resList){
            	//adding routers
            	router = new RIPRouter(res.get_name() + "_router", trace_flag);
            	router.attachHost(res, new FIFOScheduler(res.get_name()
                        + "_router_scheduler"));            	
            	routerList.add(router);
            	//setup TIER-0s
            	//policy = (DPSpaceShared) res.getAllocationPolicy();
            	character = res.getResourceCharacteristics();
            	if ( character.getNumFreePE() == 1 ){
            	  plannerRouter = router; //select the router where to attach a planer
            	  //collect all available gridlets here
            	    
            	}
            	
            	write(gridResourceToString(res));            	
            }

            //READ NETWORK
            System.out.println("ROUTERS:");
            for (RIPRouter r:routerList){
        	System.out.println(DPNetworkReader.routerToString(r));
            }
            
            DPNetworkReader.createNetwork(routerList, ParameterReader.networkFilename);
            DPNetworkReader.printLinks();
                 
            
            //CREATE USER RUNING PLANER
            NoNetworkUser pusher = new NoNetworkUser("Pusher",
                    Double.MAX_VALUE, 0.001, Integer.MAX_VALUE);   
            
            //CHOSE 2 OPTIONS
            //use this for normal filesize
            pusher.setGridletList(GridletReader.getGridletList(gridletFileName, gridletNumber));
            
            //use this for 0 filesize (disables network delays)
            //pusher.setGridletListZeroFileSize(GridletReader.getGridletList(gridletFileName, gridletNumber));
            plannerRouter.attachHost(pusher, new FIFOScheduler("Pusher"+"_router_scheduler"));     
           
            //check network type
            write( "Network type: " + GridSim.getNetworkType()  );
            //GridSim.enableDebugMode();
            
            //create network monitor
            //NetworkMonitor netMon= new NetworkMonitor("NetworkMonitor");
            //plannerRouter.attachHost(netMon, new FIFOScheduler("NetworkMonitor"+"_router_scheduler"));  

            GridSim.startGridSimulation();
            
            //write("ROUTERS:");
            //for (RIPRouter r : routerList){
        	//r.printRoutingTable();
            //}
            
            write("\nFinish data grid simulation ...");
            Logger.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unwanted errors happen");
        }
    }
    
    private static void write(String msg){
        System.out.println("Simulation: " + msg);
        Logger.write("Simulation: " + msg);
    }
    
    private static String gridResourceToString(GridResource gr){
	StringBuffer br = new StringBuffer();
	ResourceCharacteristics characteristics = gr.getResourceCharacteristics();
	
	
	br.append("name: " + gr.get_name() + ", ");
	br.append("id: " + gr.get_id() + ", ");
	br.append("PEs: " + characteristics.getMachineList().getNumPE() + ", ");
	//br.append("storage: " + ( (DPSpaceShared) gr.getAllocationPolicy() ).getStorageSize() + "(MB), ");
	
	br.append("MIPS: " +characteristics.getMIPSRatingOfOnePE()  + ", ");
	//br.append("processingRate: " +characteristics.  + ", ");
	
	br.append("link bandwidth: " + gr.getLink().getBaudRate()  + "(bit/s), ");
	//br.append("stat: " + gr.get_stat().toString());
	
	return br.toString();
	
    }
    

}

