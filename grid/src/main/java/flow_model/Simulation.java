package flow_model;


import gridsim.GridResource;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.ResourceCharacteristics;
import gridsim.net.FIFOScheduler;
import gridsim.net.RIPRouter;  // To use the new flow network package - GridSim 4.2

import java.util.Calendar;
import java.util.LinkedList;

import org.joda.time.DateTime;

/**
 * This is the main class of the simulation package. It reads all the parameters
 * from a file, constructs the simulation defined in the configuration files,
 * and runs the simulation.
 * @author Uros Cibej and Anthony Sulistio
 */
public class Simulation {
    public static void main(String[] args) {
	long startTime = System.currentTimeMillis();
        System.out.println("Starting simulation ....");

        try {
            if (args.length != 4) {
                System.out.println("Usage: java Main parameter_file traceflag prefix background_traffic_level");
                return;
            }
             
            //set prefix for all log files
            DataUnits.setPrefix(args[2]); 
            //reads parameters
            System.out.println( "Parameters file: " + args[0]);
            ParameterReader.read(args[0]);
            
            Logger.openFile("output/" + DataUnits.getPrefix() + "_PLANNER_sim.log");
            write( "Parameters file: " + args[0]);
            double backgroundFlow = Double.parseDouble(args[3]);
            int num_user = 1; // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = Boolean.parseBoolean(args[1]); // means trace GridSim events
           // boolean gisFlag = false; // means using custom gis instead
            
            //set data units for the simulation
            DataUnits.setUnits(ParameterReader.dataUnitsName, ParameterReader.dataUnitsSize);

           
            //uses flow extension
            GridSim.initNetworkType(GridSimTags.NET_PACKET_LEVEL);
            //Initializes the GridSim package
            write("Initializing GridSim package");            
            GridSim.init(num_user, calendar, trace_flag);

            

            // sets the GIS into DataGIS that handles specifically for data grid
            // scenarios
            //GridInformationService gis = new GridInformationService("GIS",Double.MAX_VALUE);
            //DataGIS gis = new DataGIS(); 
            //GridSim.setGIS(gis);
            
            ///////////
            //CREATEs RESOURCES
            LinkedList<GridResource> resList = ResourceReader.read(ParameterReader.resourceFilename);  
            //adding routers
            LinkedList<RIPRouter> routerList = new LinkedList<RIPRouter>();
            RIPRouter router, plannerRouter = null;
            
            DPSpaceShared policy = null;
            write("RESOURCES: ");
            for(GridResource res: resList){
        	
            	//adding routers
            	router = new RIPRouter(res.get_name() + "_router", trace_flag);
            	router.attachHost(res, new FIFOScheduler(res.get_name()
                        + "_router_scheduler"));            	
            	routerList.add(router);
            	//setup TIER-0s
            	policy = (DPSpaceShared) res.getAllocationPolicy();
            	if ( policy.isInputSource() ){
            	  plannerRouter = router; //select the router where to attach a planer
            	  //collect all available gridlets here
            	    
            	}
            	
            	write( policy.paramentersToString());            	
            }

            //READ NETWORK
            write("ROUTERS:");
            for (RIPRouter r:routerList){
        	write(DPNetworkReader.routerToString(r));
            }
            
            DPNetworkReader.createNetwork(routerList, ParameterReader.networkFilename);
            write(DPNetworkReader.getLinksString() );
                 
            
            //CREATE USER RUNING PLANER
            User Planer = new User("Planer",
                    Double.MAX_VALUE, 0.001, Integer.MAX_VALUE);            
            plannerRouter.attachHost(Planer, new FIFOScheduler("Planer"+"_router_scheduler"));     
           
            //check network type
            write( "Network type: " + GridSim.getNetworkType()  );
            //GridSim.enableDebugMode();
            
            //create network monitor
            NetworkMonitor netMon= new NetworkMonitor("NetworkMonitor");
            plannerRouter.attachHost(netMon, new FIFOScheduler("NetworkMonitor"+"_router_scheduler"));  

            
            //setup background traffic
            if (backgroundFlow != 0){
        	BackgroundTraficSetter.setupBackgroundTrafic(backgroundFlow, resList);
            }else{
        	write("Background traffic DISABLED");
            }
            
            
            
            GridSim.startGridSimulation();
            
            //write("ROUTERS:");
            //for (RIPRouter r : routerList){
        	//r.printRoutingTable();
            //}
            
            if (backgroundFlow != 0){
        	write(BackgroundTraficSetter.getBackgroundSetupString());
            }else{
        	write("Background traffic was DISABLED");
            }
            
            write("Finish data grid simulation ...");
            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            write("runtime: " + elapsedTime/1000 + " (s)");
            Logger.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unwanted errors happen");
        }
    }
    
    private static void write(String msg){
	DateTime now = DateTime.now();
        System.out.println(now.toString() + " Simulation: " + msg);        
        Logger.write(now.toString() + " Simulation: " + msg);
    }
    
    private static String gridResourceToString(GridResource gr){
	StringBuffer br = new StringBuffer();
	ResourceCharacteristics characteristics = gr.getResourceCharacteristics();	
	br.append("name: " + gr.get_name() + ", ");
	br.append("id: " + gr.get_id() + ", ");
	br.append("PEs: " + characteristics.getMachineList().getNumPE() + ", ");
	br.append("storage: " + ( (DPSpaceShared) gr.getAllocationPolicy() ).getStorageSize() + "(MB), ");	
	br.append("MIPS: " +characteristics.getMIPSRatingOfOnePE()  + ", ");
	//br.append("processingRate: " +characteristics.  + ", ");	
	br.append("link bandwidth: " + gr.getLink().getBaudRate()  + "(bit/s), ");
	//br.append("stat: " + gr.get_stat().toString());	
	return br.toString();
	
    }
    

}

