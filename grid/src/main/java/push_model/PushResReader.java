package push_model;

/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * License:      GPL - http://www.gnu.org/copyleft/gpl.html
 */

import flow_model.GridletReader;
import flow_model.ParameterReader;
import gridsim.GridResource;
import gridsim.Gridlet;
import gridsim.GridletList;
import gridsim.Machine;
import gridsim.MachineList;
import gridsim.ResourceCalendar;
import gridsim.ResourceCharacteristics;
import gridsim.net.FIFOScheduler;
import gridsim.net.RIPRouter;
import gridsim.net.SimpleLink;





import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.StringTokenizer;

import networkflows.planner.CompNode;


/**
 * Creates one or more GridResources 
 */
public class PushResReader {
    public static LinkedList<CompNode> planerNodes = new LinkedList<CompNode>();

    /**
     * Reads a description of resources from a file and creates the
     * GridResources.
     *
     * @param filename
     *            the name of the file where the resources are specified
     * @return the list of resources which have been read from the file
     * @throws Exception
     */
    public static LinkedList<GridResource> read(String filename, LinkedList<RIPRouter> routerList) 
	    throws Exception {
        LinkedList<GridResource> resourceList = null;

        try {
            FileReader fRead = new FileReader(filename);
            BufferedReader b = new BufferedReader(fRead);
            resourceList = createResources(b, routerList);
        } catch (IOException exp) {
            System.out.println("File not found");
        }

        return resourceList;
    }

    /**
     * Create a set of DataGridResources using a BufferedReader.
     *
     * @param buf
     * @return a list of created DataGridResources
     * @throws Exception
     */
    private static LinkedList<GridResource> createResources(BufferedReader buf, LinkedList<RIPRouter> routerList) throws Exception {
        String line;
        String resourceName;
        int PEs;
        int MIPSrate;
        double storage_size;
        boolean isInputSource;
        boolean isOutputDestination;
        boolean isInputDestination;
        boolean isOutputSource;
        int maxGridlets; // max number of gridlets to read;
        String gridletFilename; //list of initial gridlets placed at the resource
        CompNode planerNode; //to store the data for planner
        
        

        GridResource r1;
        LinkedList<GridResource>  resourceList = new LinkedList();
        StringTokenizer str;
        //SpaceShared policy;
        
        RIPRouter router;
        PushUser pusher;

        while ((line = buf.readLine()) != null) {
            str = new StringTokenizer(line);
            resourceName = str.nextToken();

            if (!(resourceName.startsWith("#"))) {
                PEs = Integer.parseInt(str.nextToken());
                MIPSrate = Integer.parseInt(str.nextToken());
                storage_size = Double.parseDouble(str.nextToken());         
                isInputSource = Boolean.parseBoolean(str.nextToken() );
                isOutputDestination = Boolean.parseBoolean(str.nextToken());
                isInputDestination = Boolean.parseBoolean(str.nextToken() );
                isOutputSource = Boolean.parseBoolean(str.nextToken());

                r1 = createStandardResource(resourceName, PEs, MIPSrate, storage_size,
                        isInputSource, isOutputDestination, isInputDestination, isOutputSource);
                //create router
                router = new RIPRouter(r1.get_name() + "_router", false);
            	router.attachHost(r1, new FIFOScheduler(r1.get_name()
                        + "_router_scheduler"));            	
            	
                //if it is an input source, add initial gridlets to the storage
                if (isInputSource) {
                    maxGridlets = Integer.parseInt(str.nextToken());
                    gridletFilename = str.nextToken();
                    GridletList gridletList = GridletReader.getGridletList(gridletFilename ,maxGridlets);
                    
                    //Create scheduller
                    pusher = new PushUser("pusher-"+r1.get_name(),
                            Double.MAX_VALUE, 0.001, Integer.MAX_VALUE);   

                    pusher.setGridletList(gridletList);
                    pusher.setStorageId(r1.get_id());
                    
                    
                    router.attachHost(pusher, new FIFOScheduler(pusher.getName()+"_router_scheduler"));     
  
                    
                    //Print Gridlet list
                    System.out.println(gridletList.size() + " GRIDLETS at " + r1.get_id() + ":" + r1.get_name());


                } 
                
               // add resource to the list
                resourceList.add(r1);
                routerList.add(router);
                
                //WARNING CHECK THIS
               //create a CompNodeEntity for planner
                /*
                planerNode = new CompNode(r1.get_id(), r1.get_name(), 
                	false,  //is Dummy
                	isInputSource, isOutputDestination,
                	isInputDestination, isOutputSource,
                	(long) storage_size, PEs, 
                	(float) ParameterReader.alpha / MIPSrate, //processing rate depends on job parameters and MIPS
                	0, 0, 0, 0, 0
                	);
                
                planerNodes.add(planerNode); */
            } 
        }

        return resourceList;
    }

  

    /**
     * Creates one Grid resource for Data Production simulations
     * CPUs).
     */
    private static GridResource createStandardResource(String name, int PEs, int processingMIPSRate,
        double storage_size, boolean isInputSource, boolean isOutputDestination,
        boolean isInputDestination, boolean isOutputSource) {
        // 1. We need to create an object of MachineList to store one or more
        // Machines
        MachineList mList = new MachineList();

        // 4. Create one Machine with its id and list of PEs or CPUs
        mList.add(new Machine(0, PEs, processingMIPSRate)); // First Machine

       
        // 6. Create a ResourceCharacteristics object that stores the
        // properties of a Grid resource: architecture, OS, list of
        // Machines, allocation policy: time- or space-shared, time zone
        // and its price (G$/PE time unit).
        String arch = "x86"; // system architecture
        String os = "Linux"; // operating system
        double time_zone = 0.0; // time zone this resource located
        double cost = 0.0; // the cost of using this resource

        ResourceCharacteristics resConfig = new ResourceCharacteristics(arch,
                os, mList, ResourceCharacteristics.SPACE_SHARED, time_zone, cost);

        // System.out.println("Creates the properties of a Grid resource and " +
        // "stores the Machine list");
        // 7. Finally, we need to create a GridResource object.
        long seed = (11L * 13 * 17 * 19 * 23) + 1;
        double peakLoad = 0.0; // the resource load during peak hour
        double offPeakLoad = 0.0; // the resource load during off-peak hr
        double holidayLoad = 0.0; // the resource load during holiday

        // incorporates weekends so the grid resource is on 7 days a week
        LinkedList Weekends = new LinkedList();
        Weekends.add(new Integer(Calendar.SATURDAY));
        Weekends.add(new Integer(Calendar.SUNDAY));

        // incorporates holidays. However, no holidays are set in this example
        LinkedList Holidays = new LinkedList();
        GridResource gridRes = null;

        try {

            // create the resource calendar
            ResourceCalendar cal = new ResourceCalendar(time_zone, peakLoad,
                    offPeakLoad, holidayLoad, Weekends, Holidays, seed);

            // create a storage, which demands the storage size in MB
            //Storage storage = new HarddriveStorage("storage",
            //        storage_size);
           
            // our logic is placed in handler
            //DPSpaceShared handler = new DPSpaceShared(name, name + "_handler", storage_size,
        	//    isInputSource, isOutputDestination, isInputDestination, isOutputSource);
            //link which connects the resource to it's router, the bandwith is
            // defined as bit/s 
            FastSpaceShared policy = new FastSpaceShared(name, name + "_policy");
            double bandwidth = Double.MAX_VALUE;
            SimpleLink link = new SimpleLink(name + "_internal_link", bandwidth, 0.01, Integer.MAX_VALUE);
            
            gridRes = new GridResource(name, link, resConfig, cal, policy);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        return gridRes;
    }
}
