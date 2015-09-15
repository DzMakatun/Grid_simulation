package flow_model;

/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * License:      GPL - http://www.gnu.org/copyleft/gpl.html
 */

import gridsim.*;
import gridsim.datagrid.DataGridResource;
import gridsim.datagrid.File;
import gridsim.datagrid.SimpleReplicaManager;
import gridsim.datagrid.index.TopRegionalRC;
import gridsim.datagrid.storage.HarddriveStorage;
import gridsim.datagrid.storage.Storage;
import gridsim.net.*;
import gridsim.net.flow.FlowLink;
import gridsim.util.NetworkReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.StringTokenizer;


/**
 * Creates one or more GridResources 
 */
public class ResourceReader {

    /**
     * Reads a description of resources from a file and creates the
     * GridResources.
     *
     * @param filename
     *            the name of the file where the resources are specified
     * @param routers
     *            the list of routers which have already been created
     * @return the list of resources which have been read from the file
     * @throws Exception
     */
    public static LinkedList read(String filename, LinkedList routers
       ) throws Exception {
        LinkedList resourceList = null;

        try {
            FileReader fRead = new FileReader(filename);
            BufferedReader b = new BufferedReader(fRead);
            resourceList = createResources(b, routers);
        } catch (IOException exp) {
            System.out.println("File not found");
        }

        return resourceList;
    }

    /**
     * Create a set of DataGridResources, described in a files.
     *
     * @param buf
     *            buffer with the description of resources
     * @param routerList
     *            the list of available routers
     * @param files
     *            the list of available files
     * @return a list of created DataGridResources
     * @throws Exception
     */
    private static LinkedList createResources(BufferedReader buf,
        LinkedList routerList) throws Exception {
        String line;
        String routerName;
        String resourceName;
        String regionalRC;
        int PEs;
        int MIPSrate;
        double storage_size;
        double bandwidth;
        boolean isInputSource;
        boolean isOutputDestination;

        GridResource r1;
        Router tempRouter;
        LinkedList resourceList = new LinkedList();
        StringTokenizer str;

        while ((line = buf.readLine()) != null) {
            str = new StringTokenizer(line);
            resourceName = str.nextToken();

            if (!(resourceName.startsWith("#"))) {
                PEs = Integer.parseInt(str.nextToken());
                MIPSrate = Integer.parseInt(str.nextToken());
                storage_size = Double.parseDouble(str.nextToken());
                bandwidth = Double.parseDouble(str.nextToken());
                routerName = str.nextToken(); // read the router name

               // System.out.println("useLocalRC: "+ ParameterReader.useLocalRC);
                if (ParameterReader.useLocalRC) {
                    regionalRC = null;
                } else {
                    regionalRC = str.nextToken();
                }
                
                isInputSource = Boolean.parseBoolean(str.nextToken() );
                isOutputDestination = Boolean.parseBoolean(str.nextToken());

                r1 = createStandardResource(resourceName, PEs, MIPSrate, storage_size,
                        bandwidth, regionalRC, isInputSource, isOutputDestination);

                // attach the resource to a router
                tempRouter = NetworkReader.getRouter(routerName, routerList);

                if (tempRouter != null) {
                     tempRouter.attachHost(r1, new FIFOScheduler(r1.get_name()
                     + "_scheduler"));

                } else {
                    System.out.println(
                        "ERROR - Resource reader- non existing router");
                }


                // add resource to the list
                resourceList.add(r1);
            }
        }

        return resourceList;
    }

  

    /**
     * Creates one Grid resource. A Grid resource contains one or more Machines.
     * Similarly, a Machine contains one or more PEs (Processing Elements or
     * CPUs).
     */
    private static GridResource createStandardResource(String name, int PEs, int processingMIPSRate,
        double storage_size, double bandwidth, String regionalRC,boolean isInputSource, boolean isOutputDestination) {
        System.out.println();

        // Here are the steps needed to create a Grid resource:
        // 1. We need to create an object of MachineList to store one or more
        // Machines
        MachineList mList = new MachineList();

        // System.out.println("Creates a Machine list");
        // 2. A Machine contains one or more PEs or CPUs. Therefore, should
        // create an object of PEList to store these PEs before creating
        // a Machine.

        // 4. Create one Machine with its id and list of PEs or CPUs
        mList.add(new Machine(0, PEs, processingMIPSRate)); // First Machine

        //5. not needed
       
        // 6. Create a ResourceCharacteristics object that stores the
        // properties of a Grid resource: architecture, OS, list of
        // Machines, allocation policy: time- or space-shared, time zone
        // and its price (G$/PE time unit).
        String arch = "x86"; // system architecture
        String os = "Linux"; // operating system
        double time_zone = 0.0; // time zone this resource located
        double cost = 0.0; // the cost of using this resource

        ResourceCharacteristics resConfig = new ResourceCharacteristics(arch,
                os, mList, ResourceCharacteristics.SPACE_SHARED, time_zone, cost); // 1 job per CPU !!!!!

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
            // create the replica manager
            //SimpleReplicaManager rm = new SimpleReplicaManager("RM_" + name,
            //        name);

            // create the resource calendar
            ResourceCalendar cal = new ResourceCalendar(time_zone, peakLoad,
                    offPeakLoad, holidayLoad, Weekends, Holidays, seed);

            // create a storage, which demands the storage size in MB
            //Storage storage = new HarddriveStorage("storage",
            //        storage_size);

            //DEFAULT NETWORK PARAMETERS for resources
            // create a grid resource, connected to a router. The bandwith is
            // defined as bit/s 
            
            // our logic is placed in handler
            DPSpaceShared handler = new DPSpaceShared(name, name + "_handler", storage_size, isInputSource, isOutputDestination);
            //link which connects the resource to it's router, the bandwith is
            // defined as bit/s 
            Link link = new FlowLink(name + "_link", bandwidth, 1, Integer.MAX_VALUE);
            gridRes = new GridResource(name, link, resConfig, cal, handler);
            
            
            //gridRes = new DPResource(name,
            //       new FlowLink(name + "_link", bandwidth, 1,
            //            Integer.MAX_VALUE), resConfig, storage_size, cal, rm);
            //gridRes.addStorage(storage);

            
            
            // create a local replica catalogue if needed
            // else set the regional RC for this resource
            //if (ParameterReader.useLocalRC) {
            //    gridRes.createLocalRC();
            //    gridRes.setHigherReplicaCatalogue(TopRegionalRC.DEFAULT_NAME);
            //} else {
            //    gridRes.setReplicaCatalogue(regionalRC);
            //}
        } catch (Exception e) {
            e.printStackTrace();
        }

        //System.out.println("Ceates Grid resource (name: " + gridRes.get_name() + ", id: " + gridRes.get_id() +", PEs: "  + mList.getNumPE() +
        //		", processing rate: " + processingMIPSRate + ", storage: " +gridRes.getTotalStorageCapacity()  + "(MB) , Link bandwidth: " + gridRes.getLink().getBaudRate()  + "(bit/s) )");

        return gridRes;
    }
}
