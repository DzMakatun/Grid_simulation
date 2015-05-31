package simulation.grid;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.Random;

import gridsim.*;

/**
 * Hello world!
 *
 */
public class App 
{
	 public static void main(String[] args)
	    {
	        System.out.println("Creating Grid resources");

	        try
	        {
	            // First step: Initialize the GridSim package. It should be called
	            // before creating any entities. We can't run GridResource
	            // entity without initializing GridSim first. We will get run-time
	            // exception error.

	            // number of users need to be created. In this example, we put
	            // zero since we don't create any user entities.
	            int num_user = 0;   
	            Calendar calendar = Calendar.getInstance();
	            boolean trace_flag = true; // mean trace GridSim events/activities

	            // list of files or processing names to be excluded from any
	            //statistical measures
	            String[] exclude_from_file = { "" };
	            String[] exclude_from_processing = { "" };

	            // the name of a report file to be written. We don't want to write
	            // anything here. See other examples of using the
	            // ReportWriter class
	            String report_name = "test_report";

	            // Initialize the GridSim package
	            System.out.println("Initializing GridSim package");
	            GridSim.init(num_user, calendar, trace_flag, exclude_from_file,
	                    exclude_from_processing, report_name);

	            // Since GridSim 3.0, there is another way to initialise GridSim
	            // without any statistical functionalities.
	            // The code is commented below:
	            // GridSim.init(num_user, calendar, trace_flag); 

	            // !!!!!!!! Initializing resources
		        int mipsRating = 377; //change this
	            GridResource gridResource = createGridResource("RCF", 16000, mipsRating);
	            
	            // !!!!!!!!Creates a list of Gridlets (JOBS)
	            GridletList list = createGridlet(10);
	            System.out.println("Creating " + list.size() + " Gridlets");
	            
	            
	            //!!!!!!!!!!!!!Create  Users
	            System.out.println("Creating users");
	            ResourceUserList userList = createGridUser(list);
	            System.out.println("Creating " + userList.size() + " Grid users");
	            
	            // print the Gridlets
	            printGridletList(list);
	            
	            System.out.println("Finish the example");
	            
	            

	            // NOTE: we do not need to call GridSim.startGridSimulation()
	            // as there are no user entities to send their jobs to this
	            // resource.
	        }
	        catch (Exception e)
	        {
	            e.printStackTrace();
	            System.out.println("Unwanted error happens");
	        }
	        
	        
	        
	    }


	    /**
	     * Creates one Grid resource. A Grid resource contains one or more
	     * Machines. Similarly, a Machine contains one or more PEs (Processing
	     * Elements or CPUs).
	     * <p>
	     * In this simple example, we are simulating one Grid resource with three
	     * Machines that contains one or more PEs.
	     * @return a GridResource object
	     */
	    private static GridResource createGridResource(String name, int Ncpu, int mipsRating)
	    {
	        System.out.println("Starting to create new Resource: " + name);

	        // Here are the steps needed to create a Grid resource:
	        // 1. We need to create an object of MachineList to store one or more
	        //    Machines
	        MachineList mList = new MachineList();
	        //System.out.println("Creates a Machine list");

	        // 2. Create one Machine with its id, number of PEs and MIPS rating per PE
	        //    In this example, we are using a resource from
	        //    hpc420.hpcc.jp, AIST, Tokyo, Japan
	        //    Note: these data are taken the from GridSim paper, page 25.
	        //          In this example, all PEs has the same MIPS (Millions
	        //          Instruction Per Second) Rating for a Machine.

	        mList.add( new Machine(0, Ncpu, mipsRating));   // First Machine
	        System.out.println("Creates the 1st Machine that has " + Integer.toString(Ncpu) +
	                " PEs and stores it into the Machine list");

	        // 4. Create a ResourceCharacteristics object that stores the
	        //    properties of a Grid resource: architecture, OS, list of
	        //    Machines, allocation policy: time- or space-shared, time zone
	        //    and its price (G$/PE time unit).
	        String arch = "x86";      // system architecture
	        String os = "Linus";          // operating system
	        double time_zone = 0.0;         // time zone this resource located
	        double cost = 0.0;              // the cost of using this resource

	        ResourceCharacteristics resConfig = new ResourceCharacteristics(
	                arch, os, mList, ResourceCharacteristics.SPACE_SHARED,	//jobs are not sharing the same cpu
	                time_zone, cost);

	        // 5. Finally, we need to create a GridResource object.
	        double baud_rate = 100.0;           // communication speed //!!!!!!!!!! edit this
	        long seed = 11L*13*17*19*23+1;
	        double peakLoad = 0.0;        // the resource load during peak hour
	        double offPeakLoad = 0.0;     // the resource load during off-peak hr
	        double holidayLoad = 0.0;     // the resource load during holiday

	        // incorporates weekends so the grid resource is on 7 days a week
	        LinkedList<Integer> Weekends = new LinkedList<Integer>();
	        Weekends.add(new Integer(Calendar.SATURDAY));
	        Weekends.add(new Integer(Calendar.SUNDAY));

	        // incorporates holidays. However, no holidays are set in this example
	        LinkedList<Integer> Holidays = new LinkedList<Integer>();

	        GridResource gridRes = null;
	        try
	        {
	            gridRes = new GridResource(name, baud_rate, seed,
	                resConfig, peakLoad, offPeakLoad, holidayLoad, Weekends,
	                Holidays);
	        }
	        catch (Exception e) {
	            e.printStackTrace();
	        }

	        System.out.println("Finally, creates one Grid resource and stores " +
	                "the properties of a Grid resource");

	        return gridRes;
	    }
	    private static GridletList createGridlet(int Njobs)
	    {
	        // Creates a container to store Gridlets
	        GridletList list = new GridletList();
	        
	        // We create three Gridlets or jobs/tasks manually without the help
	        // of GridSimRandom
	        int id = 0;
	        double length = 3500.0;
	        long file_size = 300;
	        long output_size = 300;
	        Gridlet gridlet1 = null;
	        for (int i = 0; i < Njobs; i++){
	        	gridlet1 = new Gridlet(id, length, file_size, output_size);
	        	list.add(gridlet1);
	        	id++;
	        }
	        		
	        
	        return list;
	    }

	    
	    /**
	     * Creates Grid users. In this example, we create 3 users. Then assign
	     * these users to Gridlets.
	     * @return a list of Grid users
	     */
	    private static ResourceUserList createGridUser(GridletList list)
	    {
	        ResourceUserList userList = new ResourceUserList();
	        
	        userList.add(0);    // user ID starts from 0

	        int userSize = userList.size();
	        int gridletSize = list.size();
	        int id = 0;//all jobs belong to the same user
	        
	        // assign user ID to particular Gridlets
	        for (int i = 0; i < gridletSize; i++)
	        {
	            ( (Gridlet) list.get(i) ).setUserID(id);
	        }
	        
	        return userList;
	    }

	    
	    private static void printGridletList(GridletList list)
	    {
	        int size = list.size();
	        Gridlet gridlet;
	        
	        String indent = "    ";
	        System.out.println();
	        System.out.println("Gridlet ID" + indent + "User ID" + indent +
	                "length" + indent + " file size" + indent +
	                "output size");
	        
	        for (int i = 0; i < size; i++)
	        {
	            gridlet = (Gridlet) list.get(i);
	            System.out.println(indent + gridlet.getGridletID() + indent + 
	                    indent + indent + gridlet.getUserID() + indent + indent +
	                    (int) gridlet.getGridletLength() + indent + indent +
	                    (int) gridlet.getGridletFileSize() + indent + indent +
	                    (int) gridlet.getGridletOutputSize() );
	        }
	    }
	    
}
