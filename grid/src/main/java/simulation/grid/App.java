package simulation.grid;
import java.util.Calendar;
import java.util.LinkedList;

import gridsim.*;

/**
 * Hello world!
 *
 */
public class App 
{
	 public static void main(String[] args)
	    {
	        System.out.println("Starting example of how to create one Grid " +
	                "resource");

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
	            
	            System.out.println("Finish the 1st example");

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

	        //System.out.println();
	        //System.out.println("Creates the properties of a Grid resource and " +
	                "stores the Machine list");

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
}
