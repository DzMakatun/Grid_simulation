package PUSH_parallel_transfer;
import flow_model.DataUnits;
import gridsim.GridSim;
import gridsim.net.FIFOScheduler;
import gridsim.net.flow.FlowLink;
import gridsim.net.flow.FlowRouter;
import gridsim.util.NetworkReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.StringTokenizer;

import networkflows.planner.NetworkLink;


/**
 * Reads, creates and stores the network structure
 * for Data production simulations
 * @author Dima
 *
 */
public class PushParNetReader {
    public static final double DEFAULT_PROP_DELAY = 0.001;
    public static final int DEFAULT_MTU = Integer.MAX_VALUE;
    public static final String NETWORK_BANDWITH_UNITS_NAME = "GB";
    public static final int NETWORK_BANDWITH_UNITS_SIZE = 1000000000;  // 1 Gb in bits
    
    public static LinkedList <FlowLink> links = new LinkedList<FlowLink>(); 
    public static LinkedList<FlowRouter> routers = new LinkedList<FlowRouter>();
    public static LinkedList<NetworkLink> planerLinks = new LinkedList<NetworkLink>();
    
    
    
    public static void createNetwork(LinkedList<FlowRouter> routerList, String NetworkFilename){
	routers.addAll(routerList);
        try
        {
            FileReader fileReader = new FileReader(NetworkFilename);
            BufferedReader buffer = new BufferedReader(fileReader);
            
            String line;
            String name1, name2;
            StringTokenizer str = null;
            FlowRouter r1, r2;
            FlowLink tempLink = null;
            NetworkLink planerLink1, planerLink2;
            int node1Id, node2Id;
            
            // creating the linking between two routers
            while ((line = buffer.readLine()) != null)
            {
                str = new StringTokenizer(line);
                if (!str.hasMoreTokens()) {     // ignore newlines
                    continue;
                }


                // parse the name of the connected routers
                //WRNING: routers are named like NODE_router
                name1 = str.nextToken();    // node name
                if (name1.startsWith("#")) {    // ignore comments
                    continue;
                }

                name2 = str.nextToken();    // node name
                r1 = (FlowRouter) NetworkReader.getRouter(name1 + "_router", routers);
                r2 = (FlowRouter) NetworkReader.getRouter(name2 + "_router", routers);

                if (r1 == null || r2 == null)
                {
                    System.out.println("NetworkReader.createNetworkFlow(): " +
                    "Warning - unable to connect both "+name1+" and "+name2);
                    continue;
                }

                // get baud rate of the link
                double baud = NETWORK_BANDWITH_UNITS_SIZE * Double.parseDouble(str.nextToken()) 
                	/ DataUnits.getSize(); // bandwidth (bits/s) / units

                tempLink = new FlowLink(name1 + "_" + name2, baud,
                	DEFAULT_PROP_DELAY, DEFAULT_MTU);                
                links.add(tempLink);

                FIFOScheduler r1Sched = new FIFOScheduler(r1.get_name()
                + "_to_" + r2.get_name() );
                FIFOScheduler r2Sched = new FIFOScheduler(r2.get_name()
                + "_to_" + r1.get_name() );

                r1.attachRouter(r2, tempLink, r1Sched, r2Sched);
                
                //create and store links for planer
                node1Id = GridSim.getEntityId(name1);
                node2Id = GridSim.getEntityId(name2);
                
                //link 1->2
                //bandwith is calculated in data units
                planerLink1 = new NetworkLink(tempLink.get_id(), name1 + "->" + name2, node1Id, node2Id,
                	baud / 8, false);
                planerLinks.add(planerLink1);
                
                //link 2->1
                planerLink2 = new NetworkLink(- tempLink.get_id(), name2 + "->" + name1, node2Id, node1Id,
                	baud / 8, false);
                planerLinks.add(planerLink2);
                
            }
            
            
        buffer.close();
        fileReader.close();
        }
        catch (Exception exp)
        {
            System.out.println("DPNetworkReader: File not found: " + NetworkFilename);
            exp.printStackTrace();
        }
	
	return;
    }
    
    public static void printLinks(){
	System.out.println("LINKS:");
	for (FlowLink link: links){
	    System.out.println(linkToString(link));
	}
    }
    
    public static String linkToString(FlowLink link){
	StringBuffer buf = new StringBuffer();
	buf.append(link.get_id() + " ");
	buf.append(link.get_name() + " ");
	buf.append("baud-rate: " + link.getBaudRate() + " ");
	buf.append("delay: " + link.getDelay() + " ");
	buf.append("mtu: " + link.getMTU() + " ");	
	buf.append("[Bandwidth: " + link.getBaudRate() / 8 + " (" + DataUnits.getName() + " per second) ");
	buf.append("which is " + link.getBaudRate() * DataUnits.getSize() + " (bits/s) ]");
	return buf.toString();
    }
    
    public static String routerToString(FlowRouter router){
	StringBuffer buf = new StringBuffer();
	buf.append(" " + router.get_id() + " ");	
	buf.append(" " + router.get_name() + " ");	
	//buf.append(" stat: " + router.get_stat().get_data().toString() + " ");

	return buf.toString();
    }
    

}
