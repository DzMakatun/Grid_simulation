package flow_model;

/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * License:      GPL - http://www.gnu.org/copyleft/gpl.html
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;


/**
 * Reads the parameter file and pass each line to its respective reader.
 * @author Uros Cibej and Anthony Sulistio
 */
public class ParameterReader {

    public static String networkFilename;
    public static String resourceFilename;
    public static String usersFilename;
    public static String gridletsFilename;
    public static String planerLogFilename;
    public static int maxGridlets;
    public static int numUsers;
    public static String topRCrouter;
    public static boolean useLocalRC = true;
    public static String dataUnitsName = "MB"; //default value
    public static long dataUnitsSize = 1024*1024; //default value

    public static void read(String filename) {
        try {
            FileReader fRead = new FileReader(filename);
            BufferedReader buf = new BufferedReader(fRead);
            String line;
            String name;
            String value;
            StringTokenizer str;
             
            while ((line = buf.readLine()) != null) {
                if (!line.startsWith("#")) { //ignore comments
                    str = new StringTokenizer(line);

                    //parse the name and size of file
                    name = str.nextToken("=");
                    value = str.nextToken();

                    if (name.equals("network")) {
                        networkFilename = value;
                    } else if (name.equals("resources")) {
                        resourceFilename = value;
                    } else if (name.equals("users")) {
                        usersFilename = value;
                    } else if (name.equals("maxGridlets")) {
                    	maxGridlets = Integer.parseInt(value);
                    } else if (name.equals("numUsers")) {
                        numUsers = Integer.parseInt(value);
                    } else if (name.equals("gridlets")) {
                        gridletsFilename = value;
                    } else if (name.equals("topRCrouter")) {
                        topRCrouter = value;
                    } else if (name.equals("useLocalRC")) {
                    	//System.out.println(name + " " + value);
                        useLocalRC = Boolean.valueOf(value);
                    } else if (name.equals("dataUnitsName")) {
                	dataUnitsName = value;
                    } else if (name.equals("dataUnitsSize")) {
                	dataUnitsSize = Long.valueOf(value);
                    } else if (name.equals("planerLogFilename")) {
                	planerLogFilename = value;	
                	
                    } else {
                        System.out.println("Unknown parameter " + name);
                    }
                }

            }
        } catch (Exception exp) {
            System.out.println("File not found");
        }
    }
}
