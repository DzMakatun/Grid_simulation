package flow_model;

import gridsim.net.*;
import gridsim.util.NetworkReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.StringTokenizer;

/**
 * Creates a list of users from a given file
 * @author Uros Cibej and Anthony Sulistio
 */
public class UserReader {

    /**
     * @param filename
     *            the name of the file containing the descriptions of Users
     * @param experiments
     *            the list of DataGridlets that can be used in the simulation
     * @return the list of Users
     * @throws Exception
     */
    public static LinkedList read(String filename, LinkedList routers,
        LinkedList resources) throws Exception {
        LinkedList userList = null;

        try {
            FileReader fRead = new FileReader(filename);
            BufferedReader b = new BufferedReader(fRead);
            userList = createUsers(b, routers);
        } catch (Exception exp) {
            System.out.println("User file not found: " + filename );
        }

        return userList;
    }

    /**
     *
     * @param buf
     * @param experiments
     *            the list of requests executed by the users
     * @return a list of Users initialized with the requests by the users.
     * @throws Exception
     */
    private static LinkedList createUsers(BufferedReader buf, LinkedList routers)
        throws Exception {
        String line;
        String name;
        String baudRate;
        String router_name;
        StringTokenizer str;
        LinkedList users = new LinkedList();

        while ((line = buf.readLine()) != null) {
            str = new StringTokenizer(line);
            name = str.nextToken();

            if (!name.startsWith("#")) {
                router_name = str.nextToken();

                String resource_name = str.nextToken(); //for the RC assignement
                baudRate = str.nextToken(); //baud rate is given in MB/s

                Router r = NetworkReader.getRouter(router_name, routers);

                if (r == null) {
                    System.out.println("Problem with ROUTER " + router_name);
                }

                System.out.println("1");
                User dUser = new User(name,
                        Double.parseDouble(baudRate), 1, Integer.MAX_VALUE);
                //dUser.setReplicaCatalogue(resource_name);
                
                System.out.println("2");
                
                r.attachHost(dUser, new FIFOScheduler(name+"_scheduler"));

                //dUser.setTasks(tasks);
                users.add(dUser);
            }
        }

        return users;
    }

}
