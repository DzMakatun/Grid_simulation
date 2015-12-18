package flow_model;
/**
 * this static class is dedicated to montain data units
 * consistency through the simulation
 * in keeps the name of data units and its size in bytes
 * @author Dima
 *
 */
public class DataUnits {
    /**
     * Short name for units (e.g MB, GB, TB, PB)
     */
    private static String name = "MB";
    /**COEFICIENT for bytes -> UNITS translation 
     */
    private static long size = 1024 * 1024;
    
    /**
     * prefix for all ouput files
     */
    private static String prefix = "TEST";
    
    /**
     * @return the prefix
     */
    public static String getPrefix() {
        return prefix;
    }

    /**
     * @param prefix the prefix to set
     */
    public static void setPrefix(String prefix) {
        DataUnits.prefix = prefix;
    }

    public static void setUnits(String newName, long newSize){
	DataUnits.name = newName;
	DataUnits.size = newSize;
	System.out.println("New data units set. name: " + name + " size: " + size);
    }
    
    public static String getName(){
	return name;
    }
    
    public static long getSize(){
	return size;
    }

}
