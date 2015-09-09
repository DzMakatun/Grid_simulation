package flow_model;

/**
 * This class contains additional tags to GridSim for
 * simulation of data production grid using
 * network flow maximization model (RIFT)  
 *
 * @author  Dzmitry Makatun
 */
public class RiftTags {
    
  //BASE
  //to prevent a conflict with the existing GridSimTags values
  private static final int BASE = 100500; 
  
  //TAGS
  
  /** The planner  requests nodes on their status*/
  public static final int STATUS_REQUEST = BASE + 1;
  
  /** The node  sends its status to planer*/
  public static final int STATUS_RESPONSE = BASE + 2;
  
  /** The planner  sends a new plan to nodes*/
  public static final int NEW_PLAN = BASE + 3;
  
  /** Denotes transfer of input file*/
  public static final int INPUT = BASE + 4;
  
  /** Denotes transfer of output file*/
  public static final int OUTPUT = BASE + 5;
  
  /** Remote node confirms that the input file was received*/
  public static final int CONFIRMATION_INPUT = BASE + 6;
  
  /** Remote node confirms that the output file was received*/
  public static final int CONFIRMATION_OUTPUT = BASE + 7;
  
  /**COEFICIENT for bytes -> MB translation 
   * (or other units)
   */
  public static final int SIZE_UNITS = 1024 * 1024;
}
