package flow_model;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Logger {
    //writing statistics to a file
    private static PrintWriter report_ = null; 
    
    Logger(String filename){
    }
    
    public static void openFile(String filename){
	if (report_ != null) {return;}
	try {
	    report_= new PrintWriter(filename, "UTF-8");
	} catch (IOException e) {
	    System.out.print("Failed to create a log file");
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }
    
    public static void write(String msg){
	report_.println(msg);
    }
    
    public static void close(){
	report_.close();
    }

}
