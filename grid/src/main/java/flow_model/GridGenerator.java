package flow_model;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import networkflows.planner.CompNode;
import networkflows.planner.DataProductionPlanner;
import networkflows.planner.LinkAttributeProvider;
import networkflows.planner.NetworkLink;

import org.jgrapht.VertexFactory;
import org.jgrapht.generate.RandomGraphGenerator;
import org.jgrapht.generate.ScaleFreeGraphGenerator;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.graph.SimpleGraph;

import gridsim.Gridlet;

public class GridGenerator {
    
    public static void main(String[] args) throws IOException {
	String prefix = "F:/git/Grid_simulation/grid/src/main/java/flow_model/input/L3";
	String odtFilename = prefix + "_grid.dot";
	SimpleDirectedWeightedGraph<CompNode, NetworkLink> newGrid = CreateRandomAtlasGird(50);
	writeGrid(newGrid, prefix);
	DataProductionPlanner planner = new DataProductionPlanner("deleteMe.txt", 1, 0.8f);
	planner.WriteODT(LinkAttributeProvider.BANDWIDTH, newGrid, odtFilename);
	System.out.println("DONE");
	System.out.println("NODES: " + newGrid.vertexSet().size() + " EDGES: " + newGrid.edgeSet().size() + " CPUs: " + calculateCPUS(newGrid));
    }
    
    private static int calculateCPUS(
	    SimpleDirectedWeightedGraph<CompNode, NetworkLink> newGrid) {
	int nCPU = 0;
	for (CompNode node: newGrid.vertexSet()  ){
	    if (node.getCpuN() > 1){
		nCPU += node.getCpuN();
	    }
	}
	return nCPU;
    }

    private static  SimpleDirectedWeightedGraph<CompNode, NetworkLink> CreateRandomGird(int aNumOfVertexes, int aNumOfSources){
	//int aNumOfEdges = aNumOfVertexes * (aNumOfVertexes - 1) / 6;
	//RandomGraphGenerator<CompNode, DefaultEdge> gen = new RandomGraphGenerator<CompNode, DefaultEdge>( aNumOfVertexes, aNumOfEdges);
	SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid = new SimpleDirectedWeightedGraph<CompNode, NetworkLink>(NetworkLink.class);
	
	
	//Generate graph
	final SimpleGraph<Object,DefaultEdge> tmpGraph = new SimpleGraph<Object,DefaultEdge>(DefaultEdge.class);	
	ScaleFreeGraphGenerator<Object, DefaultEdge> gen = new ScaleFreeGraphGenerator<Object, DefaultEdge>(aNumOfVertexes);
	Map<String,Object> resultMap = new HashMap<String,Object>();
	//VertexFactory<CompNode> vertexFactory = new NodeFactory(aNumOfSources, 100);
	DummyFactory vertexFactory = new DummyFactory();
	gen.generateGraph(tmpGraph, vertexFactory, resultMap);	
	
	
	//sort vertexes by degree
	List<Object>  vertexes= new LinkedList<Object>();
	vertexes.addAll(tmpGraph.vertexSet());
		Collections.sort(vertexes, 
			new Comparator<Object>() {
		            public int compare(Object obj1, Object obj2) {
		                return -( (Integer) tmpGraph.degreeOf(obj1) )
		        	    .compareTo(tmpGraph.degreeOf(obj2) );
		                }
	        });
	
	//create nodes
        int id = 0;
        //Tier-1 CPUs
        int t1MinCPU = 200;
        int t1MaxCPU = 5000;
        //Tier-2 Cpus
        int t2MinCPU = 20;
        int t2MaxCPU = 200;
        
	float alpha = 1;
	int s = 0;
	int p = 0;
	
	int degree;
	long disk;
	int cpu;
	Map<Integer,CompNode> idMap = new HashMap<Integer,CompNode>(); 
	for (Object obj : vertexes){
	    id = (Integer) obj;
	    degree = tmpGraph.degreeOf(obj);
	    CompNode node;

	    if (s < aNumOfSources){//create T1 node
		cpu = (int) ( Math.random() * (t1MaxCPU - t1MinCPU) + t1MinCPU );
		node = new CompNode(id, "SP"+s++, false, true, false, true, true, 100000000, cpu, alpha, 0, 0, 0, 0, 0);
	    }else{//create T2 node
		cpu =  (int) ( Math.random() * (t2MaxCPU - t2MinCPU) + t2MinCPU );
		disk = cpu * 30000;
		node = new CompNode(id, "P"+p++, false, false, false, true, true, disk, cpu, alpha, 0, 0, 0, 0, 0);
	    }
	    grid.addVertex(node);
	    idMap.put(id, node);
	}
	
	//create edges
	CompNode source, target;
	Object i1,i2;
	String name;
	double baseBandwidth = 0.2;
	double minBandwidth = 0.05;
	double maxBandwidth = 1;
	
	double t1Bandwidth = 10;
	int edgeDegree;
	double bandwidth;
	for (DefaultEdge edge: tmpGraph.edgeSet()){
	    i1 = tmpGraph.getEdgeSource(edge);
	    i2 = tmpGraph.getEdgeTarget(edge);
	    source = idMap.get( (Integer) i1);
	    target = idMap.get( (Integer) i2);
	    name = source.getName() + "->" + target.getName();
	    /**
	    if (source.isInputSource() || target.isInputSource()){ //if one of the nodes is a source
		cpu = Math.max(source.getCpuN(), target.getCpuN());
	    }else{//none of nodes is source
		cpu = Math.min(source.getCpuN(), target.getCpuN());
	    }	    
	    bandwidth =  cpu * Math.min(tmpGraph.degreeOf(i1), tmpGraph.degreeOf(i2)) * baseBandwidth / 1000;
	    if (source.isInputSource() && target.isInputSource()){
		bandwidth = Ncpu * baseBandwidth / 1000;
	    } **/
	    
	    if (source.isInputSource() && target.isInputSource()){
	 		bandwidth = t1Bandwidth;
	    }else{
		cpu = Math.min(source.getCpuN(), target.getCpuN());
		edgeDegree = Math.min(tmpGraph.degreeOf(i1), tmpGraph.degreeOf(i2));
		bandwidth =  Math.random() * (maxBandwidth - minBandwidth) + minBandwidth;
		bandwidth = (double) Math.round(bandwidth * 100) / 100;
	    }
	    
	    
	    
	    NetworkLink link = new NetworkLink(id++,name, source.getId() , target.getId(), bandwidth , false);		    
            grid.addEdge(source, target, link);	    
	}	
	return grid;
    }
    
    private static  SimpleDirectedWeightedGraph<CompNode, NetworkLink> CreateRandomAtlasGird(int aNumOfVertexes){
	int aNumOfSources = 11;
	int[] cpus0 = {5406, 4052, 558, 4265, 1254, 4739, 576, 3416, 2362, 12447, 12000};
	int[] bandwidth0 = {10, 20, 20, 20, 10, 10, 10, 10, 20, 60, 2};
	int[] disk0 = {76, 56, 7, 60, 18, 67, 8, 48, 33, 174, 168};
   	//int aNumOfEdges = aNumOfVertexes * (aNumOfVertexes - 1) / 6;
   	//RandomGraphGenerator<CompNode, DefaultEdge> gen = new RandomGraphGenerator<CompNode, DefaultEdge>( aNumOfVertexes, aNumOfEdges);
   	SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid = new SimpleDirectedWeightedGraph<CompNode, NetworkLink>(NetworkLink.class);
   	
   	
   	//Generate graph
   	final SimpleGraph<Object,DefaultEdge> tmpGraph = new SimpleGraph<Object,DefaultEdge>(DefaultEdge.class);	
   	ScaleFreeGraphGenerator<Object, DefaultEdge> gen = new ScaleFreeGraphGenerator<Object, DefaultEdge>(aNumOfVertexes);
   	Map<String,Object> resultMap = new HashMap<String,Object>();
   	//VertexFactory<CompNode> vertexFactory = new NodeFactory(aNumOfSources, 100);
   	DummyFactory vertexFactory = new DummyFactory();
   	gen.generateGraph(tmpGraph, vertexFactory, resultMap);	
   	
   	
   	//sort vertexes by degree
   	List<Object>  vertexes= new LinkedList<Object>();
   	vertexes.addAll(tmpGraph.vertexSet());
   		Collections.sort(vertexes, 
   			new Comparator<Object>() {
   		            public int compare(Object obj1, Object obj2) {
   		                return -( (Integer) tmpGraph.degreeOf(obj1) )
   		        	    .compareTo(tmpGraph.degreeOf(obj2) );
   		                }
   	        });
   	
   	//create nodes   	
   	Map<Integer,CompNode> idMap = new HashMap<Integer,CompNode>(); 
   	   	
   	//add Tier-0
   	CompNode tier0 = new CompNode(100500, "A", false, true, true, false, false, 2100000000, 1, 1, 0, 0, 0, 0, 0);
   	grid.addVertex(tier0);
        idMap.put(100500, tier0);	
   		
        int id = 0;
        //Tier-1 CPUs
        int t1MinCPU = 200;
        int t1MaxCPU = 5000;
        //Tier-2 Cpus
        int t2MinCPU = 100;
        int t2MaxCPU = 500;
           
   	float alpha = 1;
   	int s = 0;
   	int p = 0;
   	
   	int degree;
   	long disk;
   	int cpu;
   	String name;
   	int cpusA = 0;
   	int cpusB = 0;
   	int cpusC = 0;
   	
   	for (Object obj : vertexes){
   	    id = (Integer) obj;
   	    degree = tmpGraph.degreeOf(obj);
   	    CompNode node;

   	    if (s < aNumOfSources){//create T1 node
   		name = "B"+s;
   		//cpu = (int) ( Math.random() * (t1MaxCPU - t1MinCPU) + t1MinCPU );
   		cpu = cpus0[s] / 2;
   		cpusB += cpu;
   		node = new CompNode(id, name , false, true, false, true, true, 100000000, cpu, alpha, 0, 0, 0, 0, 0);
   		//connect each Tier-1 node to Tier-0
   	   	NetworkLink link = new NetworkLink(100500 + id,"A->" + name, tier0.getId() , node.getId(), bandwidth0[s] / 2 , false);
   	   	grid.addVertex(node);
   	        grid.addEdge(tier0, node, link);	
   	        s++;   		
   	    }else{//create T2 node
   		name = "C"+p;
   		cpu =  (int) ( Math.random() * (t2MaxCPU - t2MinCPU) + t2MinCPU );
   		cpusC += cpu;
   		disk = cpu * 30000;
   		node = new CompNode(id, name, false, false, false, true, true, disk, cpu, alpha, 0, 0, 0, 0, 0);
   		grid.addVertex(node);
   		p++;
   	    }   	    
   	    idMap.put(id, node);
   	}
   	
   	//create edges
   	CompNode source, target;
   	Object i1,i2;
   	//String name;
   	//double baseBandwidth = 0.2;
   	double minBandwidth = 0.05;
   	double maxBandwidth = 0.5;
   	
   	double t1Bandwidth = 0.5;
   	int edgeDegree;
   	double bandwidth;
   	for (DefaultEdge edge: tmpGraph.edgeSet()){
   	    i1 = tmpGraph.getEdgeSource(edge);
   	    i2 = tmpGraph.getEdgeTarget(edge);
   	    source = idMap.get( (Integer) i1);
   	    target = idMap.get( (Integer) i2);
   	    name = source.getName() + "->" + target.getName();
   	    /**
   	    if (source.isInputSource() || target.isInputSource()){ //if one of the nodes is a source
   		cpu = Math.max(source.getCpuN(), target.getCpuN());
   	    }else{//none of nodes is source
   		cpu = Math.min(source.getCpuN(), target.getCpuN());
   	    }	    
   	    bandwidth =  cpu * Math.min(tmpGraph.degreeOf(i1), tmpGraph.degreeOf(i2)) * baseBandwidth / 1000;
   	    if (source.isInputSource() && target.isInputSource()){
   		bandwidth = Ncpu * baseBandwidth / 1000;
   	    } **/
   	    
   	    if (source.isInputSource() && target.isInputSource()){
   	 		bandwidth = t1Bandwidth;
   	    }else{
   		cpu = Math.min(source.getCpuN(), target.getCpuN());
   		edgeDegree = Math.min(tmpGraph.degreeOf(i1), tmpGraph.degreeOf(i2));
   		bandwidth =  Math.random() * (maxBandwidth - minBandwidth) + minBandwidth;
   		bandwidth = (double) Math.round(bandwidth * 100) / 100;
   	    }   	    
   	    
   	    NetworkLink link = new NetworkLink(id++,name, source.getId() , target.getId(), bandwidth , false);		    
               grid.addEdge(source, target, link);	    
   	}	
   	
   	System.out.println("Tier-1 cpus: " + cpusB + "  Tier-2 cpus: " + cpusC);
   	return grid;
       }
    
    private static  SimpleDirectedWeightedGraph<CompNode, NetworkLink> CreateCusromGird(int a, int b, int c){
	int Ncpu = 100;
	long disk = Ncpu * 20000;
	float alpha = 1;
	double bandwidth = 0.025;
	
	SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid = new SimpleDirectedWeightedGraph<CompNode, NetworkLink>(NetworkLink.class);
	List<List<CompNode>> nodes = new ArrayList<List<CompNode>>();
	int id = 0;
	
	//Level A
	List<CompNode> levelA = new ArrayList<CompNode>();
	for (int i=0; i < a; i++){
	    CompNode node = new CompNode(id++, "A"+i, false, false, false, true, true, disk, Ncpu, alpha, 0, 0, 0, 0, 0);
	    grid.addVertex(node);
	    levelA.add(node);
	}	
	nodes.add(levelA);	
	
	//Level B
	List<CompNode> levelB = new ArrayList<CompNode>();
	for (int i=0; i < b; i++){
	    CompNode node = new CompNode(id++, "B"+i, false, false, false, true, true, disk, Ncpu, alpha, 0, 0, 0, 0, 0);
	    grid.addVertex(node);
	    levelB.add(node);
	}	
	nodes.add(levelB);
	
	//Level C
	List<CompNode> levelC = new ArrayList<CompNode>();
	for (int i=0; i < c; i++){
	    CompNode node = new CompNode(id++, "C"+i, false, false, false, true, true, disk, Ncpu, alpha, 0, 0, 0, 0, 0);
	    grid.addVertex(node);
	    levelC.add(node);
	}	
	nodes.add(levelC);
	
	//Network
	List<CompNode> list, prev;
	CompNode ans, peer;
	int randomIndex, old; 
	String name;
	int l;
	for (int i = 1; i < nodes.size(); i++){
	    list = nodes.get(i);
	    prev = nodes.get(i-1);
	    for (CompNode node: list){
		//link to ancestors
		//1
		randomIndex =  (int) (Math.random() * prev.size()) ;
		ans = prev.get(randomIndex);
		name = ans.getName() + "->" + node.getName();
		NetworkLink link = new NetworkLink(id++,name, ans.getId() , node.getId(), bandwidth * node.getCpuN() / 100, false);		    
		grid.addEdge(ans, node, link);
		old = randomIndex;
		//2
		while(old == randomIndex){
		    randomIndex =  (int) (Math.random() * prev.size()) ;
		}
		ans = prev.get(randomIndex);
		name = ans.getName() + "->" + node.getName();
		NetworkLink link2 = new NetworkLink(id++,name, ans.getId() , node.getId(), bandwidth * node.getCpuN() / 100, false);		    
		grid.addEdge(ans, node, link2);
		
		//links to peers
		//1
		randomIndex =  (int) (Math.random() * list.size()) ;
		while(node.equals(list.get(randomIndex))){
		    randomIndex =  (int) (Math.random() * list.size()) ;
		}
		ans = list.get(randomIndex);
		name = ans.getName() + "->" + node.getName();
		NetworkLink link3 = new NetworkLink(id++,name, ans.getId() , node.getId(), bandwidth * node.getCpuN() / 100, false);		    
		grid.addEdge(ans, node, link3);
		old = randomIndex;
		//2
		while(old == randomIndex || node.equals(list.get(randomIndex))){
		    randomIndex =  (int) (Math.random() * list.size()) ;
		}
		ans = list.get(randomIndex);
		name = ans.getName() + "->" + node.getName();
		NetworkLink link4 = new NetworkLink(id++,name, ans.getId() , node.getId(), bandwidth * node.getCpuN() / 100, false);		    
		grid.addEdge(ans, node, link4);
	    }
	}	
	return grid;
    }
    
    
    private static SimpleDirectedWeightedGraph<CompNode, NetworkLink> CreateCompleteGrid(int nodesN){
	int Ncpu = 100;
	long disk = Ncpu * 20000;
	float alpha = 1;
	double bandwidth = 0.025;
	SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid = new SimpleDirectedWeightedGraph<CompNode, NetworkLink>(NetworkLink.class);	
	for (int i = 0; i < nodesN; i++){
	    CompNode node = new CompNode(i, "N"+i, false, false, false, true, true, disk, Ncpu, alpha, 0, 0, 0, 0, 0);
	    grid.addVertex(node);
	}
	int n = grid.vertexSet().size();
	int linkId = n;
	String name;
	//create fully connected network
	
	for (CompNode node1: grid.vertexSet()){
	    for  (CompNode node2: grid.vertexSet()){
		if (node1.getId() < node2.getId()){
		    name = node1.getName() + "->" + node2.getName();
		    NetworkLink link = new NetworkLink(linkId++,name, node1.getId() , node2.getId(), bandwidth, false);		    
		    grid.addEdge(node1, node2,link);
		}		
	    }
	}
	return grid;
    }
	
    private static SimpleDirectedWeightedGraph<CompNode, NetworkLink> CreateStarGrid(int nodesN){
	int Ncpu = 100;
	long disk = Ncpu * 20000;
	float alpha = 1;
	double bandwidth = 0.025;
	SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid = new SimpleDirectedWeightedGraph<CompNode, NetworkLink>(NetworkLink.class);	
	CompNode tier0 = new CompNode(0, "S0", false, true, true, false, false, 500000000, 1, alpha, 0, 0, 0, 0, 0);
	grid.addVertex(tier0);
	String name;
	int linkId = nodesN;
	for (int i = 1; i < nodesN; i++){
		   CompNode node = new CompNode(i, "N"+i, false, false, false, true, true, disk, Ncpu, alpha, 0, 0, 0, 0, 0);
		   grid.addVertex(node);
		   name = node.getName() + "->" + tier0.getName();
		   NetworkLink link = new NetworkLink(linkId++,name, node.getId() , tier0.getId(), bandwidth, false);		    
		   grid.addEdge(tier0, node,link);
	}
	return grid;
    }
    
    private static void writeGrid(SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid, String prefix) throws IOException{
	String nodesFilename = prefix + "_nodes.txt";
	String linksFilename = prefix + "_network.txt";
	writeNodes(grid, nodesFilename);
	writeLinks(grid, linksFilename);
    }
    
    private static void writeNodes(SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid, String filename) throws IOException{
	System.out.println("Writing nodes to file: " + filename);
	Writer writer = new BufferedWriter(new OutputStreamWriter(
	          new FileOutputStream(filename)));
	String header = "#resource_name Number_of_PE ProcessingRate(job length / per second) storage_size(in MB) "
		+ "isInputSource isOutputDestination isInputDestination isOutputSource"
		+ " maxGridlets gridletFile";
	System.out.println(header);
	writer.write(header + "\n");
	String record;
	for (CompNode node: grid.vertexSet()){
	    record = getNodeRecord(node);
	    System.out.println(record);
	    writer.write(record + "\n");
	}
	writer.close();
    }
    
    private static void writeLinks(SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid, String filename) throws IOException{
	System.out.println("Writing links to file: " + filename);
	Writer writer = new BufferedWriter(new OutputStreamWriter(
	          new FileOutputStream(filename)));
	String header = "#node1 node2 baud_rate(Gbps)";
	System.out.println(header);
	writer.write(header + "\n");
	String record;
	for (NetworkLink link: grid.edgeSet() ){
	    record = getLinkRecord(link,grid);
	    System.out.println(record);
	    writer.write(record + "\n");
	}
	
	writer.close();
    }
    
    static String getNodeRecord(CompNode node){
	//resource_name Number_of_PE ProcessingRate(job length / per second) storage_size(in MB)
	//isInputSource isOutputDestination isInputDestination isOutputSource 
	//maxGridlets gridletFile
	String indent = " ";
	StringBuffer buf = new StringBuffer();
	buf.append(node.getName() + indent);
	buf.append(node.getCpuN() + indent);
	buf.append((int) node.getAlpha() + indent);
	buf.append(node.getDisk() + indent);
	buf.append(node.isInputSource() + indent);
	buf.append(node.isOutputDestination() + indent);
	buf.append(node.isInputDestination() + indent); 
	buf.append(node.isOutputSource() + indent); 	
	if (node.isOutputDestination()){
	    buf.append("200000 F:/git/Grid_simulation/grid/src/main/java/flow_model/input/200kJobsUniqueIds.csv");
	    return buf.toString();
	}
	
	if (node.isInputSource()){
	    buf.append("20000 F:/git/Grid_simulation/grid/src/main/java/flow_model/input/200kPart1_10.csv");
	}else{
		buf.append("0" + indent);
		buf.append("null");
	}
	return buf.toString();
    }
    
    private static String getLinkRecord(NetworkLink link, SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid){
	//#node1 node2 baud_rate(Gbps)
	String indent = " ";
	StringBuffer buf = new StringBuffer();
	buf.append(grid.getEdgeSource(link).getName() + indent);
	buf.append(grid.getEdgeTarget(link).getName()  + indent);
	buf.append(link.getBandwidth());	
	return buf.toString();
    }

}

class NodeFactory implements VertexFactory{
    private int id;
	int Ncpu = 100;	
	float alpha = 1;
	int numberOfStorages;
	int s,p;

    public NodeFactory(int numberOfStorages,int maxCPUs) {
	this.id = 0;
	this.Ncpu = maxCPUs;
	this.numberOfStorages = numberOfStorages;
	this.s = 0;
	this.p = 0;
    }
    

    public Object createVertex() {
	if (s < numberOfStorages){	    
	    CompNode storage = new CompNode(id++, "S"+s++, false, true, false, false, false, 500000000, 1, alpha, 0, 0, 0, 0, 0);
	    return storage;
	}	
	int cpu =  10 + (int) (Math.random() * ( Ncpu -10 ) );
	long disk = cpu * 30000;
	SimpleDirectedWeightedGraph<CompNode, NetworkLink> grid = new SimpleDirectedWeightedGraph<CompNode, NetworkLink>(NetworkLink.class);	
	CompNode node = new CompNode(id++, "P"+p++, false, false, false, true, true, disk, cpu, alpha, 0, 0, 0, 0, 0);
	//System.out.println("Created node: " + GridGenerator.getNodeRecord(node));
	return node;
    }    
}

class DummyFactory implements VertexFactory{
    private Integer id;
    public DummyFactory() {
	this.id = new Integer(0);
    }
    
    public Object createVertex() {
	return id++;
    }
    
}

