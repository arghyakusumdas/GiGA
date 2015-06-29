package pga;

import java.io.IOException;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
public class GraphSurf extends BasicComputation<Text, Text, Text, Text> 
{
	GraphHop.GraphHopMap graphHopMap =null;
	GraphHop.GraphHopRed graphHopRed= null;
	long superStepID = 0;
	long noOfVertices = 0;
	private static final String NUMBER_OF_VERTICES = "vertices";
	private static final Logger sLogger = Logger.getLogger(GraphSurf.class);
	
	public  GraphSurf(){
		graphHopMap = new GraphHop.GraphHopMap(this);
		graphHopRed = new GraphHop.GraphHopRed(this);				
		
	}
	
	
	public void preSuperstep(){
		int k = Integer.parseInt(getConf().get("K"));
		this.graphHopMap.setK(k);
		this.graphHopRed.setK(k);
		this.graphHopRed.setInsertLength(Integer.parseInt(getConf().get("insert_Length")));
		this.graphHopRed.setMinWigle(Integer.parseInt(getConf().get("min_wiggle")));
	}
	
	public void compute(Vertex<Text,Text,Text> vertex,Iterable<Text> messages)
	   throws IOException {
		Node node = new Node();
		Text value=vertex.getValue();
		node.fromNodeMsg(value.toString());
	
		superStepID = getSuperstep();
		//sLogger.info("We are inside superstep " + superStepID+" PSK");
		
		if (superStepID == 0){
			this.graphHopMap.sendMessages(node,superStepID==0);
		}
		else if(superStepID<14)
		{
			//System.out.print(" Before node__ID "+node.nodeid_m+ " bundles "+node.getBundles().size()+ "mate threads "+node.getMateThreads().size());
			this.graphHopRed.parseMessages(node, messages.iterator());
			
			//System.out.print(" After node__ID "+  node.nodeid_m + " bundles "+node.getBundles().size()+ "mate threads "+node.getMateThreads().size());
			this.graphHopMap.sendMessages(node,superStepID==0);
		}
		else
		{
			this.graphHopRed.parseMessages(node, messages.iterator());
		}
		if(vertex.getValue() != null)
		{
			vertex.setValue(new Text(node.toNodeMsg(true)));
		}
		if(getSuperstep() > 500)
			vertex.voteToHalt();
				
	}	
			
}
