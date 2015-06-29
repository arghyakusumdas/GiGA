package pga;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TipComputation extends BasicComputation<Text, Text, Text, Text> {

	GiraphRemoveTips.GiraphRemoveTipsMapper tipMapper = null;
	GiraphRemoveTips.GiraphRemoveTipsReducer tipReducer = null;
	GiraphRemoveTips.GiraphRemoveTipsCombiner tipCombiner = null;
	Node node = null;
	Vertex vertex = null;
	
	
	public TipComputation()
	{
		this.tipMapper = new GiraphRemoveTips.GiraphRemoveTipsMapper(this);
		this.tipReducer = new GiraphRemoveTips.GiraphRemoveTipsReducer(this);
		this.tipCombiner = new GiraphRemoveTips.GiraphRemoveTipsCombiner(this); 
		
	}
	@Override
	public void preSuperstep() {
		
		int k = Integer.parseInt(getConf().get("K"));
		this.tipMapper.setReads(k);
		this.tipReducer.setReads(k);
		long TipLength = Long.parseLong(getConf().get("TIPLENGTH"));
		this.tipMapper.setTipLength(TipLength);
		
	   	}

	
	@Override
	public void compute(Vertex<Text, Text, Text> vertex, Iterable<Text> messages)
			throws IOException {
		this.node = new Node();
		Text value=(Text)vertex.getValue();
		this.node.fromNodeMsg(value.toString());
		
		
		switch ((int) getSuperstep()) {
		case 0:
			this.tipMapper.sendMessages(node, vertex);
			break;
		case 1:
			
			this.tipReducer.parseMessages(node, messages.iterator());
			break;
			
		case 2:	
			this.tipCombiner.deleteNodes(node, messages.iterator(), vertex);
			
		}
		
		if (vertex.getValue() != null)
	    {
	      vertex.setValue(new Text(node.toNodeMsg(true)));
	    }
	
		if(getSuperstep() > 500)
			vertex.voteToHalt();
				
	}
	
	@Override
	public void postSuperstep() {
		//System.out.println("Completed superStep: "+getSuperstep());
	  }


}
