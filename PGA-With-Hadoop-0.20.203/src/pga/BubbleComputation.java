package pga;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class BubbleComputation extends BasicComputation<Text, Text, Text, Text> {

	GiraphFindBubbles.GiraphFindBubblesMapper fbubbleMapper = null;
	GiraphFindBubbles.GiraphFindBubblesReducer fbubbleReducer = null;
	GiraphPopBubbles.GiraphPopBubblesMapper pbubbleMapper = null;
	GiraphPopBubbles.GiraphPopBubblesReducer pbubbleReducer = null;
	
	public BubbleComputation()
	{
		this.fbubbleMapper = new GiraphFindBubbles.GiraphFindBubblesMapper(this);
		this.fbubbleReducer = new GiraphFindBubbles.GiraphFindBubblesReducer(this);
		this.pbubbleMapper = new GiraphPopBubbles.GiraphPopBubblesMapper(this);
		this.pbubbleReducer = new GiraphPopBubbles.GiraphPopBubblesReducer(this);
	}
	@Override
	public void preSuperstep() {
		if(getSuperstep()==0)
		{
		int k = Integer.parseInt(getConf().get("K"));
		this.fbubbleMapper.setReads(k);
		long MaxBubbleLen =  Long.parseLong(getConf().get("MAXBUBBLELEN"));  //75;   
		this.fbubbleMapper.setMaxBubLen(MaxBubbleLen);
		}
		
		if(getSuperstep()==1)
		{
		int k = Integer.parseInt(getConf().get("K"));
		this.fbubbleReducer.setReads(k);
		float BUBBLEEDITRATE = Float.parseFloat(getConf().get("BUBBLEEDITRATE")); //0.05f; 
		this.fbubbleReducer.setMaxEditRate(BUBBLEEDITRATE);
		}
		if(getSuperstep()==2)
		{
			int k = Integer.parseInt(getConf().get("K"));
			this.pbubbleMapper.setReads(k);
		}
		if(getSuperstep()==3)
		{
			int k = Integer.parseInt(getConf().get("K"));
			this.pbubbleReducer.setReads(k);
		}
		
		}

	
	@Override
	public void compute(Vertex<Text, Text, Text> vertex, Iterable<Text> messages)
			throws IOException {
		Node node = new Node();
		Text value=vertex.getValue();
		node.fromNodeMsg(value.toString());
		
		
		switch ((int) getSuperstep()) {
		case 0:
			fbubbleMapper.sendMessages(node);
			break;
		case 1:
			fbubbleReducer.parseMessages(node, messages.iterator());
			pbubbleMapper.sendMessages(node);
			break;
		case 2:
			pbubbleReducer.parseMessages(node, messages.iterator(), vertex);
			break;
		}
		if(vertex.getValue() != null)
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
