package pga;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class MergeComputation extends BasicComputation<Text, Text, Text, Text> {
	private static final String RAND = "rand";
	GiraphPairMark.PairMarkMapper markMapper = null;
	GiraphPairMark.PairMarkReducer markReducer = null;
	GiraphPairMerge.PairMergeMapper mergeMapper = null;
	GiraphPairMerge.PairMergeReducer mergeReducer = null;
	private static final String PHASE  = "phase";
	private static final String STEP = "step";
	
	//Classes related to Remove tips
	TipComputation tipcompute = null;
	GiraphRemoveTips.GiraphRemoveTipsMapper tipMapper = null;
	GiraphRemoveTips.GiraphRemoveTipsReducer tipReducer = null;
	GiraphRemoveTips.GiraphRemoveTipsCombiner tipCombiner = null;
	
	//Classes related to bubble phse
	BubbleComputation bubbCompute = null;
	GiraphFindBubbles.GiraphFindBubblesMapper fbubbleMapper = null;
	GiraphFindBubbles.GiraphFindBubblesReducer fbubbleReducer = null;
	GiraphPopBubbles.GiraphPopBubblesMapper pbubbleMapper = null;
	GiraphPopBubbles.GiraphPopBubblesReducer pbubbleReducer = null;	
	Node node = null;
	Vertex vertex = null;
	
	
	public MergeComputation()
	{
		this.markMapper = new GiraphPairMark.PairMarkMapper(this);
		this.markReducer = new GiraphPairMark.PairMarkReducer(this);
		this.mergeMapper = new GiraphPairMerge.PairMergeMapper(this);
		this.mergeReducer = new GiraphPairMerge.PairMergeReducer(this);
		
		//Initialize objects for tipRemoval
		this.tipMapper = new GiraphRemoveTips.GiraphRemoveTipsMapper(this);
		this.tipReducer = new GiraphRemoveTips.GiraphRemoveTipsReducer(this);
		this.tipCombiner = new GiraphRemoveTips.GiraphRemoveTipsCombiner(this);
		
		//Initialize objects for bubble computation
		this.fbubbleMapper = new GiraphFindBubbles.GiraphFindBubblesMapper(this);
		this.fbubbleReducer = new GiraphFindBubbles.GiraphFindBubblesReducer(this);
		this.pbubbleMapper = new GiraphPopBubbles.GiraphPopBubblesMapper(this);
		this.pbubbleReducer = new GiraphPopBubbles.GiraphPopBubblesReducer(this);
	}
	@Override
	public void preSuperstep() {
		if(getSuperstep() % 2 == 0){
			Double rand=((DoubleWritable)getAggregatedValue(RAND)).get();
			this.markMapper.setRand((int) (rand * 10000000));
			//System.err.println( "Rand number genrated "+ rand);
		}
		int k = Integer.parseInt(getConf().get("K"));
		this.mergeReducer.setReads(k);
		this.tipMapper.setReads(k);
		this.tipReducer.setReads(k);
		long TipLength = Long.parseLong(getConf().get("TIPLENGTH"));
		this.tipMapper.setTipLength(TipLength);
		this.fbubbleMapper.setReads(k);
		long MaxBubbleLen =  Long.parseLong(getConf().get("MAXBUBBLELEN"));  //75;   
		this.fbubbleMapper.setMaxBubLen(MaxBubbleLen);
		this.fbubbleReducer.setReads(k);
		float BUBBLEEDITRATE = Float.parseFloat(getConf().get("BUBBLEEDITRATE")); //0.05f; 
		this.fbubbleReducer.setMaxEditRate(BUBBLEEDITRATE);
	  }
	
	@Override
	public void compute(Vertex<Text, Text, Text> vertex, Iterable<Text> messages)
			throws IOException {
		Node node = new Node();
		Text value=vertex.getValue();
		node.fromNodeMsg(value.toString());
		
		long stepps= getSuperstep();
		
		long phase = ((LongWritable)getAggregatedValue(PHASE)).get();
   	    int step = ((IntWritable)getAggregatedValue(STEP)).get();
   	    // System.out.println("Phase "+ phase +"Step "+ step);
		
   	    if(phase == 0)
   	    {
   	    	if(step >-1)
   	    	{
   	    		switch ((int) step % 4)
   	    		{
   	    			case 0:
   	    				markMapper.sendMessages(node);
   	    				break;
   	    			case 1:
   	    				markReducer.parseMessages(node, messages.iterator());
   	    				break;
   	    			case 2:
   	    				mergeMapper.sendMessages(node,  vertex);
   	    				break;
   	    			case 3:
   	    				mergeReducer.parseMessages(node, messages.iterator());
   	    				break;
   	    			}
   	    		}
   	    }   //End of phase 0	    
   	    else if(phase == 1)
   	    {
   	    	if(step > -1)
   	    	{
   	    		switch ((int) step )
   	    		{
   	    			case 0:
   	    				tipMapper.sendMessages(node, vertex);
   	    				break;
   	    			case 1:   				
   	    				tipReducer.parseMessages(node, messages.iterator());
   	    				break;   				
   	    			case 2:	
   	    				tipCombiner.deleteNodes(node, messages.iterator(), vertex);   				
   	    		}
   	    	}
   	    }
   	    else if(phase == 2)
   	    {
   	    	switch ((int) step)
   	    	{
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
   	    }
   	    else if(phase == 3)
   	    {
   	    	if(step >-1)
   	    	{
   	    		switch ((int) step % 4)
   	    		{
   	    			case 0:
   	    				markMapper.sendMessages(node);
   	    				break;
   	    			case 1:
   	    				markReducer.parseMessages(node, messages.iterator());
   	    				break;
   	    			case 2:
   	    				mergeMapper.sendMessages(node,  vertex);
   	    				break;
   	    			case 3:
   	    				mergeReducer.parseMessages(node, messages.iterator());
   	    				break;
   	    			}
   	    		}
   	    }
			
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
        /*
		if (stepps == 0) {

		markMapper.sendMessages(node);

		} else {

		switch ((int) stepps % 2) {

		case 1:

		markReducer.parseMessages(node, messages.iterator());

		mergeMapper.sendMessages(node, vertex);

		break;

		case 0:

		mergeReducer.parseMessages(node, messages.iterator());

		markMapper.sendMessages(node);

		break;
		}

		}
        */

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