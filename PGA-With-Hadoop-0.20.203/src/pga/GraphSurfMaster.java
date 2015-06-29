package pga;
import java.io.PrintStream;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class GraphSurfMaster extends
DefaultMasterCompute {
	private static final String SHORT ="short";
	private static final String LONG ="long";
	private static final String INVALID="invalid";
	private static final String VALID="valid";
	private static final String TOOLONG="toolong";
	private static final String ACTIVE="active";
	
	
	public void compute() {
		 long superstep = getSuperstep();
		    if(superstep == 0)
		    {
		    	System.out.print("We are in superstep 0");
		    	setAggregatedValue(SHORT, new LongWritable(0));
		    	setAggregatedValue(LONG, new LongWritable(0));
		    	setAggregatedValue(INVALID, new LongWritable(0));
		    	setAggregatedValue(VALID, new LongWritable(0));
		    	setAggregatedValue(TOOLONG, new LongWritable(0));
		    	setAggregatedValue(ACTIVE, new LongWritable(0));
		    	
		    }
		   // This values needs to be reconfigured. 
		    if(superstep ==16){
		    	//System.out.print("Aggregated value"+((LongWritable) getAggregatedValue(MATETHREADS)).get());
		    	System.out.print("We are in superstep 16");
		    	System.out.print("Aggregated value"+((LongWritable) getAggregatedValue(ACTIVE)).get());
		    	haltComputation();
		    } 
	}
	
	public void initialize() throws InstantiationException,
    IllegalAccessException {
  registerPersistentAggregator(
		  SHORT, LongSumAggregator.class);
  registerPersistentAggregator(
		  LONG, LongSumAggregator.class);
  registerPersistentAggregator(
		  INVALID, LongSumAggregator.class);
  registerPersistentAggregator(
		  VALID, LongSumAggregator.class);
  registerPersistentAggregator(
		  TOOLONG, LongSumAggregator.class);
  registerPersistentAggregator(
		  ACTIVE, LongSumAggregator.class);
  
}
	
	

}
