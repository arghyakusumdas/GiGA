package pga;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/** Master compute which uses aggregators. To be used for testing. */
public class BubbleMaster extends
    DefaultMasterCompute {
	/** Name of regular aggregator */
	  private static final String POTENTIALBUBBLES = "potentialbubbles";
	  private static final String BUBBLESPOPPED= "bubblespopped";
	  public static long potbubb;
	  public static long bubb;

  @Override
  public void compute() {
    long superstep = getSuperstep();
    if(superstep == 0)
    {
    	System.out.print("Bubble Phase We are in superstep 0");
    	setAggregatedValue(POTENTIALBUBBLES, new LongWritable(0));
    	setAggregatedValue(BUBBLESPOPPED, new LongWritable(0));
    }
    else if(superstep==1){
    	System.out.print("Bubble Phase We are in superstep 1 \n");
    }
    else if(superstep==2){
    	System.out.print("Bubble phase We are in superstep 2");
    }
    else if (superstep >= 3)
    {       	
        	//System.out.println( " " + ((LongWritable)getAggregatedValue(POTENTIALBUBBLES)).get()+ "bubbles found");
        	//System.out.println( " " + ((LongWritable)getAggregatedValue(BUBBLESPOPPED)).get()+ "bubbles popped");
    	  
        	bubb = ((LongWritable)getAggregatedValue(BUBBLESPOPPED)).get();
        	potbubb = ((LongWritable)getAggregatedValue(POTENTIALBUBBLES)).get();   		
    		
    		if(bubb>0){
    			try{
    				//File output = new File("/home/pkondi1/counts_bubb.txt");
    				File output = new File(PgaConfig.localBasePath+"/counts_bubb.txt");
            		//File output = new File("counts_bubb.txt");
            		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
            		bw.write(bubb + " "+potbubb);
            		bw.close();
    			}
    			catch(IOException ex){
    				ex.printStackTrace();
    			}
    		}
    		// Halt the computation after three supersteps
    		haltComputation();   
    	   }        
       
    
  }
    
  @Override 
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerPersistentAggregator(
    		POTENTIALBUBBLES, LongSumAggregator.class);
    registerPersistentAggregator(
    		BUBBLESPOPPED, LongSumAggregator.class);
  }
}