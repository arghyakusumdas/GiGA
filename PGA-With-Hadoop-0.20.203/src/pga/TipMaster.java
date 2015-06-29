package pga;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/** Master compute which uses aggregators. To be used for testing. */
public class TipMaster extends
    DefaultMasterCompute {
	/** Name of regular aggregator */
	private static final String TIPS_FOUND= "tips_found";	
	private static final String TIPS_KEPT= "tips_kept";
	  

  @Override
  public void compute() {
    long superstep = getSuperstep();
    if(superstep == 0)
    {
    	setAggregatedValue(TIPS_FOUND, new LongWritable(0));
    	setAggregatedValue(TIPS_KEPT, new LongWritable(0));
    	GiraphRemoveTips.GiraphRemoveTipsMapper.tips_found=0;
    	GiraphRemoveTips.GiraphRemoveTipsReducer.tips_kept=0;
    }
        if (superstep == 3) {
        	
        	System.out.println( " \n" + ((LongWritable)getAggregatedValue(TIPS_FOUND)).get()+ "tips found");
        	System.out.println( "\n " + ((LongWritable)getAggregatedValue(TIPS_KEPT)).get()+ "tips remaining");
        	//Pga.tfound= ((LongWritable)getAggregatedValue(TIPS_FOUND)).get();
        	//Pga.tkept= ((LongWritable)getAggregatedValue(TIPS_KEPT)).get();
        	System.out.println("Before reading values");
        	Long tipsFound = ((LongWritable)getAggregatedValue(TIPS_FOUND)).get();
        	Long tipsRemains = ((LongWritable)getAggregatedValue(TIPS_KEPT)).get();
        	System.out.println("After  reading values Tipes Found "+tipsFound +"Tips Remaining "+tipsRemains);
        	if(tipsFound>0) {
        	try
        	{
        		System.out.println("Opening file to write the count values \n");
        		File output = new File(PgaConfig.localBasePath+"/counts_tip.txt");
        		//File output = new File("/home/pkondi1/counts_tip.txt");
        		//File output = new File("counts_tip.txt");
        		BufferedWriter writer = new BufferedWriter(new FileWriter(output));
        		writer.write(tipsFound+" "+tipsRemains);
        		writer.close();
        	}
        	
        	catch(IOException ex)
        	{
        		ex.printStackTrace();
        	}
        	}
        	haltComputation();
    	   }
        
       
    
  }

  @Override 
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerPersistentAggregator(
    		TIPS_FOUND, LongSumAggregator.class);
    registerPersistentAggregator(
    		TIPS_KEPT, LongSumAggregator.class);
  }
}