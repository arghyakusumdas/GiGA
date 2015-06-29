package pga;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.codehaus.jackson.map.DeserializerFactory.Config;

/** Master compute which uses aggregators. To be used for testing. */
public class MasterVertex extends
    DefaultMasterCompute {
	/** Name of regular aggregator */
	
	/** Constants needed to identify the Phase of Assembly Line   */
	private static final String COMPRESSION = "compression";
	private static final String REMOVETIPS = "removetips";
	private static final String POPBUBBLES = "popbubbles";
	private static final String SCAFFOLDER ="scaffolder";
	private static final String PHASE  = "phase";
	private static final String STEP = "step";
	
	//compression  = 00
	//RemoveTips   = 01
	//PopBubbles   = 02
	//Scaffolder   = 03
	
	// Constants for Compression phase
	private static final String NEEDSCOMPRESS = "needscompress";
	private static final String RAND = "rand";
	
	//Constants for Remove tips phase
	private static final String TIPS_FOUND= "tips_found";	
	private static final String TIPS_KEPT= "tips_kept";
	
	//Constants for popbubble
	private static final String POTENTIALBUBBLES = "potentialbubbles";
	private static final String BUBBLESPOPPED= "bubblespopped";
	public static long potbubb;
	public static long bubb;

  @Override
  public void compute() 
  {	  
    long superstep = getSuperstep();
    long phase = 0 ;
    int step = 0;
    
    //System.out.println("Entering giraph jobs");
    //Initial setup on phase and step
    
    //System.out.println("PgaConfig.trackSSPath = " + PgaConfig.trackSSPath.toString());
    
    if(superstep==0)
    {
    	System.out.println("\n First superStep to set phase and  step number for stage management \n");
    	setAggregatedValue(PHASE, new LongWritable(0));
    	setAggregatedValue(STEP, new IntWritable(0));
    	//Using file read write method for synchronizing between the phases and steps
    	//File write = new File("step.txt");
    	//File write = new File("/home/pkondi1/step.txt");
    	File write = new File(PgaConfig.trackSSPath.toString()+"/step.txt");
    	if(write.exists()) 
    		write.delete();
    	try
    	{
    		BufferedWriter bw = new BufferedWriter(new FileWriter(write));
    		bw.write(Integer.toString(step));
    		bw.close();
    	}
    	catch(IOException ex)
    	{
    		ex.printStackTrace();
    	}
    }
    else
    {
    	 phase = ((LongWritable)getAggregatedValue(PHASE)).get();
    	 //step = ((LongWritable)getAggregatedValue(STEP)).get();
    	 //Get the step from the previous run 
    	 //File read = new File("step.txt");
    	 //File read = new File("/home/pkondi1/step.txt");
    	 File read = new File(PgaConfig.trackSSPath.toString()+"/step.txt");
    	 try
    	 {
    		 BufferedReader rd = new BufferedReader(new FileReader(read));
    		 String val = rd.readLine();
    		 rd.close();
    		 val=val.trim();    	
    		 step =Integer.parseInt(val);
    		 
    		 step = step+1;    	
    		 System.out.println("Superstep "+ superstep +" Phase "+ phase+ " step "+ step + "\n") ;
    		 BufferedWriter wt = new BufferedWriter(new FileWriter(read));
    		 wt.write(Integer.toString(step));
    		 wt.close();
    	 }
    	 catch(IOException ex)
    	 {
    		 ex.printStackTrace();
    	 }
    	 
    	 setAggregatedValue(STEP, new IntWritable(step));
    	    	
    }  //end for Initial set on phase and step
    //Below code is for stage management 
    //Run the individual phase of the assembly 
    if(phase==0)
    {
    	 if(step%4 == 0)
    	    {
    	    	setAggregatedValue(RAND, new DoubleWritable(0));
    	    	Double rand = Math.random();
    	    	setAggregatedValue(RAND, new DoubleWritable(rand));
    	    	System.out.println("Still need compress "+ ((LongWritable)getAggregatedValue(NEEDSCOMPRESS)).get() + " at super step " + superstep+ "and Rand number genrated"+ rand);
    	    }
    	 if (step > 0 && step%4 == 0)
    	 {
    		 long needscompress=((LongWritable)getAggregatedValue(NEEDSCOMPRESS)).get();
      	     if(needscompress == 0)
      	     {
      	    	 System.out.println("Now time to halt all vertex at super step"+ superstep);
      	    	 //haltComputation();
      	    	 //BELOW LINE IS CRITICAL WHICH NEEDS TO BE UPDATED TO THE NEXT PHASE IN THE ASSEMBLY 
       		    phase = phase +1 ;
       	    	setAggregatedValue(PHASE, new LongWritable(phase));
       	    	setAggregatedValue(STEP, new IntWritable(-1));
       	    	//Re-initialize the count of step and phase to zero
       	    	//File write = new File("step.txt");
       	    	File write = new File(PgaConfig.trackSSPath.toString()+"/step.txt");
       	    	//File write = new File("/home/pkondi1/step.txt");
       	    	try
       	    	{
       	    		BufferedWriter wr = new BufferedWriter(new FileWriter(write));
       	    		wr.write(Integer.toString(-1));
       	    		wr.close();
       	    	}
       	    	catch(IOException ex)
       	    	{
       	    		ex.printStackTrace();
       	    	}
       	    	//We move to next phase i.e tip removal 
       	    	//setAggregatedValue(TIPS_FOUND, new LongWritable(0));
             	//setAggregatedValue(TIPS_KEPT, new LongWritable(0));
      	     }
         }
         else
         {
          	setAggregatedValue(NEEDSCOMPRESS, new LongWritable(0));
         }
    	    	
    } //End of if for compression phase which is phase 0   
    
    else if(phase==1) 
    {
    	//System.out.println("Superstep "+ superstep +" Phase "+ phase+ " step "+ step + "\n") ;
    	if(step==0)
    	{
    		setAggregatedValue(TIPS_FOUND, new LongWritable(0));
        	setAggregatedValue(TIPS_KEPT, new LongWritable(0));
        	//System.out.println("Superstep "+superstep+" Phase " + phase + " step "+ step);
        	System.out.println( " \n" + ((LongWritable)getAggregatedValue(TIPS_FOUND)).get()+ "tips found");
        	System.out.println( "\n " + ((LongWritable)getAggregatedValue(TIPS_KEPT)).get()+ "tips remaining");
        	//step = step+1;
        	//setAggregatedValue(STEP, new IntWritable(step)); 
    	}
    	else if(step ==1 )
    	{
    		System.out.println( " \n" + ((LongWritable)getAggregatedValue(TIPS_FOUND)).get()+ "tips found");
        	System.out.println( "\n " + ((LongWritable)getAggregatedValue(TIPS_KEPT)).get()+ "tips remaining");
    		//System.out.println("Superstep "+superstep+" Phase " + phase + " step "+ step);
    		//step = step+1;
        	//setAggregatedValue(STEP, new LongWritable(step));
    	}
    	else if(step == 2)
    	{
    		System.out.println( " \n" + ((LongWritable)getAggregatedValue(TIPS_FOUND)).get()+ "tips found");
        	System.out.println( "\n " + ((LongWritable)getAggregatedValue(TIPS_KEPT)).get()+ "tips remaining");
        	System.out.println("\n Am I exiting here");
    		//System.out.println("Superstep "+superstep+" Phase " + phase + " step "+ step);
    		//step = step+1;
        	//setAggregatedValue(STEP, new LongWritable(step));
    	}
    	if(step==3)
    	{
    		//System.out.println("Superstep "+superstep+" Phase " + phase + " step "+ step + "\n");
    		System.out.println( " \n" + ((LongWritable)getAggregatedValue(TIPS_FOUND)).get()+ "tips found");
        	System.out.println( "\n " + ((LongWritable)getAggregatedValue(TIPS_KEPT)).get()+ "tips remaining");
        	//System.out.println("Before reading values");
        	Long tipsFound = ((LongWritable)getAggregatedValue(TIPS_FOUND)).get();
        	Long tipsRemains = ((LongWritable)getAggregatedValue(TIPS_KEPT)).get();
        	//System.out.println("After  reading values Tipes Found "+tipsFound +"Tips Remaining "+tipsRemains);
        	long needscompress=((LongWritable)getAggregatedValue(NEEDSCOMPRESS)).get();
        	
        	if(tipsFound>0)
        	{
        		
        		//If we get more tips then go back to compression         	    		
           	    // After the tip removal we run compression
        		//TODO - This is temporary approach of exiting the giraph job.        	
            	//File write = new File("exit.txt");
        		File write = new File(PgaConfig.trackSSPath.toString()+"/exit.txt");
        		//File write = new File("/home/pkondi1/exit.txt");
      	    	try
      	    	{
      	    		BufferedWriter wr = new BufferedWriter(new FileWriter(write));
      	    		wr.write(Long.toString(tipsFound));
      	    		wr.close();
      	    		
      	    		//Argo: Start: scp exit.txt to all hadoop nodes including masters/////
      	    		{//copy to masters
       	        	   //System.out.println("Copying to masters");
       	        	   BufferedReader br = new BufferedReader(new FileReader(PgaConfig.hadoopHomePath.toString()+"/conf/masters")); 
       	        	   String master_line = null;
       	        	   while ((master_line = br.readLine()) != null) {
       	        		   System.out.println("Copied to: " + master_line);
       	        		   String command = "scp " + PgaConfig.trackSSPath + "/exit.txt " + master_line + ":" + PgaConfig.trackSSPath + "/";
       	        		   System.out.println(command);
       	        		   Process p = Runtime.getRuntime().exec(command); //Sice hadoop uses same user name (passphraseless ssh) we can scp like this
       	        	   }
       	        	   br.close();
       	           }
      	           {//copy to slaves
      	        	   System.out.println("Copying to slaves");
      	        	   BufferedReader br = new BufferedReader(new FileReader(PgaConfig.hadoopHomePath.toString()+"/conf/slaves")); 
      	        	   String slave_line = null;
      	        	   while ((slave_line = br.readLine()) != null) {
      	        		   //System.out.println("Copied to: " + slave_line);
      	        		   //Process p1 = Runtime.getRuntime().exec("echo arghyatest >>" + PgaConfig.trackSSPath + "/argo.txt");
      	        		   String command = "scp " + PgaConfig.trackSSPath + "/exit.txt " + slave_line + ":" + PgaConfig.trackSSPath + "/";
      	        		   System.out.println(command);
      	        		   Process p = Runtime.getRuntime().exec(command); //Sice hadoop uses same user name (passphraseless ssh) we can scp like this
      	        	   }
      	        	   br.close();
      	          }
      	          Thread.sleep(15000);
      	           
      	           //Argo: End: scp exit.txt to all hadoop nodes including masters/////*/
      	    	}
      	    	catch(Exception ex)
      	    	{
      	    		ex.printStackTrace();
      	    	}      
      	    	//After writing the file we exit
      	    	System.out.println("Exiting from Giraph Job Tips found ="+tipsFound);
      	    	haltComputation();
      	    	//setAggregatedValue(PHASE, new LongWritable(0));
      	    	//setAggregatedValue(STEP, new IntWritable(-1)); 
      	    	
            } //end of tipsFound
        	else if(tipsFound==0 && needscompress == 0)
        	{
        		//haltComputation();        
        		//If we dont find tips and no nodes are availathen go next phase which is popbubble
        		System.out.println("Exiting from Giraph Job Tips found ="+tipsFound);
        		setAggregatedValue(PHASE, new LongWritable(2));
      	    	setAggregatedValue(STEP, new IntWritable(-1));
      	    	//File write = new File("step.txt");
      	    	File write = new File(PgaConfig.trackSSPath.toString()+"/step.txt");
      	    	//File write = new File("/home/pkondi1/step.txt");
      	    	try
      	    	{
      	    		BufferedWriter wr = new BufferedWriter(new FileWriter(write));
      	    		wr.write(Integer.toString(-1));
      	    		wr.close();
      	    	}
      	    	catch(IOException ex)
      	    	{
      	    		ex.printStackTrace();
      	    	}         	    	
        	} 
        	else
        	{
        		System.out.println("Exiting from Giraph Job Tips found ="+tipsFound);
        		haltComputation(); 
        	}
    	}    	
    } 
    else if(phase==2)
    {
    	if(step==0)
    	{
    		System.out.println("Superstep "+superstep+" Phase " + phase + " step "+ step + "\n");
    		//step = step+1;
        	//setAggregatedValue(STEP, new LongWritable(step));
        	
        	setAggregatedValue(POTENTIALBUBBLES, new LongWritable(0));
        	setAggregatedValue(BUBBLESPOPPED, new LongWritable(0));
    	}
    	else if(step==1)
    	{
    		System.out.println("Superstep "+superstep+" Phase " + phase + " step "+ step + "\n");
    		//step = step+1;
        	//setAggregatedValue(STEP, new LongWritable(step));
    	}
    	else if(step==2)
    	{
    		System.out.println("Superstep "+superstep+" Phase " + phase + " step "+ step + "\n");
    		//step = step+1;
        	//setAggregatedValue(STEP, new LongWritable(step));
    	}
    	else if(step >= 3)
    	{
    		//System.out.println("Superstep "+superstep+" Phase " + phase + " step "+ step + "\n");
    		bubb = ((LongWritable)getAggregatedValue(BUBBLESPOPPED)).get();
        	potbubb = ((LongWritable)getAggregatedValue(POTENTIALBUBBLES)).get();
        	System.out.println("Bubbles popped "+ bubb +"\n");
        	
        	if(bubb>0)
        	{
        		//If bubbles are popped go back to compression phase
        		//File write = new File("step.txt");
        		File write = new File(PgaConfig.trackSSPath.toString()+"/step.txt");
        		//File write = new File("/home/pkondi1/step.txt");
      	    	try
      	    	{
      	    		BufferedWriter wr = new BufferedWriter(new FileWriter(write));
      	    		wr.write(Integer.toString(-1));
      	    		wr.close();
      	    	}
      	    	catch(IOException ex)
      	    	{
      	    		ex.printStackTrace();
      	    	}
      	    	    	    	
      	    	setAggregatedValue(PHASE, new LongWritable(3));
      	    	setAggregatedValue(STEP, new IntWritable(-1));         	 
        	}
        	else
        	{
        		System.out.println("End of bubble phase"+"\n");
        		haltComputation();
        	}        	
    	}
    	//Increment the step within each phase
    	 
    }//End of bubble phase
    else if(phase == 3)
    {
    	if(step%4 == 0)
	    {
	    	setAggregatedValue(RAND, new DoubleWritable(0));
	    	Double rand = Math.random();
	    	setAggregatedValue(RAND, new DoubleWritable(rand));
	    	System.out.println("Still need compress "+ ((LongWritable)getAggregatedValue(NEEDSCOMPRESS)).get() + " at super step " + superstep+ "and Rand number genrated"+ rand);
	    }
	 if (step > 0 && step%4 == 0)
	 {
		 long needscompress=((LongWritable)getAggregatedValue(NEEDSCOMPRESS)).get();
  	     if(needscompress == 0)
  	     {
  	    	 System.out.println("Now time to halt all vertex at super step"+ superstep);
  	    	 //haltComputation();
  	    	 //When we are in bubble phase, at the end of bubble phase we need to do compression 
   		    phase = 2 ;
   	    	setAggregatedValue(PHASE, new LongWritable(phase));
   	    	setAggregatedValue(STEP, new IntWritable(-1));
   	    	//Re-initialize the count of step and phase to zero
   	    	//File write = new File("step.txt");
   	    	File write = new File(PgaConfig.trackSSPath.toString()+"/step.txt");
   	    	//File write = new File("/home/pkondi1/step.txt");
   	    	try
   	    	{
   	    		BufferedWriter wr = new BufferedWriter(new FileWriter(write));
   	    		wr.write(Integer.toString(-1));
   	    		wr.close();
   	    	}
   	    	catch(IOException ex)
   	    	{
   	    		ex.printStackTrace();
   	    	}
   	    	//We move to next phase i.e tip removal 
   	    	//setAggregatedValue(TIPS_FOUND, new LongWritable(0));
         	//setAggregatedValue(TIPS_KEPT, new LongWritable(0));
  	     }
     }
     else
     {
      	setAggregatedValue(NEEDSCOMPRESS, new LongWritable(0));
     }
    }
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /*
    if(superstep%2 == 0)
    {
    	setAggregatedValue(RAND, new DoubleWritable(0));
    	Double rand = Math.random();
    	setAggregatedValue(RAND, new DoubleWritable(rand));
    	//System.err.println("Still need compress "+ ((LongWritable)getAggregatedValue(NEEDSCOMPRESS)).get() + " at super step " + superstep+ "and Rand number genrated"+ rand);
    }
    if (superstep > 1 && superstep%2 == 1)
    {
    	   long needscompress=((LongWritable)getAggregatedValue(NEEDSCOMPRESS)).get();
    	   if(needscompress == 0)
    	   {
    		  //System.out.print("Total no of supersteps taken : "+superstep+"\n");
    		  haltComputation();
    	   }
    }
    else
    {
        	setAggregatedValue(NEEDSCOMPRESS, new LongWritable(0));
    }
     */
  }   

  @Override 
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerPersistentAggregator(
    		NEEDSCOMPRESS, LongSumAggregator.class);
    registerPersistentAggregator(
    		RAND, DoubleSumAggregator.class);
    registerPersistentAggregator(
    		POTENTIALBUBBLES, LongSumAggregator.class);
    registerPersistentAggregator(
    		BUBBLESPOPPED, LongSumAggregator.class);
    registerPersistentAggregator(
    		TIPS_FOUND, LongSumAggregator.class);
    registerPersistentAggregator(
    		TIPS_KEPT, LongSumAggregator.class);
    registerPersistentAggregator(
    		PHASE,  LongSumAggregator.class);
    registerPersistentAggregator(
    		STEP,  IntSumAggregator.class);
  }
}
