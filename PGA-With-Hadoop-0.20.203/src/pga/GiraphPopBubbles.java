package pga;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



public class GiraphPopBubbles
{	
	private static final Logger sLogger = Logger.getLogger(GiraphPopBubbles.class);
	
	/** Name of regular aggregator */
	private static final String BUBBLESPOPPED= "bubblespopped";	
	// PopBubblesMapper
	///////////////////////////////////////////////////////////////////////////
	
	public static class GiraphPopBubblesMapper 
	{
		private static int K = 0;
		BubbleComputation bubcom = null;
		private long bubblespopped;
		MergeComputation merge = null ;
		
		GiraphPopBubblesMapper(BubbleComputation bubblecomputation)
		{
			this.bubcom= bubblecomputation;
		}
		GiraphPopBubblesMapper(MergeComputation mergecomputation)
		{
			this.merge = mergecomputation;
		}
		public void setReads(int k)
		{
			this.K=k;
		}
		
		public void sendMessages(Node node)
		throws IOException 
		{
			//Node node = new Node();
			//node.fromNodeMsg(nodetxt.toString());

			List<String> bubbles = node.getBubbles();
			if (bubbles != null)
			{
				for(String bubble : bubbles)
				{
					String [] vals = bubble.split("\\|");
					String minor    = vals[0];
					String minord   = vals[1];
					String dead     = vals[2];
					String newd     = vals[3];
					String newid    = vals[4];
					String extracov = vals[5];
					/*
					bubcom.sendMessage(new Text(minor), 
							       new Text(Node.KILLLINKMSG + "\t" + minord + "\t" + dead + "\t" + newd + "\t" + newid));
					*/
					this.merge.sendMessage(new Text(minor), 
						       new Text(Node.KILLLINKMSG + "\t" + minord + "\t" + dead + "\t" + newd + "\t" + newid));

					//bubcom.sendMessage(new Text(dead), new Text(Node.KILLMSG));
					this.merge.sendMessage(new Text(dead), new Text(Node.KILLMSG));
					//bubcom.sendMessage(new Text(dead), new Text(Node.KILLMSG));
					this.merge.sendMessage(new Text(dead), new Text(Node.KILLMSG));
					//bubcom.sendMessage(new Text(newid), new Text(Node.EXTRACOV + "\t" + extracov));
					this.merge.sendMessage(new Text(newid), new Text(Node.EXTRACOV + "\t" + extracov));

					//this.bubcom.aggregate(BUBBLESPOPPED, new LongWritable(1));
					this.merge.aggregate(BUBBLESPOPPED, new LongWritable(1));
					bubblespopped++;
					
					//reporter.incrCounter("Pga", "bubblespopped", 1);
				}

				node.clearBubbles();
			}
		}

	}

	// PopBubblesReducer
	///////////////////////////////////////////////////////////////////////////

	public static class GiraphPopBubblesReducer 
	{
		private static int K = 0;
		BubbleComputation bubcom = null;
		MergeComputation merge = null;
		
		GiraphPopBubblesReducer(BubbleComputation bubblecomputation)
		{
			this.bubcom= bubblecomputation;
		}
		GiraphPopBubblesReducer(MergeComputation mergecomputation)
		{
			this.merge = mergecomputation;
		}
		public void setReads(int k)
		{
			this.K=k;
		}
		
		public class ReplacementLink
		{
		    public String deaddir;
		    public String deadid;
		    public String newdir;
		    public String newid;
		    
		    public ReplacementLink(String[] vals, int offset) throws IOException
		    {
		    	if (!vals[offset].equals(Node.KILLLINKMSG))
		    	{
		    		throw new IOException("Unknown msg");
		    	}
		    	
		    	deaddir = vals[offset+1];
		    	deadid  = vals[offset+2];
		    	newdir  = vals[offset+3];
		    	newid   = vals[offset+4];
		    }
		}
		
		public void parseMessages(Node node, Iterator<Text> iter, Vertex vertex)
		throws IOException 
		{	
			boolean killnode = false;
			float extracov = 0;
			List<ReplacementLink> links = new ArrayList<ReplacementLink>(); 

			while(iter.hasNext())
			{
				String msg = iter.next().toString();

				//System.err.println(nodeid.toString() + "\t" + msg);

				String [] vals = msg.split("\t");

				
				if (vals[0].equals(Node.KILLLINKMSG))
				{
					ReplacementLink link = new ReplacementLink(vals, 0);
					links.add(link);
				}
				else if (vals[0].equals(Node.KILLMSG))
				{
					killnode = true;
				}
				else if (vals[0].equals(Node.EXTRACOV))
				{
					extracov += Float.parseFloat(vals[1]);
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
			if (killnode)
			{				
				vertex.setValue(null);
				vertex.voteToHalt();
				return;
			}

			if (extracov > 0)
			{
				int merlen = node.len() - K + 1;
				float support = node.cov() * merlen + extracov;
				node.setCoverage((float) support /  (float) merlen);
			}

			if (links.size() > 0)
			{
				for(ReplacementLink link : links)
				{
					node.removelink(link.deadid, link.deaddir);
					node.updateThreads(link.deaddir, link.deadid, link.newdir, link.newid);
					
				}
				int threadsremoved = node.cleanThreads();				
			}
			//output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}
	
}
