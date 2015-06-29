package pga;

import java.io.IOException;
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



public class GiraphPairMerge  
{	
	private static final Logger sLogger = Logger.getLogger(GiraphPairMerge.class);
	/** Name of regular aggregator */
	  private static final String NEEDSCOMPRESS = "needscompress";
	public static class PairMergeMapper  
	{
		MergeComputation merge=null;
		PairMergeMapper(MergeComputation mergeComputation)
		{
			this.merge=mergeComputation;
		}
		public void sendMessages(Node node, Vertex vertex)
		throws IOException 
		{
			

			String mergedir = node.getMerge();

			if (mergedir != null)
			{
				TailInfo compressed = node.gettail(mergedir);

				this.merge.sendMessage(new Text(compressed.id), new Text(Node.COMPRESSPAIR + "\t" + mergedir + "\t" + compressed.dir + "\t" + node.toNodeMsg(true)));
				//output.collect(new Text(compressed.id),
						//new Text(Node.COMPRESSPAIR + "\t" + mergedir + "\t" + compressed.dir + "\t" + node.toNodeMsg(true)));
				//System.err.println(node.getNodeId() + " " + mergedir + " " );
					vertex.setValue(null);
					vertex.voteToHalt();
				
			}
			

			//this.vertex.getPeer().incrementCounter("Pga", "nodes", 1);
		}
	}
	
	public static class PairMergeReducer  
	{
		private  int K = 0;
		private static boolean VERBOSE = false;
		
		MergeComputation merge=null;
		private long needCompress;
		PairMergeReducer(MergeComputation mergeComputation)
		{
			this.merge=mergeComputation;
			
		}
		
		public void setReads(int k)
		{
			this.K=k;
		}
		
		class CompressInfo
		{
			String dir;
			String bdir;
			Node   node;
			
			public String toString()
			{
				return node.getNodeId() + " " + dir + " " + bdir;
			}
		}
		
		public void mergepair(Node node, CompressInfo ci) throws IOException
		{
			
		      if (VERBOSE)
		      {
		        System.err.println("[==");
		        System.err.println("Merging " + node.getNodeId() + " " + ci);
		        System.err.println(node.toNodeMsg(true));
		        System.err.println(ci.node.toNodeMsg(true));
		      }

		      // update the node string
		      String astr = ci.node.str();
		      if (ci.dir.equals("r")) 
		      { 
		        astr = Node.rc(astr);
		        ci.node.revreads();
		      }

		      String bstr = node.str();
		      if (ci.bdir.equals("r")) 
		      { 
		        bstr = Node.rc(bstr); 
		        node.revreads();
		      }

		      int shift = astr.length() - K + 1;
		      ci.node.addreads(node, shift);
		      node.setreads(ci.node);

		      String str = Node.str_concat(astr, bstr, K);
		      if (ci.bdir.equals("r")) { str = Node.rc(str); }
		      
		      node.setstr(str);

		      if (ci.bdir.equals("r"))
		      {
		    	  node.revreads();
		      }

		      int amerlen = astr.length() - K + 1;
		      int bmerlen = bstr.length() - K + 1;
		      
		      float ncov = node.cov();
		      float ccov = ci.node.cov();
		      
		      node.setCoverage(((ncov * amerlen) + (ccov * bmerlen)) / (amerlen + bmerlen));
		      
		      List<String> threads = ci.node.getThreads();
		      if (threads != null)
		      {
		    	  if (!ci.bdir.equals(ci.dir))
		    	  {
		    		  // Flip the direction of the threads
		    		  for(String thread : threads)
		    		  {
		    			  String [] vals = thread.split(":"); // t link r

		    			  String ta = Node.flip_dir(vals[0].substring(0,1));
		    			  String tb = vals[0].substring(1,2);

		    			  node.addThread(ta+tb, vals[1], vals[2]);
		    		  }
		    	  }
		    	  else
		    	  {
		    		  for(String thread : threads)
		    		  {
		    			  node.addThread(thread);
		    		  }
		    	  }
		      }

		      // update the appropriate neighbors with $cnode's pointers

		      //print " orig: $cdir $cbdir\n";
		      
		      ci.dir = Node.flip_dir(ci.dir);
		      ci.bdir = Node.flip_dir(ci.bdir);

		      for(String adj : Node.dirs)
		      {
		        String key = ci.dir + adj;
		        String fkey = ci.bdir + adj;

		        //print "  Updating my $fkey with cnode $key\n";
		        
		        List<String> ce = ci.node.getEdges(key);
		        node.setEdges(fkey, ce);
		      }

		      // Now update the can compress flag
		      //if(ci.node.canCompress(ci.dir))
			      //this.vertex.getPeer().incrementCounter("Pga", "compressible", 1);
		      node.setCanCompress(ci.bdir, ci.node.canCompress(ci.dir));

		      if (VERBOSE) { System.err.println(node.toNodeMsg());}
		}
		
		public void parseMessages(Node node, Iterator<Text> iter)
				throws IOException 
		{
						
			CompressInfo fci = null;
			CompressInfo rci = null;
			
			while(iter.hasNext())
			{
				String msg = iter.next().toString();
				
				//System.err.println(node.toString() + "\t" + msg);
				
				String [] vals = msg.split("\t");
				
				if (vals[0].equals(Node.COMPRESSPAIR))
				{
					CompressInfo ci = new CompressInfo();
					
					ci.dir  = vals[1];
					ci.bdir = vals[2];
					ci.node = new Node(vals[3]);
					ci.node.parseNodeMsg(vals, 4);
					
					if (ci.bdir.equals("f"))
					{
						if (fci != null)
						{
							throw new IOException("Multiple f compresses to " + node.getNodeId());
						}
						
						fci = ci;
					}
					else
					{
						if (rci != null)
						{
							throw new IOException("Multiple r compresses to " + node.getNodeId());
						}
						
						rci = ci;
					}
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
			
//			if (sawnode != 1)
//			{
//				throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + node.getNodeId());
//			}

			if (fci != null) { mergepair(node, fci); }
			if (rci != null) { mergepair(node, rci); }

			node.cleanThreads();

			// Update the tail pointers, and cancompress flag
			for (String adj : Node.dirs)
			{
				TailInfo ti = node.gettail(adj);

				// check for a cycle, and break link
				if ((ti == null) || ti.id.equals(node.getNodeId()))
				{
					node.setCanCompress(adj, false);
					//this.vertex.getPeer().incrementCounter("Pga", "compressible", 1);
				}
			}

			if (node.canCompress("f") || node.canCompress("r"))
			{
				//System.err.println("incrementing NEEDSCOMPRESS");
				this.merge.aggregate(NEEDSCOMPRESS, new LongWritable(1));
				needCompress++;
				//this.vertex.getPeer().incrementCounter("Pga", "needscompress", 1);
			}
//			else
//			{
//				this.vertex.voteToHalt();
//			}

			//output.collect(nodeid, new Text(node.toNodeMsg()));
		}
		public long getNeedsCompress() {
			return needCompress;
		}
		
	}
	
	

}
