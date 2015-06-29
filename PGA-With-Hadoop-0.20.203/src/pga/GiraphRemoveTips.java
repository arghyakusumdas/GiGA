package pga;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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


public class GiraphRemoveTips 
{	
	private static final Logger sLogger = Logger.getLogger(GiraphRemoveTips.class);
	/** Name of regular aggregator */
	
	// RemoveTipsMapper
	///////////////////////////////////////////////////////////////////////////
	
	public static class GiraphRemoveTipsMapper
	{
		private static int K = 0;
		public static long TIPLENGTH = 0;
		public static boolean VERBOSE = false;
		public static long tips_found;

		TipComputation tipcom = null;
		Pga contra = null;
		MergeComputation merge =null;
		GiraphRemoveTipsMapper(TipComputation tipcomputation)
		{
			this.tipcom= tipcomputation;
		}
		GiraphRemoveTipsMapper(MergeComputation mergecomputation)
		{
			this.merge = mergecomputation;
		}
		public void setReads(int k)
		{
			this.K=k;
		}
		public void setTipLength(long TIPLENGTH)
		{
			this.TIPLENGTH= TIPLENGTH;
		}
		
		
		public void sendMessages(Node node, Vertex vertex) throws IOException 
				
		{
			
			int fdegree = node.degree("f");
			int rdegree = node.degree("r");
			int len     = node.len();

			if ((len <= TIPLENGTH) && (fdegree + rdegree <= 1))
			{
				// this.tipcom.aggregate("tips_found", new LongWritable(1));
				this.merge.aggregate("tips_found", new LongWritable(1));
				tips_found++;

				if (VERBOSE)
				{
					System.err.println("Removing tip " + node.getNodeId() + " len=" + len);
				}

				if ((fdegree == 0) && (rdegree == 0))
				{
					//this node is not connected to the rest of the graph
					//nothing to do
					vertex.setValue(null);
					vertex.voteToHalt();
				}
				else
				{
					// Tell the one neighbor that I'm a tip
					String linkdir = (fdegree == 0) ? "r" : "f";

					for(String adj : Node.dirs)
					{
						String key = linkdir + adj;

						List<String> edges = node.getEdges(key);

						if (edges != null)
						{
							if (edges.size() != 1)
							{
								throw new IOException("Expected a single edge from " + node.getNodeId());
							}

							String p = edges.get(0);

							if (p.equals(node.getNodeId()))
							{
								// short tandem repeat, trim away
							}
							else
							{
								String con = Node.flip_dir(adj) + Node.flip_dir(linkdir);
								//this.tipcom.sendMessage(new Text(p), new Text(Node.TRIMMSG + "\t" + con + "\t" + node.toNodeMsg(true)));
								this.merge.sendMessage(new Text(p), new Text(Node.TRIMMSG + "\t" + con + "\t" + node.toNodeMsg(true)));
								//vertex.setValue(null);
								//vertex.voteToHalt();
							}
						}
					}
				}
			}
		}
		
		public static long getTipsFound() {
			return (tips_found);
		}
	  
	  }
	
	// RemoveTipsReducer
	///////////////////////////////////////////////////////////////////////////
	
	public static class GiraphRemoveTipsReducer
	{
		private static int K = 0;
		public static long tips_kept;
		TipComputation tipcom = null;
		MergeComputation merge = null;
		GiraphRemoveTipsReducer(TipComputation tipcomputation)
		{
			this.tipcom= tipcomputation;
		}
		GiraphRemoveTipsReducer(MergeComputation mergecomputation)
		{
			this.merge = mergecomputation;
		}
		public void setReads(int k)
		{
			this.K=k;
		}
		
		public void parseMessages(Node node, Iterator<Text> iter)
				throws IOException 
		{
			Map<String, List<Node>> tips = new HashMap<String, List<Node>>();
			
			
			
			while(iter.hasNext())
			{
				String msg = iter.next().toString();
				
				//System.err.println(key.toString() + "\t" + msg);
				
				String [] vals = msg.split("\t");
				
				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					
				}
				else if (vals[0].equals(Node.TRIMMSG))
				{
					String adj = vals[1];
					
					Node tip = new Node(vals[2]);
					tip.parseNodeMsg(vals, 3);
					
					if (tips.containsKey(adj))
					{
						tips.get(adj).add(tip);
					}
					else
					{
						List<Node> tiplist = new ArrayList<Node>();
						tiplist.add(tip);
						tips.put(adj, tiplist);
					}
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
			
			
			if (tips.size() != 0)
			{
			
				for(String d : Node.dirs)
				{
					int deg = node.degree(d);
					
					int numtrim = 0;
					
					List<Node> ftips = tips.get(d+"f");
					List<Node> rtips = tips.get(d+"r");
					
					if (ftips != null) { numtrim += ftips.size(); }
					if (rtips != null) { numtrim += rtips.size(); }
					
					if (numtrim == 0) { continue; }
					
					Node besttip = null;
					int bestlen = 0;
					
					if (numtrim == deg)
					{
						// All edges in this direction are tips, only keep the longest one
						
						if (ftips != null)
						{
							for (Node t : ftips)
							{
								if (t.len() > bestlen)
								{
									bestlen = t.len();
									besttip = t;
								}
							}
						}
						
						if (rtips != null)
						{
							for (Node t : rtips)
							{
								if (t.len() > bestlen)
								{
									bestlen = t.len();
									besttip = t;
								}
							}
						}
						
						//this.tipcom.addVertexRequest(new Text(besttip.getNodeId()), new Text(besttip.toNodeMsg()));
						  //this.tipcom.sendMessage(new Text(besttip.getNodeId()), new Text(Node.TIPKEEP));
			              this.merge.aggregate("tips_kept", new LongWritable(1));
			              tips_kept++;		              
			                 		              
					}	
					if (ftips != null)
					{
						String adj = d+"f";
						
						for (Node t : ftips)
						{
							if (t != besttip)
							{
								node.removelink(t.getNodeId(), adj);
								//this.tipcom.sendMessage(new Text(t.getNodeId()), new Text(Node.TIPDELETE));
								this.merge.sendMessage(new Text(t.getNodeId()), new Text(Node.TIPDELETE));
								
							}
						}
					}
					
					if (rtips != null)
					{
						String adj = d+"r";
						
						for (Node t : rtips)
						{
							if (t != besttip)
							{
								node.removelink(t.getNodeId(), adj);
								//this.tipcom.sendMessage(new Text(t.getNodeId()), new Text(Node.TIPDELETE));
								this.merge.sendMessage(new Text(t.getNodeId()), new Text(Node.TIPDELETE));
								
							}
						}
					}
				}
			}
			
			
		}
		
		public static long getTipsKept() {
			return (tips_kept);
		}
	}
	

	public static class GiraphRemoveTipsCombiner
	{
		
		TipComputation tipcom = null;
		MergeComputation merge = null;
		GiraphRemoveTipsCombiner(TipComputation tipcomputation)
		{
			this.tipcom= tipcomputation;
		}
		GiraphRemoveTipsCombiner(MergeComputation mergecomputation)
		{
			this.merge = mergecomputation;
		}
		
		public void deleteNodes(Node node, Iterator<Text> iter, Vertex vertex)
				throws IOException 
		{
			while(iter.hasNext())
			{
				String msg = iter.next().toString();
				
				//System.err.println(key.toString() + "\t" + msg);
				
				String [] vals = msg.split("\t");
				
				if (vals[0].equals(Node.TIPDELETE))
				{
					vertex.setValue(null);
					vertex.voteToHalt();
				}
			}	
		}
		
	}
}
