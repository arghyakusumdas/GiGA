package pga;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;



public class GiraphPairMark  
{	
	private static final Logger sLogger = Logger.getLogger(GiraphPairMark.class);
	
	public static class PairMarkMapper  
	{
		private long randseed = 0;
		private Random rfactory = new Random();
		MergeComputation merge=null;
		//private Vertex<Text, MyEdgeValue, Text> vertex;
		PairMarkMapper(MergeComputation mergeComputation)
		{
			this.merge=mergeComputation;
			
		}
		public void setRand(long seed)
		{
			this.randseed = seed;
		}
		public boolean isMale(String nodeid)
		{
			// randseed=vertex.getPeer().getCounter("Pga", "rand").getValue();
			
			rfactory.setSeed(nodeid.hashCode() ^ randseed);
			
			double rand = rfactory.nextDouble();
			
			boolean male = (rand >= .5);
			
			//System.err.println(nodeid + " " + randseed + " " + male+" "+ rand);

			return male;
		}
		
		public TailInfo getBuddy(Node node, String dir)
		{
			if (node.canCompress(dir))
			{
				return node.gettail(dir);
			}
			
			return null;
		}
		
		public void sendMessages(Node node)
                throws IOException 
        {
			
			TailInfo fbuddy = getBuddy(node, "f");
			TailInfo rbuddy = getBuddy(node, "r");

			if ((fbuddy != null) || (rbuddy != null))
			{
				String nodeid = node.getNodeId();

				String compress = null;
				String compressdir = null;
				String compressbdir = null;

				if (isMale(node.getNodeId()))
				{
					// Prefer Merging forward
					if (fbuddy != null)
					{
						boolean fmale = isMale(fbuddy.id);

						if (!fmale)
						{
							compress     = fbuddy.id;
							compressdir  = "f";
							compressbdir = fbuddy.dir;
						}
					}

					if ((compress == null) && (rbuddy != null))
					{
						boolean rmale = isMale(rbuddy.id);

						if (!rmale)
						{
							compress     = rbuddy.id;
							compressdir  = "r";
							compressbdir = rbuddy.dir;
						}
					}
				}
				else
				{
					if ((rbuddy != null) && (fbuddy != null))
					{
						boolean fmale = isMale(fbuddy.id);
						boolean rmale = isMale(rbuddy.id);

						if (!fmale && !rmale &&
								(nodeid.compareTo(fbuddy.id) < 0) && 
								(nodeid.compareTo(rbuddy.id) < 0))
						{
							// FFF and I'm the local minimum, go ahead and compress
							compress     = fbuddy.id;
							compressdir  = "f";
							compressbdir = fbuddy.dir;
						}
					}
					else if (rbuddy == null)
					{
						boolean fmale = isMale(fbuddy.id);

						if (!fmale && (nodeid.compareTo(fbuddy.id) < 0))
						{
							// Its X*=>FF and I'm the local minimum
							compress     = fbuddy.id;
							compressdir  = "f";
							compressbdir = fbuddy.dir;
						}
					}
					else if (fbuddy == null)
					{
						boolean rmale = isMale(rbuddy.id);

						if (!rmale && (nodeid.compareTo(rbuddy.id) < 0))
						{
							// Its FF=>X* and I'm the local minimum
							compress     = rbuddy.id;
							compressdir  = "r";
							compressbdir = rbuddy.dir;
						}
					}
				}

				if (compress != null)
				{

					//Save that I'm supposed to merge
					node.setMerge(compressdir);

					// Now tell my ~CD neighbors about my new nodeid
					String toupdate = Node.flip_dir(compressdir);

					for(String adj : Node.dirs)
					{
						String key = toupdate + adj;

						String origadj = Node.flip_dir(adj) + compressdir;
						String newadj  = Node.flip_dir(adj) + compressbdir;

						List<String> edges = node.getEdges(key);

						if (edges != null)
						{
							for (String p : edges)
							{
								
								merge.sendMessage(new Text(p), new Text(Node.UPDATEMSG + "\t" + nodeid + "\t" + origadj + "\t" + compress + "\t" + newadj));
								
							}
						}
					}
				}
			}

        }
	}
	
	public static class PairMarkReducer 
	{
		private static long randseed = 0;
		private MergeComputation merge;
		public PairMarkReducer(MergeComputation mergeComputation) {
			this.merge=mergeComputation;
		}

		public void set(long seed) 
		{
			randseed = seed;
		}
		
		private class Update
		{
			public String oid;
			public String odir;
			public String nid;
			public String ndir;
		}
		
		public void parseMessages(Node node, Iterator<Text> iter)
				throws IOException 
		{
			
			List<Update> updates = new ArrayList<Update>();
			
			//int sawnode = 0;
			
			while(iter.hasNext())
			{
				String msg = iter.next().toString();
				
				//System.err.println(key.toString() + "\t" + msg);
				
				String [] vals = msg.split("\t");
				
//				if (vals[0].equals(Node.NODEMSG))
//				{
//					node.parseNodeMsg(vals, 0);
//					sawnode++;
//				}
			    if (vals[0].equals(Node.UPDATEMSG))
				{
					Update up = new Update();
					
					up.oid  = vals[1];
					up.odir = vals[2];
					up.nid  = vals[3];
					up.ndir = vals[4];
					
					updates.add(up);
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
			
			if (updates.size() > 0)
			{
				for(Update up : updates)
				{
					node.replacelink(up.oid, up.odir, up.nid, up.ndir);
				}
			}
			
			//output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}

}
