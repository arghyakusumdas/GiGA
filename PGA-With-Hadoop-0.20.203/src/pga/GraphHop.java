package pga;
import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;

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

class GraphHop {
	private static final Logger sLogger = Logger.getLogger(GraphHop.class);
	public static boolean V = false;
	private static final String SELFLOOP ="selfloop";
	private static final String MATETHREADMSGS="matethreadmsgs";
	private static final String NODES = "nodes";
	private static final String SHORT ="short";
	private static final String LONG ="long";
	private static final String INVALID="invalid";
	private static final String VALID="valid";
	private static final String TOOLONG="toolong";
	private static final String ACTIVE="active";
		
	public static class GraphHopMap
	{
		GraphSurf graphSurf = null;
		boolean FIRST_HOP = false;  // TODO - Cheack parameter passing
		private static int K = 0;  // TODO - Check parameter passing
		private long selfloop;
		
		public GraphHopMap(GraphSurf graphsurf)
		{
			this.graphSurf = graphsurf;
		}
		public void setK(int k){
			this.K= k;
		}
		public void print_hopmsg(Node node, String path, String dest, 
		         String curdist, String outdir, 
		         String expdist, String expdir) throws IOException
		         //,
		         //OutputCollector<Text, Text> output,
		         //Reporter reporter) throws IOException
		{
            int matethreadmsgs = 0;
			
			String startnode = null;
			int pos = path.indexOf(':');
			if (pos > 0)
			{
				startnode = path.substring(0, pos);
				//System.err.println("Avoid selfloops to " + startnode + " in " + path);
			}

			for (String curdir : Node.dirs)
			{
				String tt = outdir + curdir;

				List<String> edges = node.getEdges(tt);

				if (edges != null)
				{
					for (String v : edges)
					{
						if ((startnode != null) && (v.equals(startnode)))
						{
							if (V) { System.err.println("Found self loop: " + path + " to " + v); }
							//reporter.incrCounter("Pga", "selfloop", 1);
					    //TODO Different method to implement below functionality		
						//	this.graphSurf.aggregate(SELFLOOP, new LongWritable(1));
							selfloop++;
						}
						else
						{
							//output.collect(new Text(v), 
							//	new Text(Node.MATETHREAD + "\t" + path + ":" + tt + "\t" +
							//			curdist + "\t" + curdir + "\t" + 
							//			expdist + "\t" + expdir + "\t" +
							//			dest));
							graphSurf.sendMessage(new Text(v),new Text(Node.MATETHREAD + "\t" + path + ":" + tt + "\t" +
									curdist + "\t" + curdir + "\t" + 
									expdist + "\t" + expdir + "\t" +
									dest));
							//this.graphSurf.aggregate(MATETHREADMSGS, new LongWritable(1));
							matethreadmsgs++;
						}
					}
				}
			}

			//reporter.incrCounter("Pga", "matethreadmsgs", matethreadmsgs);
			//TODO - PSK redesgin aggregaters
			//this.graphSurf.aggregate(MATETHREADMSGS, new LongWritable(1));
			//this.graphSurf.aggregate(MATETHREADMSGS,new LongWritable(matethreadmsgs));
		}
		public void sendMessages(Node node, boolean firsthop) throws IOException 
		{
			//Check belowline 
			FIRST_HOP= firsthop;
			if (FIRST_HOP) 
			{
				List<String> bundles = node.getBundles();
				
				if (bundles != null)
				{
					int curdist = -K+1;

					for(String bstr : bundles)
					{
						String [] vals = bstr.split(":");
						String dest     = vals[0];
						String edgetype = vals[1];
						String expdist  = vals[2];
						//String weight   = vals[3];
						//String unique   = vals[4];

						String outdir = edgetype.substring(0, 1);
						String expdir = edgetype.substring(1, 2);

						//print_hopmsg(node, node.getNodeId(), dest,
						//		Integer.toString(curdist), outdir,
						//		expdist, expdir,
						//		output, reporter);
						print_hopmsg(node,node.getNodeId(),dest,Integer.toString(curdist),outdir,expdist,expdir);
					}
				}
			}
			else
			{				
				List<String> matethreads = node.getMateThreads();
				/*
				if(matethreads != null)
				{
					System.out.print("PSK mate thread count "+ matethreads.size()+"\n");
				}
				*/
				if (matethreads != null)
				{
					for (String hopmsg : matethreads)
					{
						String [] vals = hopmsg.split("%");
						String path    = vals[0];
						String curdist = vals[1];
						String outdir  = vals[2];
						String expdist = vals[3];
						String expdir  = vals[4];
						String dest    = vals[5];

					//	print_hopmsg(node, path, dest,
					//			curdist, outdir,
					//			expdist, expdir,
					//			output, reporter);
						print_hopmsg(node,path,dest,curdist,outdir,expdist,expdir);
					}

					node.clearMateThreads();
				}
			}
			//output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			//reporter.incrCounter("Pga", "nodes", 1);
			
			//TODO - redesign PSK
			//this.graphSurf.aggregate(NODES, new LongWritable(1));
			
			
		}
	
	}
	public static class GraphHopRed
	{
		GraphSurf graphSurf = null;
		private static int K = 0;
		private static int INSERT_LEN = 0;
		private static int MIN_WIGGLE = 0;
		private static long WIGGLE = 0;
		
 
		public void setK(int k){
			this.K = k;
		}
		
		public void setInsertLength(int insert)
		{
			this.INSERT_LEN = insert;
		}
		
		public void setMinWigle(int minwiggle){
			this.MIN_WIGGLE = minwiggle;
		}
				
		public GraphHopRed(GraphSurf graphsurf) {
			this.graphSurf = graphsurf;
			
		}
		public class Hop
		{
			String path;
			int curdist;
			String curdir;
		    int expdist;
		    String expdir;
		    String dest;
		    
		    public Hop(String[] vals, int offset)
		    {
		    	path    = vals[offset];
		    	curdist = Integer.parseInt(vals[offset+1]);
		    	curdir  = vals[offset+2];
		    	expdist = Integer.parseInt(vals[offset+3]);
		    	expdir  = vals[offset+4];
		    	dest    = vals[offset+5];
		    }
		    
		    public String toString()
		    {
		    	return dest + " " + expdist + expdir + " | cur: " + curdist + curdir + " " + path;
		    }
		}
		public void parseMessages(Node node, Iterator<Text> iter) throws IOException
		{
			List<Hop> hops = new ArrayList<Hop>();
			int sawnode = 0;
			//Setting the value for the wiggle, standard deviation of the insert length
			WIGGLE = Node.mate_wiggle(INSERT_LEN, MIN_WIGGLE);
			//sLogger.info("inside parse messages PSK  "+ WIGGLE);
			
			while(iter.hasNext())
			{
				String msg = iter.next().toString();

				//System.err.println(nodeid.toString() + "\t" + msg);

				String [] vals = msg.split("\t");
				
				if (vals[0].equals(Node.MATETHREAD))
				{
					Hop h = new Hop(vals, 1);
					hops.add(h);
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
			
			long foundshort   = 0;
			long foundlong    = 0;
			long foundinvalid = 0;
			long foundvalid   = 0;
			long active       = 0;
			long toolong      = 0;
			List<String> bundles = node.getBundles();
			if (bundles != null)
			{
				for (String bstr : bundles)
				{
					if (bstr.charAt(0) == '#')
					{
						// There is a valid path already saved away
				        foundvalid++;
				        //TODO  ridesign PSK
				        //this.graphSurf.aggregate(VALID, new LongWritable(1));
					}
				}
			}
			if (hops.size() > 0)
			{
				for (Hop h : hops)
				{
					if (V) { System.err.println("Checking : " + h.toString()); }

					if (h.dest.equals(node.getNodeId()))
					{
						if (h.curdist < h.expdist - WIGGLE)
						{
							if (V) { System.err.println("Found too short: " + h.curdist + " exp: " + h.expdist + " " + h.path); }
							
							foundshort++;
							//TODO ridesign
							//this.graphSurf.aggregate(SHORT,new LongWritable(1));
							continue; // don't try to extend this search, because the dest node is supposed to be unique
						}

						if (h.curdist > h.expdist + 2*WIGGLE)
						{
							if (V) { System.err.println("Found too long: " + h.curdist + " exp: " + h.expdist + " " + h.path); }
							foundlong++;
							//TODO ridesign
							//this.graphSurf.aggregate(LONG, new LongWritable(1));
							
							continue;
						}

						if (!h.curdir.equals(h.expdir))
						{
							if (V) { System.err.println("Found invalid"); }
							foundinvalid++;
							//TODO redesign PSK
							//this.graphSurf.aggregate(INVALID, new LongWritable(1));
							continue;
						}

						// Success!
						foundvalid++;
						//TODO ridesign PSK
						//this.graphSurf.aggregate(VALID, new LongWritable(1));

						String pp = "#" + h.path + ":" + node.getNodeId();
						node.addBundle(pp);

						if (V) { System.err.println("Found valid path: " + pp); }
					}
					else
					{
						if (h.curdist > h.expdist + 2*WIGGLE)
						{
							if (V) { System.err.println("too long: " + h.curdist + " exp: " + h.expdist + " " + h.path); }
							
							toolong++;
							continue;
						}

						// The current path is still active, save away for next hop
						int curdist = h.curdist + node.len() - K + 1;
						String path = h.path + ":" + node.getNodeId();

						String msg = path      + "%" +
						curdist   + "%" +
						h.curdir  + "%" +
						h.expdist + "%" +
						h.expdir  + "%" +
						h.dest;

						if (V) { System.err.println("Keep searching: " + msg); }

						node.addMateThread(msg);
						active++;
						//TODO redisgn PSK 
						//this.graphSurf.aggregate(ACTIVE, new LongWritable(1));
					}
				} 
			}
			this.graphSurf.aggregate(ACTIVE, new LongWritable((long) active));
			
			//System.out.print("PSK active "+ active);
			//output.collect(nodeid, new Text(node.toNodeMsg()));
			//reporter.incrCounter("Pga", "foundshort",   foundshort);
			//reporter.incrCounter("Pga", "foundlong",    foundlong)
			//reporter.incrCounter("Pga", "foundinvalid", foundinvalid);
			//reporter.incrCounter("Pga", "foundvalid",   foundvalid);
			//reporter.incrCounter("Pga", "active",       active);
			//reporter.incrCounter("Pga", "toolong",      toolong);
			
		}
	}

}
