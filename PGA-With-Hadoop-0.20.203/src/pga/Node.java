package pga;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * The node class represents a node in the graph.
 * Each node represents a k-mer and edges are drawn
 * between two nodes if they overlap by k-1 bases.
 * 
 * Each node contains a bunch of fields stored in a hash
 * table. The value of each field is a list of strings.
 * 
 * Each node stores a list of out going edges. The out 
 * going edges are divided into four groups:
 *  "ff", "fr", "rf","rr"
 * where each group corresponds to a different edge type.
 * The edge type is determined by the canonical direction
 * of the kmer (see BuildGraph.BuildGraphMapper.Map)  
 *  
 * 
 */
public class Node extends NodeBase {

	// Different messages can be serialized as strings.
	// The first field in a string denotes the type of message;
	// thereby indicating how the subsequent tab delimited values
	// should be interpreted.

	// NODEMSG - string represents the serialized version of a Node
	public static final String NODEMSG           = "N";
	public static final String HASUNIQUEP        = "P";
	public static final String UPDATEMSG         = "U";
	public static final String TRIMMSG           = "T";
	public static final String KILLMSG           = "K";
	public static final String EXTRACOV          = "V";
	public static final String KILLLINKMSG       = "L";
	public static final String COMPRESSPAIR      = "C";
	public static final String BUBBLELINKMSG     = "B";
	public static final String THREADIBLEMSG     = "I";
	public static final String RESOLVETHREADMSG  = "R";
	public static final String MATEDIST          = "D";
	public static final String MATEEDGE          = "E";
	public static final String TIPKEEP           = "TK";
	public static final String TIPDELETE          = "TD";

	// Node msg field codes
	public static final String STR      = "s"; // the sequence represented by this node 
	public static final String COVERAGE = "v"; // the coverage of sequencing
	public static final String MERTAG   = "t";
	public static final String R5       = "5";
	public static final String THREAD   = "d";

	public static final String CANCOMPRESS = "c";
	public static final String POPBUBBLE   = "p";
	public static final String MERGE       = "m";
	public static final String THREADPATH  = "e";
	public static final String MATETHREAD  = "a";
	public static final String BUNDLE      = "b";
    
    static String [] edgetypes = {"ff", "fr", "rf", "rr"};
    public static String [] dirs      = {"f", "r"};

	/**
	 * Use a hash table to store the values of the different fields.
	 */
	protected Map<String, List<String>> fields = new HashMap<String, List<String>>();
	
	/**
	 * Return a list of strings for the specified field.
	 * 
	 * If the field doesn't exist then a new list of strings is initialized
	 * and returned.
	 * 
	 * @param field - Name of the field
	 * @return - List of strings to represent this field. 
	 */
	protected List<String> getOrAddField(String field)
	{
		if (fields.containsKey(field))
		{
			return fields.get(field);
		}

		List<String> retval = new ArrayList<String>();
		fields.put(field, retval);

		return retval;
	}

	public void setMertag(String tag)
	{
		List<String> l = getOrAddField(MERTAG);
		l.clear();
		l.add(tag);
	}

	public String getMertag() throws IOException
	{
		if (!fields.containsKey(MERTAG))
		{
			throw new IOException("Mertag not found: " + toNodeMsg());
		}

		return fields.get(MERTAG).get(0);
	}

	public void setCoverage(float cov)
	{
		List<String> l = getOrAddField(COVERAGE);
		l.clear();		
		l.add(Float.toString(cov));		
	}


	public boolean isUnique(int MIN_CTG_LEN, float MIN_UNIQUE_COV, float MAX_UNIQUE_COV)
	{
		float cov = cov();
		int len = len();

		return ((cov >= MIN_UNIQUE_COV) && 
				(cov <= MAX_UNIQUE_COV) &&
				(len >= MIN_CTG_LEN));
	}




	public void setMerge(String dir)
	{
		List<String> l = getOrAddField(MERGE);
		l.clear();
		l.add(dir);
	}

	public String getMerge()
	{
		if (fields.containsKey(MERGE))
		{
			return fields.get(MERGE).get(0);
		}

		return null;
	}

	public void addR5(String tag, int offset, int isRC, int maxR5)
	{
		List<String> l = getOrAddField(R5);

		if (l.size() < maxR5)
		{
			if (isRC == 1)
			{
				l.add("~" + tag + ":" + offset);
			}
			else
			{
				l.add(tag + ":" + offset);
			}
		}
	}

	/**
	 * Add an outgoing edge to this node. 
	 * @param et - This is a two letter string e.g "rf", "fr" corresponding to the directions
	 * 	for the K-Mers
	 * @param v - A compressed string representing the K-Mer of the destination node
	 */
	public void addEdge(String et, String v)
	{
		List<String> l = getOrAddField(et);		
		l.add(v);
	}

	public List<String> getEdges(String et) throws IOException
	{
		if (et.equals("ff") ||
				et.equals("fr") ||
				et.equals("rr") ||
				et.equals("rf"))
		{
			return fields.get(et);
		}

		throw new IOException("Unknown edge type: " + et);
	}

	public void setEdges(String et, List<String> edges)
	{
		if (edges == null || edges.size() == 0)
		{
			fields.remove(et);	
		}
		else
		{
			fields.put(et, edges);
		}
	}

	public void clearEdges(String et)
	{
		fields.remove(et);
	}

	public boolean hasEdge(String et, String nid) throws IOException
	{
		List<String> edges = getEdges(et);
		if (edges == null) { return false; }

		for (String v : edges)
		{
			if (v.equals(nid))
			{
				return true;
			}
		}

		return false;
	}

	public void addBundle(String bun)
	{
		List<String> l = getOrAddField(BUNDLE);
		l.add(bun);
	}

	public List<String> getBundles()
	{
		if (fields.containsKey(BUNDLE))
		{
			return fields.get(BUNDLE);
		}

		return null;
	}

	public void clearBundles()
	{
		fields.remove(BUNDLE);
	}

	public List<String> getMateThreads()
	{
		if (fields.containsKey(MATETHREAD))
		{
			return fields.get(MATETHREAD);
		}

		return null;
	}

	public void addMateThread(String thread)
	{
		List<String> l = getOrAddField(MATETHREAD);
		l.add(thread);
	}

	public void clearMateThreads()
	{
		fields.remove(MATETHREAD);
	}

	public boolean canCompress(String d)
	{
		if (fields.containsKey(CANCOMPRESS + d))
		{
			return fields.get(CANCOMPRESS + d).get(0).equals("1");
		}

		return false;
	}

	/**
	 * Set the value of the field CANCOMPRESS in the direction "d".
	 * The name of the field is CANCOMPRESS+d.
	 * 
	 * I think this gets set by the reducer in Compressible.java. 
	 * 
	 * @param d
	 * @param can
	 */
	public void setCanCompress(String d, boolean can)
	{
		if (can)
		{
			List<String> l = getOrAddField(CANCOMPRESS + d);
			l.clear();
			l.add("1");
		}
		else
		{
			fields.remove(CANCOMPRESS+d);
		}
	}

	public void removelink(String id, String dir) throws IOException
	{
		boolean found = false;

		List<String> edges = getEdges(dir);

		if (edges != null)
		{
			for(int i = 0; i < edges.size(); i++)
			{
				if (edges.get(i).equals(id))
				{
					edges.remove(i);

					setEdges(dir, edges);

					found = true;
					break;
				}
			}
		}

		if (!found)
		{
			throw new IOException("Error removing link from " + getNodeId() + ": Can't find " + id + ":" + dir + "\n" + toNodeMsg());
		}
	}

	public boolean forceremovelink(String id, String dir) throws IOException
	{
		boolean found = false;

		List<String> edges = getEdges(dir);

		if (edges != null)
		{
			for(int i = 0; i < edges.size(); i++)
			{
				if (edges.get(i).equals(id))
				{
					edges.remove(i);

					setEdges(dir, edges);

					found = true;
					break;
				}
			}
		}

		return found;
	}

	public void replacelink(String o, String ot, String n, String nt) throws IOException
	{
		//System.err.println(nodeid_m + " replacing " + o + ":" + ot + " => " + n + ":" + nt);

		boolean found = false;

		List<String> l = getOrAddField(ot);
		for (int li = l.size() - 1; li >= 0; li--)
		{
			if (l.get(li).equals(o))
			{
				l.remove(li);
				found = true;
			}
		}

		if (!found)
		{
			throw new IOException("Couldn't find link " + o + " " + ot);
		}

		if (l.size() == 0)
		{
			fields.remove(ot);
		}

		l = getOrAddField(nt);
		l.add(n);

		if (fields.containsKey(THREAD))
		{
			l = getOrAddField(THREAD);

			for (int i = 0; i < l.size(); i++)
			{
				String thread = l.get(i);

				String [] vals = thread.split(":");

				if (vals[0].equals(ot) && vals[1].equals(o))
				{
					thread = nt + ":" + n + ":" + vals[2];
					l.set(i, thread);
				}
			}
		}
	}

	public void addThread(String et, String v, String read)
	{
		List<String> l = getOrAddField(THREAD);
		l.add(et + ":" + v + ":" + read);
	}

	public void addThread(String thread)
	{
		List<String> l = getOrAddField(THREAD);
		l.add(thread);
	}

	public List<String> getThreads()
	{
		if (fields.containsKey(THREAD))
		{
			return fields.get(THREAD);
		}

		return null;
	}

	public void clearThreads()
	{
		if (fields.containsKey(THREAD))
		{
			fields.remove(THREAD);
		}
	}


	public int cleanThreads()
	{
		int threadsremoved = 0;

		List<String> threads = fields.get(THREAD);

		if (threads != null)
		{
			// after tip removal, there may be dead threads
			// Only keep threads associated with current edges

			Map<String, Set<String>> edges = new HashMap<String, Set<String>>();

			for (String et : Node.edgetypes)
			{
				if (fields.containsKey(et))
				{
					Set<String> elist = new HashSet<String>();
					edges.put(et, elist);

					for(String v : fields.get(et))
					{
						elist.add(v);
					}
				}
			}

			List<String> newthreads = new ArrayList<String>();

			for(String thread : threads)
			{
				String [] vals = thread.split(":"); //tdir, tn, read

				if (edges.containsKey(vals[0]) && edges.get(vals[0]).contains(vals[1]))
				{
					newthreads.add(thread);
				}
				else
				{
					threadsremoved++;
				}
			}

			if (newthreads.size() == 0)
			{
				fields.remove(THREAD);
			}
			else
			{
				fields.put(THREAD, newthreads);
			}
		}

		return threadsremoved;
	}

	public void updateThreads(String olddir, String oldid, String newdir, String newid)
	{
		List<String> threads = getThreads();
		if (threads != null)
		{
			for(int ti = 0; ti < threads.size(); ti++)
			{
				String thread = threads.get(ti);
				String[] vals = thread.split(":");

				String t    = vals[0];
				String link = vals[1];
				String read = vals[2];

				if ((t.equals(olddir)) && link.equals(oldid))
				{
					thread = newdir + ":" + newid + ":" + read;
					threads.set(ti, thread);
				}
			}
		}
	}

	public void addThreadPath(String path)
	{
		List<String> l = getOrAddField(THREADPATH);
		l.add(path);
	}

	public void setThreadPath(String path)
	{
		List<String> l = getOrAddField(THREADPATH);
		l.clear();
		l.add(path);
	}

	public List<String> getThreadPath()
	{
		if (fields.containsKey(THREADPATH))
		{
			return fields.get(THREADPATH);
		}

		return null;
	}

	public void clearThreadPath()
	{
		fields.remove(THREADPATH);
	}

	public void clearThreadibleMsg()
	{
		fields.remove(THREADIBLEMSG);
	}

	public void addThreadibleMsg(String port)
	{
		List<String> l = getOrAddField(THREADIBLEMSG);
		l.add(port);
	}

	public List<String> getThreadibleMsgs()
	{
		if (fields.containsKey(THREADIBLEMSG))
		{
			return fields.get(THREADIBLEMSG);
		}

		return null;
	}


	public void addBubble(String minor, String vmd, String vid, String umd, String uid, float extracov)
	{
		String msg = minor + "|" + vmd + "|" + vid + "|" + umd + "|" + uid + "|" + extracov;

		List<String> l = getOrAddField(POPBUBBLE);
		l.add(msg);
	}

	public List<String> getBubbles()
	{
		if (fields.containsKey(POPBUBBLE))
		{
			return fields.get(POPBUBBLE);
		}

		return null;
	}

	public void clearBubbles()
	{
		fields.remove(POPBUBBLE);
	}

	public String getNodeId() { return nodeid_m; }
	public void setNodeId(String nid) { nodeid_m = nid; }

	public String toNodeMsg()
	{
		return toNodeMsg(false);
	}

	public void revreads()
	{
		int len = len();

		if (fields.containsKey(R5))
		{
			List<String> r5list = fields.get(R5);

			for (int i = 0; i < r5list.size(); i++)
			{
				String rstr = r5list.get(i);
				String [] vals = rstr.split(":");

				String read = vals[0];
				int pos = Integer.parseInt(vals[1]);

				if (read.startsWith("~"))
				{
					read = read.substring(1);
				}
				else
				{
					read = "~" + read;
				}

				pos = len - 1 - pos;
				rstr = read + ":" + pos;
				r5list.set(i, rstr);
			}
		}
	}

	/**
	 * Serialize the node to a string.
	 *
	 * @param tagfirst - If true, then the first field is nodeid_m.
	 * 
	 * The node is serialized is serialized as a tab delimited sequence of fields.
	 * Furthermore the '*' character is used as a separation field between fields.
	 * Each field is encoded as a pair
	 * 	  *FIELD_ID\tFIELD_VALUE
	 * where FIELD_ID is a string identifying the field and FIELD_VALUE is the value.
	 * For some fields (e.g the fields corresponding to the edge types) FIELD_VALUE can
	 * be a list of values in which case the items are tab delimeted. e.g
	 *    *FIELD_ID\tVALUE1\tVALUE2\tVALUE3
	 * 
	 * If tagfirst is true then the first value is nodeid_m.
	 * then  
	 * @return
	 */
	public String toNodeMsg(boolean tagfirst)
	{
		StringBuilder sb = new StringBuilder();

		DecimalFormat df = new DecimalFormat("0.00");

		if (tagfirst)
		{
			sb.append(nodeid_m);
			sb.append("\t");
		}

		sb.append(NODEMSG);

		sb.append("\t*"); sb.append(STR);      
		sb.append("\t"); sb.append(str_raw());

		sb.append("\t*"); sb.append(COVERAGE); 
		sb.append("\t"); sb.append(df.format(cov()));

		for(String t : edgetypes)
		{
			if (fields.containsKey(t))
			{
				sb.append("\t*"); sb.append(t);

				for(String i : fields.get(t))
				{
					sb.append("\t"); sb.append(i);
				}
			}
		}

		char [] dirs = {'f', 'r'};

		for(char d : dirs)
		{
			String t = CANCOMPRESS + d;
			if (fields.containsKey(t))
			{
				sb.append("\t*"); sb.append(t);
				sb.append("\t");  sb.append(fields.get(t).get(0));
			}
		}

		if (fields.containsKey(MERGE))
		{
			sb.append("\t*"); sb.append(MERGE); 
			sb.append("\t");  sb.append(fields.get(MERGE).get(0));
		}

		if (!tagfirst && fields.containsKey(MERTAG))
		{
			sb.append("\t*"); sb.append(MERTAG); 
			sb.append("\t");  sb.append(fields.get(MERTAG).get(0));
		}

		if (fields.containsKey(THREAD))
		{
			sb.append("\t*"); sb.append(THREAD);
			for(String t : fields.get(THREAD))
			{
				sb.append("\t"); sb.append(t);
			}
		}

		if (fields.containsKey(THREADPATH))
		{
			sb.append("\t*"); sb.append(THREADPATH);
			for(String t : fields.get(THREADPATH))
			{
				sb.append("\t"); sb.append(t);
			}
		}

		if (fields.containsKey(THREADIBLEMSG))
		{
			sb.append("\t*"); sb.append(THREADIBLEMSG);
			for(String t : fields.get(THREADIBLEMSG))
			{
				sb.append("\t"); sb.append(t);
			}
		}

		if (fields.containsKey(POPBUBBLE))
		{
			sb.append("\t*"); sb.append(POPBUBBLE);
			for(String t : fields.get(POPBUBBLE))
			{
				sb.append("\t"); sb.append(t);
			}
		}

		if (fields.containsKey(R5))
		{
			sb.append("\t*"); sb.append(R5);
			for(String r : fields.get(R5))
			{
				sb.append("\t"); sb.append(r);
			}
		}

		if (fields.containsKey(BUNDLE))
		{
			sb.append("\t*"); sb.append(BUNDLE);
			for(String r : fields.get(BUNDLE))
			{
				sb.append("\t"); sb.append(r);
			}
		}

		if (fields.containsKey(MATETHREAD))
		{
			sb.append("\t*"); sb.append(MATETHREAD);
			for(String r : fields.get(MATETHREAD))
			{
				sb.append("\t"); sb.append(r);
			}
		}

		return sb.toString();
	}

	public Node(String nodeid)
	{
		nodeid_m = nodeid;
	}

	public Node()
	{

	}

	/**
	 * Fill in the member variables for this instance using the information in a tab
	 *   delimited string. 
	 *   
	 * The output of mapreduce steps (e.g BuildGraph) is stored in TextFormat. 
	 * This function is used to reconstruct they key & value(Node) from the text
	 * string. The key outputted by the previous MR job is the first tab delimited
	 * field and is stored in the variable nodeid_m. The remaining fields are
	 * parsed using parseNodeMsg.
	 *   
	 * @param nodestr
	 * @throws IOException
	 */
	public void fromNodeMsg(String nodestr) throws IOException
	{	
		fields.clear();

		String [] items = nodestr.split("\t");

		nodeid_m = items[0];
		parseNodeMsg(items, 1);
	}

	/**
	 * Parse an array of strings to extract the relevant field values.
	 * items is interpreted as a set of values encoding a message. The first
	 * item (i.e items[offset]) indicates the type of message encoded and must
	 * correspond to NODEMSG.
	 * 
	 * @param items - Array of strings to be parsed
	 * @param offset - The offset into items. Only items[offset:] are considered
	 *   items[:offset] are ignored. 
	 * @throws IOException
	 */
	public void parseNodeMsg(String[] items, int offset) throws IOException
	{
		if (!items[offset].equals(NODEMSG))
		{
			throw new IOException("Unknown code: " + items[offset]);
		}

		List<String> l = null;

		offset++;

		while (offset < items.length)
		{
			if (items[offset].charAt(0) == '*')
			{
				String type = items[offset].substring(1);
				l = fields.get(type);

				if (l == null)
				{
					l = new ArrayList<String>();
					fields.put(type, l);
				}
			}
			else if (l != null)
			{
				l.add(items[offset]);
			}

			offset++;
		}
	}

	public void fromNodeMsg(String nodestr, Set<String> desired)
	{	
		fields.clear();

		String [] items = nodestr.split("\t");
		List<String> l = null;

		// items[0] == nodeid
		// items[1] == NODEMSG

		for (int i = 2; i < items.length; i++)
		{
			if (items[i].charAt(0) == '*')
			{
				l = null;

				String type = items[i].substring(1);

				if (desired.contains(type))
				{
					l = fields.get(type);

					if (l == null)
					{
						l = new ArrayList<String>();
						fields.put(type, l);
					}
				}
			}
			else if (l != null)
			{
				l.add(items[i]);
			}
		}
	}

	public boolean hasCustom(String key)
	{
		return fields.containsKey(key);
	}

	/**
	 * Set a custom field.  
	 * 
	 * The field is created if it doesn't exist.
	 * If the field exists, the list of values is cleared before adding the
	 * value to the list of values for this field.
	 * 
	 * @param key - The name for the field.
	 * @param value - A value to put in the empty list for this key.
	 */
	public void setCustom(String key, String value)
	{
		List<String> l = getOrAddField(key);
		l.clear();		
		l.add(value);
	}

	/**
	 * Add a field to the node.
	 * 
	 * If the field doesn't exist the node is created. If the
	 * field exists, the value is added to the list of values for this field.
	 * 
	 * @param key - The name for the field.
	 * @param value - The value to add to the list of values for this field. 
	 */
	public void addCustom(String key, String value)
	{
		List<String> l = getOrAddField(key);
		l.add(value);
	}

	public List<String> getCustom(String key)
	{
		return fields.get(key);
	}

	/**
	 * Compute the reverse complement of the DNA sequence. 
	 * 
	 * @param seq
	 * @return
	 */
	public static String rc(String seq)
	{
		StringBuilder sb = new StringBuilder();

		for (int i = seq.length() - 1; i >= 0; i--)
		{
			if      (seq.charAt(i) == 'A') { sb.append('T'); }
			else if (seq.charAt(i) == 'T') { sb.append('A'); }
			else if (seq.charAt(i) == 'C') { sb.append('G'); }
			else if (seq.charAt(i) == 'G') { sb.append('C'); }
		}

		return sb.toString();
	}

	/**
	 * Compare a string to its reverse complement
	 * 
	 * @param seq - The sequence.
	 * @return - 'f' if the DNA sequence precedes is reverse complement
	 * 	lexicographically. 'r' otherwise. 
	 */
	public static char canonicaldir(String seq)
	{
		String rc = rc(seq);
		if (seq.compareTo(rc) < 0)
		{
			return 'f';
		}

		return 'r';
	}

	/**
	 * Returns the canonical version of a DNA sequence.
	 * 
	 * The canonical version of a sequence is the result
	 * of comparing a DNA sequence to its reverse complement
	 * and returning the one which comes first when ordered lexicographically.
	 *  
	 * @param seq
	 * @return - The canonical version of the DNA sequence.
	 */
	public static String canonicalseq(String seq)
	{
		String rc = rc(seq);
		if (seq.compareTo(rc) < 0)
		{
			return seq;
		}

		return rc;
	}


	/**
	 * Return the opposite direction.
	 * 
	 * @param dir
	 * @return
	 * @throws IOException
	 */
	public static String flip_dir(String dir) throws IOException
	{
		if (dir.equals("f")) { return "r"; }
		if (dir.equals("r")) { return "f"; }

		throw new IOException("Unknown dir type: " + dir);
	}

	public static String flip_link(String link) throws IOException
	{
		if (link.equals("ff")) { return "rr"; }
		if (link.equals("fr")) { return "fr"; }
		if (link.equals("rf")) { return "rf"; }
		if (link.equals("rr")) { return "ff"; }

		throw new IOException("Unknown link type: " + link);
	}

	/**
	 * @return The DNA sequence represented by this node as a normal ASCII string. 
	 */
	public String str()
	{
		return dna2str(fields.get(STR).get(0));
	}

	/**
	 * @return The tight encoding of the DNA sequence represented by this node. 
	 */
	public String str_raw()
	{
		return fields.get(STR).get(0);
	}

	/**
	 * Set the field encoding the compressed version of the DNA sequence.
	 * @param rawstr The compressed DNA sequence.
	 */
	public void setstr_raw(String rawstr)
	{
		List<String> l = getOrAddField(STR);
		l.clear();		
		l.add(rawstr);
	}

	public void setstr(String str)
	{
		List<String> l = getOrAddField(STR);
		l.clear();
		l.add(Node.str2dna(str));
	}


	public int len()
	{
		return str().length();
	}

	/**
	 * Compute the degree for this node. 
	 * 
	 * The degree is the number of outgoing edges from this node when the kmer 
	 * sequence represented by this node is read in direction dir.
	 * 
	 * @param dir - The orientation direction for this kmer. Can be "r" or "f".
	 * @return
	 */
	public int degree(String dir)
	{
		int retval = 0;

		String fd = dir + "f";
		if (fields.containsKey(fd)) { retval += fields.get(fd).size(); }

		String rd = dir + "r";
		if (fields.containsKey(rd)) { retval += fields.get(rd).size(); }

		return retval;
	}

	public String edgestr()
	{
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < edgetypes.length; i++)
		{
			if (fields.containsKey(edgetypes[i]))
			{
				String et = edgetypes[i];
				sb.append(et + "(" + fields.get(et).size() + ")");
				for (int j = 0; j < fields.get(et).size(); j++)
				{
					if (sb.length() != 0) { sb.append(" "); }
					sb.append(et + ":" + fields.get(et).get(j));
				}
			}
		}

		return sb.toString();
	}


	public float cov()
	{
		return Float.parseFloat(fields.get(COVERAGE).get(0)); 
	}

	/**
	 * Concatenate two strings that overlap by k-1 characters.
	 *
	 * The last k-1 characters of astr must equal the first k-1 characters of bstr.
	 * The returned string is astr+bstr[K-1:]
	 * @param astr
	 * @param bstr
	 * @param K
	 * @return 
	 * @throws IOException
	 */
	public static String str_concat(String astr, String bstr, int K) throws IOException
	{
		//System.err.println("Concating: " + astr + " " + bstr);
		String as = astr.substring(astr.length()-(K-1));
		String bs = bstr.substring(0, K-1);

		if (!as.equals(bs))
		{
			throw new IOException("as (" + as + ") != bs (" + bs + ")");
		}

		return astr + bstr.substring(K-1);
	}

	public void addreads(Node othernode, int shift)
	{
		if (othernode.fields.containsKey(R5))
		{
			List<String> myr5 = getOrAddField(R5);

			for(String rstr : othernode.fields.get(R5))
			{
				String [] vals = rstr.split(":");
				int offset = Integer.parseInt(vals[1]) + shift;

				myr5.add(vals[0] + ":" + offset);
			}
		}
	}

	public List<String> getreads()
	{
		if (fields.containsKey(R5))
		{
			return fields.get(R5);
		}

		return null;
	}

	public void setreads(Node othernode)
	{
		if (othernode.fields.containsKey(R5))
		{
			fields.put(R5, othernode.fields.get(R5));
		}
		else
		{
			fields.remove(R5);
		}
	}

	public void setR5(Node other)
	{
		if (other.fields.containsKey(R5))
		{
			fields.put(R5, other.fields.get(R5));
		}
		else
		{
			fields.remove(R5);
		}
	}

	/**
	 * Return the tail information for this node.
	 * 
	 * A node is a tail if the degree of the node (# of outgoing edges) is 1.
	 * 
	 * @param dir - The read direction for this kmer for which we consider tail information.
	 * @return - An instance of Tailinfo. ID is set to the compressed kmer representation of the destination node,
	 * 	 dist is initialized to 1, and dir is the direction coresponding to the destination node. 
	 */
	public TailInfo gettail(String dir)
	{
		if (degree(dir) != 1)
		{
			return null;
		}

		TailInfo ti = new TailInfo();
		ti.dist = 1;

		String fd = dir + "f";
		if (fields.containsKey(fd)) 
		{ 
			ti.id = fields.get(fd).get(0);  
			ti.dir = "f";
		}

		fd = dir + "r";
		if (fields.containsKey(fd))
		{
			ti.id = fields.get(fd).get(0);
			ti.dir = "r";
		}

		return ti;
	}

	public static String mate_basename(String readname)
	{
		if (readname.endsWith("_1") || readname.endsWith("_2"))
		{
			return readname.substring(0, readname.length()-2);
		}

		return null;
	}

	public static int mate_insertlen(String read1, String read2, int INSERT_LEN)
	{
		return INSERT_LEN;
	}

	public static int mate_insertstdev(int INSERT_LEN)
	{
		return (int) .1 * INSERT_LEN;
	}


	public static int mate_wiggle(int INSERT_LEN, int MIN_WIGGLE)
	{
		int stdev = mate_insertstdev(INSERT_LEN);
		int retval = (int)((double) stdev / Math.sqrt(10)); // TODO: weight
		if (retval < MIN_WIGGLE)
		{
			retval = MIN_WIGGLE;
		}
		return retval;
	}


	public static String joinstr(String sep, List<String> strs)
	{
		StringBuffer buffer = new StringBuffer();

		boolean first = true;
		for (String str : strs)
		{
			if (!first) { buffer.append(sep); }
			first = false;
			buffer.append(str);
		}

		return buffer.toString();
	}

	public static String joinstr(String sep, String [] strs, int offset)
	{
		StringBuffer buffer = new StringBuffer();

		boolean first = true;
		while (offset < strs.length)
		{
			if (!first) { buffer.append(sep); }
			first = false;
			buffer.append(strs[offset]);
			offset++;
		}

		return buffer.toString();
	}

	public void printlinks()
	{
		for(String et : Node.edgetypes)
		{
			List<String> edges;
			try {
				edges = getEdges(et);
				if (edges != null)
				{
					for (String v : edges)
					{
						System.err.println(et + " " + v);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}


	protected Iterator<String> FieldsIterator(){
		return this.fields.keySet().iterator();
	}


	protected boolean hasField (String field){
		return fields.containsKey(field);
	}

	public static void main(String[] args) throws Exception 
	{


	}

	protected Iterator<?> fieldsIterator(){
		return fields.keySet().iterator();
	}

	// Iterator over the list of values associated with a field.
	protected Iterator<?> fieldValuesIterator(String field) {
		return fields.get(field).iterator();
	}
}
