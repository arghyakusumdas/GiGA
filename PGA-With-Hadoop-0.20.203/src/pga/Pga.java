package pga;

// Jar construction: Use the farjar exporter to build a runnable jar.
// Set the main class to pga/Pga
// It is not necessary to package any external libraries into the jar



import java.io.*;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.TTCCLayout;
import org.apache.log4j.helpers.DateLayout;



public class Pga extends Configured implements Tool
{
	public static String VERSION = "0.8.2";
	
	private static DecimalFormat df = new DecimalFormat("0.00");
	private static FileOutputStream logfile;
	private static PrintStream logstream;
	
	JobConf baseconf = new JobConf(Pga.class);
	
    static String preprocess   = "00-preprocess";
	static String initial      = "01-initial";
	static String initialcmp   = "02-initialcmp";
	static String notips       = "03-notips";
	static String notipscmp    = "04-notipscmp";
	static String nobubbles    = "05-nobubbles";
	static String nobubblescmp = "06-nobubblescmp";
	static String lowcov       = "07-lowcov";
	static String lowcovcmp    = "08-lowcovcmp";
	static String repeats      = "09-repeats";
	static String repeatscmp   = "10-repeatscmp";
	static String scaff        = "11-scaffold";
	static String finalcmp     = "99-final";


	// Message Management
	///////////////////////////////////////////////////////////////////////////	
	
	long GLOBALNUMSTEPS = 0;
	long JOBSTARTTIME = 0;
	public void start(String desc)
	{
		msg(desc + ":\t");
		JOBSTARTTIME = System.currentTimeMillis();
		GLOBALNUMSTEPS++;
	}
	
	public void end(RunningJob job) throws IOException
	{
		long endtime = System.currentTimeMillis();
		long diff = (endtime - JOBSTARTTIME) / 1000;
		
		msg(job.getJobID() + " " + diff + " s");
		
		if (!job.isSuccessful())
		{
			System.out.println("Job was not successful");
			System.exit(1);
		}
	}
	
	public void end(String desc) throws IOException
	{
		long endtime = System.currentTimeMillis();
		long diff = (endtime - JOBSTARTTIME) / 1000;
		
		msg(desc + " " + diff + " s");
		
		
	}

	
	public static void msg(String msg)
	{
		logstream.print(msg);
		System.out.print(msg);
	}
	
	public long counter(RunningJob job, String tag) throws IOException
	{
		return job.getCounters().findCounter("Pga", tag).getValue();
	}


	
	// Stage Management
	///////////////////////////////////////////////////////////////////////////	

	boolean RUNSTAGE = false;
	private String CURRENTSTAGE;
	
	public boolean runStage(String stage)
	{
		CURRENTSTAGE = stage;
		
		if (PgaConfig.STARTSTAGE == null || PgaConfig.STARTSTAGE.equals(stage))
		{
			RUNSTAGE = true;
		}
		
		return RUNSTAGE;
	}
	
	public void checkDone()
	{
		if (PgaConfig.STOPSTAGE != null && PgaConfig.STOPSTAGE.equals(CURRENTSTAGE))
		{
			RUNSTAGE = false;
			msg("Stopping after " + PgaConfig.STOPSTAGE + "\n");
			System.exit(0);
		}
	}

	
	// File Management
	///////////////////////////////////////////////////////////////////////////	
	
	public void cleanup(String path) throws IOException
	{
		FileSystem.get(baseconf).delete(new Path(path), true);
	}
	
	public void save_result(String base, String opath, String npath) throws IOException
	{
		//System.err.println("Renaming " + base + opath + " to " + base + npath);
		
		msg("Save result to " + npath + "\n\n");
		
		FileSystem.get(baseconf).delete(new Path(base+npath), true);
		FileSystem.get(baseconf).rename(new Path(base+opath), new Path(base+npath));
	}
	
	
	// Compute Graph Statistics
	///////////////////////////////////////////////////////////////////////////	
	
	public void computeStats(String base, String dir) throws Exception
	{
		start("Compute Stats " + dir);
		Stats stats = new Stats();
		RunningJob job = stats.run(base+dir, base+dir+".stats");
		end(job);
		
		msg("\n\nStats " + dir + "\n");
		msg("==================================================================================\n");
		
		FSDataInputStream statstream = FileSystem.get(baseconf).open(new Path(base+dir+".stats/part-00000"));
		BufferedReader b = new BufferedReader(new InputStreamReader(statstream));

		String s;
		while ((s = b.readLine()) != null)
		{
			msg(s);
			msg("\n");
		}
		
		msg("\n");
	}
	
	
	// convertFasta
	///////////////////////////////////////////////////////////////////////////	
	
	public void convertFasta(String basePath, String graphdir, String fastadir) throws Exception
	{
		start("convertFasta " + graphdir);
		Graph2Fasta g2f = new Graph2Fasta();
		RunningJob job = g2f.run(basePath + graphdir, basePath + fastadir);
		end(job);
		
		long nodes = counter(job, "printednodes");
		msg ("  " + nodes + " converted (len >= " + PgaConfig.FASTA_MIN_LEN + " cov >= " + PgaConfig.FASTA_MIN_COV + ")\n");
	}
	
	
	// preprocess
	///////////////////////////////////////////////////////////////////////////	
	
	public void preprocess(String readpath, String basePath, String preprocess) throws Exception
	{
		start("Preprocess " + readpath);
		FastqPreprocessor fqp = new FastqPreprocessor();
		RunningJob job = fqp.run(readpath, basePath + preprocess);
		end(job);
		
		long reads = counter(job, "preprocessed_reads");
		
		if (PgaConfig.PREPROCESS_SUFFIX == 1)
		{
			long pair_unpaired = counter(job, "pair_unpaired");
			long pair_1 = counter(job, "pair_1");
			long pair_2 = counter(job, "pair_2");

			msg ("  " + reads + " converted. unpaired: " + pair_unpaired + " pair_1: " + pair_1 + " pair_2: " + pair_2 + "\n");
		}
		else
		{
			msg ("  " + reads + " converted\n");
		}
	}
	
	
	// Build initial graph
	///////////////////////////////////////////////////////////////////////////	
	
	public void buildInitial(String basePath, String preprocessed, String initial, String initialcmp) throws Exception
	{
		RunningJob job;
		
		/*Argo Start: Comment Build Graph
		if (PgaConfig.RESTART_INITIAL == 0)
		{
			start("Build Initial");
			
			//Commented below code to start from the compression phase directly.
			//This is only for the testing purpose when the disk space is small compared to the total graph size
			
			BuildGraph bg = new BuildGraph();
			job = bg.run(basePath + preprocessed, basePath + initial);
			end(job);

			long nodecnt      = counter(job, "nodecount");
			long reads_goodbp = counter(job, "reads_goodbp");
			long reads_good   = counter(job, "reads_good");
			long reads_short  = counter(job, "reads_short");
			long reads_skip   = counter(job, "reads_skipped");

			long reads_all = reads_good + reads_short + reads_skip;

			if (reads_good == 0)
			{
				//PSK - commented below statement 
				//throw new IOException("No good reads"); //Argo: Commented this line
			}

			String frac_reads = df.format(100*reads_good/reads_all);
			msg("  " + nodecnt + " nodes [" + reads_good +" (" + frac_reads + "%) good reads, " + reads_goodbp + " bp]\n");
			
		}
		else
		{
			msg("Skipping initial build\n");
		}
		Date date_GB_end = new Date(); //Argo: added
		msg("----------Category GraphBuild ends at:" + date_GB_end.toString() + "----------\n"); //Argo: added
		//Argo End: Comment Build graph*/
		
		//Argo Start: Comment Post Build Graph
		Date date_EC_start = new Date(); //Argo: added
		msg("----------Category ErrorCorrection starts at:" + date_EC_start.toString() + "----------\n"); //Argo: added
		// Quick merge
		//PSK -commennted quick merge since it is taken care in pregel based merge
		
		//start("  Quick Merge");
		QuickMerge qmerge = new QuickMerge();
		job = qmerge.run(basePath + initial, basePath + initial + ".m");
		end(job);

		msg("  " + counter(job, "saved") + " saved\n");	
		
		//PSK -End
				
		compressChains(basePath, initial + ".m", initialcmp);	
		
		
		//Testing the code
		//compressChains(basePath, initialcmp,initial+".totips1");
		//compressChains(basePath,initial+".totips1", initial+".totips2");
		//compressChains(basePath,initial+".totips2", initial+".totips3");
		//compressChains(basePath,initial+".totips3", initial+".totips4");
		 
		 		//PSK
		//Implementation of assembly workflow starts here
		int stage=1;
		int cmplCompress = 1;
		//File reader = new File("exit.txt");
		String input = initial  +".m";  // making sure that we get the input from previous step
		//PSK add .m for quick merge
		String output = initialcmp + stage;
		while(cmplCompress > 0)
		{
			compressChains(basePath, input,output);
			
			//File reader = new File("exit.txt");
			//File reader = new File("/home/pkondi1/exit.txt");
			File reader = new File(PgaConfig.trackSSPath.toString()+"/exit.txt");
			if (reader.exists())
			{
				BufferedReader br = new BufferedReader(new FileReader(reader));
				String val = br.readLine();
				val = val.trim();			
				br.close();
				reader.delete();
				cmplCompress = Integer.parseInt(val);
				System.out.print(cmplCompress);
				stage = stage+1;
				input=output;
				output=output+stage;
			}
			else
			{
				cmplCompress =0;
			}
			
		}
		//At the end we copy the output to input for the next phase
		save_result(basePath, input, initialcmp);
		
		//Argo End: Comment Post Build Graph*/
	}	
	// Maximally compress chains
	///////////////////////////////////////////////////////////////////////////	
	public void compressChains(String basePath, String startname, String finalname) throws Exception
	{
		Compressible comp = new Compressible();
		
		QuickMark qmark   = new QuickMark();
		QuickMerge qmerge = new QuickMerge();

		PairMark pmark   = new PairMark();
		PairMerge pmerge = new PairMerge();
		
		int stage = 0;
		long compressible = 0;
		//Test PSK
		compressible =1;
		RunningJob job = null;
		
		if (PgaConfig.RESTART_COMPRESS > 0)
		{
			stage = PgaConfig.RESTART_COMPRESS;
			compressible = PgaConfig.RESTART_COMPRESS_REMAIN;
			
			msg("  Restarting compression after stage " + stage + ":");
			
			PgaConfig.RESTART_COMPRESS = 0;
			PgaConfig.RESTART_COMPRESS_REMAIN = 0;
		}
		else
		{
			// Mark compressible nodes
			start("  Compressible");
			
			job = comp.run(basePath+startname, basePath+startname+"."+stage);
			compressible = counter(job, "compressible");
			end(job);
			
		}

		msg("  " + compressible + " compressible\n");

		long lastremaining = compressible;
		if (!PgaConfig.USEPREGELMERGE) {
			msg("start pairMark and PairMerge");
			long startTime = System.currentTimeMillis();
			while (lastremaining > 0) {
				int prev = stage;
				stage++;

				String input = basePath + startname + "."
						+ Integer.toString(prev);
				String input0 = input + ".0";
				String output = basePath + startname + "."
						+ Integer.toString(stage);

				long remaining = 0;

				if (lastremaining < PgaConfig.HADOOP_LOCALNODES) {
					// Send all the compressible nodes to the same machine for
					// serial processing
					start("  QMark " + stage);
					job = qmark.run(input, input0);
					end(job);

					msg("  " + counter(job, "compressibleneighborhood")
							+ " marked\n");

					start("  QMerge " + stage);
					job = qmerge.run(input0, output);
					end(job);

					remaining = counter(job, "needcompress");
				} else {
					// Use the randomized algorithm
					double rand = Math.random();

					start("  Mark " + stage);
					job = pmark.run(input, input0, (int) (rand * 10000000));
					end(job);

					msg("  " + counter(job, "mergestomake") + " marked\n");

					start("  Merge " + stage);
					job = pmerge.run(input0, output);
					end(job);

					remaining = counter(job, "needscompress");
				}

				// cleanup(input);
				// cleanup(input0);

				String percchange = df.format((lastremaining > 0) ? 100
						* (remaining - lastremaining) / lastremaining : 0);
				msg("  " + remaining + " remaining (" + percchange + "%)\n");

				lastremaining = remaining;
			}
			long endTime = System.currentTimeMillis();
			long secs= (startTime- endTime)/ 1000;
			msg("End of pairMark and PairMerge	" + secs+"s");
		}
		else if(lastremaining > 0)
		{
			long startTime = System.currentTimeMillis(); //Argo: added
			start("  start merge on giraph pregel " );
			String input = basePath + startname + "." + Integer.toString(stage);
			stage++;
			String output = basePath + startname + "."
					+ Integer.toString(stage);
			String compress=""+lastremaining;
			String args[] = { input, output,""+PgaConfig.NOWORKERS };
			//ToolRunner.run(new GraphMerge(), args);
			new GraphMerge().run(args);
			end("end merge on giraph pregel");
			long endTime = System.currentTimeMillis(); //Argo: added
			//new CompleteMerge().run(args);
			long secs= (startTime- endTime)/ 1000; //Argo: added
			msg("End of GiraphGraphMerge	" + secs+"s");
		}

		save_result(basePath, startname + "." + stage, finalname);
	}
	
	
	// Maximally remove tips
	///////////////////////////////////////////////////////////////////////////	
	
	public void removeTips(String basePath, String current, String prefix, String finalname) throws Exception
	{
		RemoveTips tips = new RemoveTips();
		
		int round = 0;
		long remaining = 1;
		
		if (PgaConfig.RESTART_TIP > 0)
		{
			round = PgaConfig.RESTART_TIP;
			PgaConfig.RESTART_TIP = 0;
		}

		while (remaining > 0)
		{
			round++;

			String output = prefix + "." + round;
			long removed = 0;
			
			if (PgaConfig.RESTART_TIP_REMAIN > 0)
			{
				remaining = PgaConfig.RESTART_TIP_REMAIN;
				PgaConfig.RESTART_TIP_REMAIN = 0;
				msg("Restart remove tips " + round + ":");
				removed = 123456789;
			}
			else if(!PgaConfig.USEPREGELMERGE)
			{
				start("Remove Tips " + round);
				RunningJob job = tips.run(basePath+current, basePath+output);
				end(job);

				removed = counter(job, "tips_found");
				remaining = counter(job, "tips_kept");
			

			msg("  " + removed + " tips found, " + remaining + " remaining\n");

			if (removed > 0)
			{
				if (round > 1) { cleanup(current); }

				current = output + ".cmp";
				compressChains(basePath, output, current);
				remaining = 1;
			}
			
		}
		    else
		     {
			start("start tip removal on giraph  " );
			
			String args[] = { basePath+current, basePath+output, ""+PgaConfig.NOWORKERS };
			
			new TipMerge().run(args);
			
				
			//removed =  tfound;//GiraphRemoveTips.GiraphRemoveTipsMapper.getTipsFound();
			//remaining = tkept;//GiraphRemoveTips.GiraphRemoveTipsReducer.getTipsKept();
			//PSK start
			//File input = new File("/home/pkondi1/counts_tip.txt");
			//File input = new File("counts_tip.txt");
			File input = new File(PgaConfig.localBasePath+"/counts_tip.txt");
			if(input.exists()) {
			BufferedReader in = new BufferedReader(new FileReader(input));
			String str = in.readLine();
			in.close();
			String[] nos = str.split(" ");
			removed = Integer.parseInt(nos[0]) ; //+Integer.parseInt(nos[0]);
			remaining = Integer.parseInt(nos[1]) ; //+Integer.parseInt(nos[1]);
			input.delete();
			}
			else
			{
			   removed = 0;
			   remaining =0;
			}
			msg("  " + removed + " tips found, " + remaining + " remaining\n");
			
			end("end tip removal on giraph ");
						
			if (removed > 0)
			{
				if (round > 1) { cleanup(current); }

				current = output + ".cmp";
				compressChains(basePath, output, current);
				remaining = 1;
			}
		}

			cleanup(output);
		}

		save_result(basePath, current, finalname);
		msg("\n");
	}
	
	
	// Maximally pop bubbles
	///////////////////////////////////////////////////////////////////////////	
	
	public long popallbubbles(String basePath, String basename, String intermediate, String finalname) throws Exception
	{
		long allpopped = 0;
		long popped    = 1;
		int round      = 1;
		
		FindBubbles finder = new FindBubbles();
		PopBubbles  popper = new PopBubbles();

		while (popped > 0)
		{
			
			String findname = intermediate + "." + round + ".f";
			String popname  = intermediate + "." + round;
			String cmpname  = intermediate + "." + round + ".cmp";
			if(!PgaConfig.USEPREGELMERGE)
			{
			start("Find Bubbles " + round);
			RunningJob job = finder.run(basePath+basename, basePath+findname);
			end(job);

			long potential = counter(job, "potentialbubbles");
			msg("  " + potential + " potential bubbles\n");
			
			start("  Pop " + round);
			job = popper.run(basePath+findname, basePath+popname);
			end(job);

			popped = counter(job, "bubblespopped");
			msg("  " + popped + " bubbles popped\n");

			cleanup(findname);

			if (popped > 0)
			{
				if (round > 1)
				{
					cleanup(basename);
				}

				compressChains(basePath, popname, cmpname);

				basename = cmpname;
				allpopped += popped;
				round++;
			}

			cleanup(popname);
			}
			else {
				start("start pop bubbles on giraph " );
				
				String args[] = { basePath+basename, basePath+popname,""+PgaConfig.NOWORKERS };
				
				new BubbleMerge().run(args);
				
				//long potential = pga.BubbleMaster.potbubb;
				
				long potential =0;
                popped =0;
                //File input = new File("/home/pkondi1/counts_bubb.txt");
                File input = new File(PgaConfig.localBasePath+"/counts.bubb.txt");
				//File input = new File("counts_bubb.txt");
				if(input.exists()){
					
					BufferedReader in = new BufferedReader(new FileReader(input));
					String str = in.readLine();
					in.close();
					String[] nos = str.split(" ");
					popped = Integer.parseInt(nos[0]) ; //+Integer.parseInt(nos[0]);
					potential = Integer.parseInt(nos[1]) ; //+Integer.parseInt(nos[1]);
					input.delete();
				}
				
				msg("  " + potential + " potential bubbles\n");
				
				//popped = pga.BubbleMaster.bubb;
				msg("  " + popped + " bubbles popped\n");
				
				end("end pop bubbels on giraph ");
				if (popped > 0)
				{
					if (round > 1)
					{
						cleanup(basename);
					}

					compressChains(basePath, popname, cmpname);

					basename = cmpname;
					allpopped += popped;
					round++;
				}

				cleanup(popname);
			}
		}    //End of while

		// Copy the basename to the final name
		save_result(basePath, basename, finalname);
		msg("\n");

		return allpopped;
	}



	// Maximally remove low coverage nodes & compress
	///////////////////////////////////////////////////////////////////////////	
	
	public void removelowcov(String basePath, String nobubblescmp, String lowcov, String lowcovcmp) throws Exception
	{
		RemoveLowCoverage remlowcov = new RemoveLowCoverage();
		
		start("Remove Low Coverage");
		RunningJob job = remlowcov.run(basePath+nobubblescmp, basePath+lowcov);
		end(job);
		
		long lcremoved = counter(job, "lowcovremoved");
		msg("  " + lcremoved +" low coverage nodes removed\n");
		
		if (lcremoved > 0)
		{
			//After the removal of low coverage we need to rerun the entire error correction phase
			//compressChains(basePath, lowcov, lowcov+".c");
			//removeTips(basePath, lowcov+".c", lowcov+".t", lowcov+".tc");
			//popallbubbles(basePath, lowcov+".tc", lowcov+".b", lowcovcmp);
			int stage = 1;
			int lowcovCmpl = 1;
			String input = lowcov;
			String output = lowcov+stage;
			while(lowcovCmpl > 0)
			{
				compressChains(basePath, input,output);
				
				//File reader = new File("exit.txt");
				//File reader = new File("/home/pkondi1/exit.txt");
				File reader = new File(PgaConfig.trackSSPath.toString()+"/exit.txt");
				if (reader.exists())
				{
					BufferedReader br = new BufferedReader(new FileReader(reader));
					String val = br.readLine();
					val = val.trim();			
					br.close();
					reader.delete();
					lowcovCmpl = Integer.parseInt(val);
					System.out.print(lowcovCmpl);
					stage = stage+1;
					input=output;
					output=output+stage;
				}
				else
				{
					lowcovCmpl =0;
				}
				
			}
			//Error correction phase after removing low coverage nodes is done
			//Copy the output as input to next phase i.e scaffolding
			save_result(basePath, input, lowcovcmp);				
		}
		else
		{
			save_result(basePath, nobubblescmp, lowcovcmp);
		}
	}

	
	
	// Resolve Repeats / Scaffolding
	///////////////////////////////////////////////////////////////////////////	
	
	public void resolveRepeats(String basePath, String current, String prefix, String finalname, boolean scaffold) throws Exception
	{		
		long threadiblecnt = 1;
		int phase = 1;
		
		// short repeats
		ThreadRepeats repeatthreader = new ThreadRepeats();
		Threadible threadibler = new Threadible();
		ThreadResolve threadresolver = new ThreadResolve();
		
		// mate resolved repeats
		MateDist matedist = new MateDist();
		MateBundle matebundle = new MateBundle();
		MateHop matehop = new MateHop();
		MateHopFinalize matehopfinalize = new MateHopFinalize();
		MateFinalize matefinalize = new MateFinalize();
		MateCleanLinks matecleanlinks = new MateCleanLinks();

		UnrollTandem unroller = new UnrollTandem(); 

		RunningJob job;
		
		if (PgaConfig.RESTART_SCAFF_PHASE > 1)
		{
			phase = PgaConfig.RESTART_SCAFF_PHASE-1;
			current = prefix + "." + phase + ".popfin";
			phase++;
		}

		while (threadiblecnt > 0)
		{
			if (scaffold)
			{
				msg("Scaffolding phase PSK" + phase + "\n");

				String edgefile   = prefix + "." + phase + ".edges";
				String bundlefile = prefix + "." + phase + ".bundles";
				String matepath   = prefix + "." + phase + ".matepath";
				String finalpath  = prefix + "." + phase + ".final";
				String scaffpath  = prefix + "." + phase + ".scaff";
				
				long allctg = 1;
				long uniquectg = 1;
				
				if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
					PgaConfig.RESTART_SCAFF_STAGE.equals("edges"))
				{
					PgaConfig.RESTART_SCAFF_STAGE = null;
					
					// Find Mates
					start("  edges");
					job = matedist.run(basePath + current, basePath + edgefile);
					end(job);

					allctg          = counter(job, "nodes");
					uniquectg       = counter(job, "unique_ctg");

					long linking         = counter(job, "linking_edges");
					long internaldist    = counter(job, "internal_dist");
					//long internaldistsq  = counter(job, "internal_distsq");
					long internalcnt     = counter(job, "internal_mates");
					long internalinvalid = counter(job, "internal_invalid");
					
					float internalavg = internalcnt > 0 ? (float)internaldist/(float)internalcnt : 0.0f;
					//double variance    = internalcnt > 0 ?  (internaldistsq - (internaldist*internaldist)/internalcnt) : 0.0;
					//double internalstd = Math.sqrt(Math.abs(variance));

					msg("  " + linking + " linking edges, " + internalcnt + " internal " + 
						internalavg + " avg, " + internalinvalid + " invalid\n");
				}
				
				if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
					PgaConfig.RESTART_SCAFF_STAGE.equals("bundles"))
				{
					PgaConfig.RESTART_SCAFF_STAGE = null;
					
					// Bundle mates
					start("  bundles");
					job = matebundle.run(basePath + current, basePath + edgefile, basePath + bundlefile);
					end(job);

					long ubundles = counter(job, "unique_bundles");

					msg ("  " + ubundles + " U-bundles " + uniquectg + " unique / " + allctg + " all contigs\n");
				}
				
				String curgraph = "";

				if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
					PgaConfig.RESTART_SCAFF_STAGE.equals("frontier"))
				{
					PgaConfig.RESTART_SCAFF_STAGE = null;
				
					threadiblecnt = 0;

					// Perform frontier search for mate-consistent paths
					long active = 1; // assert there are active mate-threads to consider
					long stage  = PgaConfig.RESTART_SCAFF_FRONTIER;
					PgaConfig.RESTART_SCAFF_FRONTIER = 0;
					
					curgraph = bundlefile;
					
					if (stage > 0)
					{
						msg(" Restarting frontier search after stage: " + stage + "\n");
						curgraph = prefix + "." + phase + ".search" + stage;
					}
					
					boolean pregel=true;
					if (pregel){
						String prevgraph = curgraph;
						curgraph = prefix + "." + phase + ".search" ;
						String girInput = basePath + prevgraph ;
						String girOutput = basePath + curgraph;
						long girInsertLen = PgaConfig.INSERT_LEN;
						long girminWiggle = PgaConfig.MIN_WIGGLE;
						long KmerLen = PgaConfig.K;
						String[] girahpArgs={girInput,girOutput,""+girInsertLen,""+girminWiggle,""+PgaConfig.K,""+PgaConfig.NOWORKERS};
						msg("Start of Giraph Scaffolding \n");
						long startTime = System.currentTimeMillis();
						new GiraphScaffolder().run(girahpArgs);
						long endTime = System.currentTimeMillis();
						msg("No of Supersteps =15 \n");
						msg("Graph Surf for Scaffolding: "+ ((endTime-startTime)/1000) + "s \n" );
						msg("End of Giraph Scaffolding\n");
					}
					else
					{ 
					while ((active > 0) && (stage < PgaConfig.MAX_FRONTIER))
					{
						stage++;

						String prevgraph = curgraph;
						curgraph = prefix + "." + phase + ".search" + stage;

						start("  search " + stage);
						//PSK - Place where Giraph mate hop needs to be included.
						job = matehop.run(basePath + prevgraph, basePath + curgraph, stage==1);
						
						end(job);						
 
						long shortcnt = counter(job, "foundshort");
						long longcnt  = counter(job, "foundlong");
						long invalid  = counter(job, "foundinvalid");
						long valid    = counter(job, "foundvalid");
						long toolong  = counter(job, "toolong");
						active        = counter(job, "active");

						msg(" active: "  + active + " toolong: " + toolong +
							" | valid: " + valid + " short: "   + shortcnt + " long: " + longcnt + 
							" invalid: " + invalid + "\n");
							
					}
				}
					
				}
					
				if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
					PgaConfig.RESTART_SCAFF_STAGE.equals("update"))
				{
					if (PgaConfig.RESTART_SCAFF_STAGE != null)
					{
						PgaConfig.RESTART_SCAFF_STAGE = null;
						long stage = PgaConfig.MAX_FRONTIER;
						
						if (PgaConfig.RESTART_SCAFF_FRONTIER > 0)
						{
							stage = PgaConfig.RESTART_SCAFF_FRONTIER;
							PgaConfig.RESTART_SCAFF_FRONTIER = 0;
						}
							
						curgraph = prefix + "." + phase + ".search" + stage;
					}
				
					start("  update");
					job = matehopfinalize.run(basePath + curgraph, basePath + matepath);
					end(job);
				
					long bresolved = counter(job, "resolved_bundles");
					long eresolved = counter(job, "resolved_edges");
					long ambig     = counter(job, "total_ambiguous");

					msg(" " + bresolved + " bundles resolved, " + eresolved + " edges, " + ambig + " ambiguous\n");
				}
				
				long updates = 1;
				long deadedges = 1;
				
				if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
					PgaConfig.RESTART_SCAFF_STAGE.equals("finalize"))
				{
					PgaConfig.RESTART_SCAFF_STAGE = null;

					// Record path
					start("  finalize");
					job = matefinalize.run(basePath + matepath, basePath + finalpath);
					end(job);

					updates = counter(job, "updates");
					msg("  " + updates + " nodes resolved\n");
				}
				
				if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
					PgaConfig.RESTART_SCAFF_STAGE.equals("clean"))
				{
					PgaConfig.RESTART_SCAFF_STAGE = null;

					// Clean bogus links from unique nodes
					start("  clean");
					job = matecleanlinks.run(basePath + finalpath, basePath + scaffpath);
					end(job);

					deadedges = counter(job, "removed_edges");
					msg("  " + deadedges + " edges removed\n");
				}

				threadiblecnt = updates + deadedges;
				current = scaffpath;
			}
			else
			{
				String output = prefix + "." + phase + ".threads";
				
				if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
					 PgaConfig.RESTART_SCAFF_STAGE.equals("threadrepeats"))
				{
					PgaConfig.RESTART_SCAFF_STAGE = null;

					// Find threadible nodes
					start("Thread Repeats " + phase);
					job = repeatthreader.run(basePath+current, basePath+output);
					end(job);

					threadiblecnt  = counter(job, "threadible");
					long xcut      = counter(job, "xcut");
					long half      = counter(job, "halfdecision");
					long deadend   = counter(job, "deadend");
					msg("  " + threadiblecnt +" threadible (" + xcut + " xcut, " + half + " half, " + deadend + " deadend)\n");
				}

				current = output;
			}

			if (threadiblecnt > 0)
			{
				// Mark threadible neighbors
				String threadible = prefix + "." + phase + ".threadible";
				String resolved = prefix + "." + phase + ".resolved";

				if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
						 PgaConfig.RESTART_SCAFF_STAGE.equals("threadible"))
				{
					PgaConfig.RESTART_SCAFF_STAGE = null;
				
					start("  Threadible " + phase);
					job = threadibler.run(basePath+current, basePath+threadible);
					end(job);
					threadiblecnt = counter(job, "threadible");
					msg("  " + threadiblecnt + " threaded nodes\n");
				}
				
				long remaining = -1;
				
				if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
						 PgaConfig.RESTART_SCAFF_STAGE.equals("resolve"))
				{
					PgaConfig.RESTART_SCAFF_STAGE = null;

					// Resolve a subset of threadible nodes
					start("  Resolve " + phase);
					job = threadresolver.run(basePath + threadible, basePath+resolved);
					end(job);
				
					remaining = counter(job, "needsplit");
					msg("  " + remaining + " remaining\n");
				}
				
				if (remaining == threadiblecnt)
				{
					msg("  Didn't thread any node, giving up\n");
					threadiblecnt = 0;
				}
				else
				{
					if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
							 PgaConfig.RESTART_SCAFF_STAGE.equals("compress"))
					{
						PgaConfig.RESTART_SCAFF_STAGE = null;
						
						//PSK
						//Write the same logic when we exit from tip removal phase.
						int scaffoldStage =1;
						int needCompress =1;
						String input = resolved;
						String output = input+scaffoldStage;
						while(needCompress > 0)
						{
							compressChains(basePath, input, output);
							//File reader = new File("exit.txt");
							File reader = new File(PgaConfig.trackSSPath.toString()+"/exit.txt");
							//File reader = new File("/home/pkondi1/exit.txt");
							if (reader.exists())
							{
								BufferedReader br = new BufferedReader(new FileReader(reader));
								String val = br.readLine();
								val = val.trim();			
								br.close();
								reader.delete();
								needCompress = Integer.parseInt(val);
								System.out.print(needCompress);
								scaffoldStage = scaffoldStage+1;
								input=output;
								output=output+scaffoldStage;
							}
							else
							{
								needCompress =0;
							}
						}
						current = input ;
						
						//compressChains(basePath, resolved, prefix + "." + phase + ".cmp");
					}
					//PSK - Below lines of code is commended as compressChains would handle all the phases
					/*
					if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
							 PgaConfig.RESTART_SCAFF_STAGE.equals("removetips"))
					{						
						PgaConfig.RESTART_SCAFF_STAGE = null;
						removeTips(basePath,	
							prefix + "." + phase + ".cmp", 
							prefix + "." + phase + ".tips",
							prefix + "." + phase + ".tipsfin");
					}

					if ((PgaConfig.RESTART_SCAFF_STAGE == null) ||
							 PgaConfig.RESTART_SCAFF_STAGE.equals("popbubbles"))
					{
						PgaConfig.RESTART_SCAFF_STAGE = null;
						popallbubbles(basePath,
							prefix + "." + phase + ".tipsfin", 
							prefix + "." + phase + ".pop", 
							prefix + "." + phase + ".popfin");
					}
					*/
					
					//Set the current path as the output of the previous path
					//current = prefix + "." + phase + ".popfin";
					
					computeStats(basePath, current);

					phase++;
					msg("\n");
				}
			}
		}

		
		// Unroll simple tandem repeats
		boolean UNROLL_TANDEM = false;
		
		if (scaffold && UNROLL_TANDEM)
		{
			msg("\n\n");

			String output = prefix + ".unroll";

			start("Unroll tandems");
			job = unroller.run(basePath + current, basePath + output);
			end(job);

			long unrolled = counter(job, "simpletandem");
			long tandem   = counter(job, "tandem");
			msg("  " + unrolled + " unrolled (" + tandem + " total)\n");

			current = output + ".cmp";
			compressChains(basePath, output, current);
		}

		
		// The current phase did nothing, so save away current
		save_result(basePath, current, finalname);
		msg("\n");
	}
	
	

	
	// Run an entire assembly
	///////////////////////////////////////////////////////////////////////////	

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception 
	{  
	    // A few preconfigured datasets

		String dataset = "";
		//dataset = "arbrcrd";
		//dataset = "Ba10k";
		//dataset = "Ba100k";
		//dataset = "Ec500k";
		//dataset = "Ec500k.cor.21";
		//dataset = "Ec500k.21";
		//dataset = "10hop";
		//dataset = "15hop";
		//dataset = "20359.prb";
		//dataset = "202.dad";
		//dataset = "202.prb";
		
		//dataset = "pga-small";
		
		if (dataset.equals("pga-small"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/pga-bio/data/pga-small";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/pga-small";
		
			PgaConfig.K = 55;
			PgaConfig.LOW_COV_THRESH = 3.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;

		}
		else if (dataset.equals("202.prb"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/202.prb.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/202.prb/";
		
			PgaConfig.K = 21;
			PgaConfig.LOW_COV_THRESH = 3.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;
		}
		else if (dataset.equals("202.dad"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/202.dad.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/202.dad/";
		
			PgaConfig.K = 21;
			PgaConfig.LOW_COV_THRESH = 3.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;
		}
		else if (dataset.equals("20359.prb"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/20359.prb.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/20359.prb/";
		
			PgaConfig.K = 21;
			PgaConfig.LOW_COV_THRESH = 3.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;
		}
		else if (dataset.equals("non"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/nonoverlap.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/non/";
		
			PgaConfig.K = 25;
			PgaConfig.LOW_COV_THRESH = 0.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 1;
			
			PgaConfig.INSERT_LEN = 210;
			PgaConfig.MIN_UNIQUE_COV = 10;
			PgaConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ba10k"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/schatzlab-public/pga-bio/data/Ba10k";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/Ba10k/";
			
			PgaConfig.PREPROCESS_SUFFIX = 1;
			
			PgaConfig.K = 21;
			PgaConfig.LOW_COV_THRESH = 5.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;

			PgaConfig.INSERT_LEN = 210;
			PgaConfig.MIN_CTG_LEN = PgaConfig.K;
			PgaConfig.MIN_UNIQUE_COV = 10;
			PgaConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ba100k"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/Ba100k.sim.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/Ba100k/";
			//PgaConfig.STARTSTAGE = "scaffolding";
			
			PgaConfig.K = 21;
			PgaConfig.LOW_COV_THRESH = 5.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;

			PgaConfig.INSERT_LEN = 210;
			PgaConfig.MIN_CTG_LEN = PgaConfig.K;
			PgaConfig.MIN_UNIQUE_COV = 10;
			PgaConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec10k"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/Ec10k.sim.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/Ec10k/";
			
			PgaConfig.K = 21;
			PgaConfig.LOW_COV_THRESH = 5.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;

			PgaConfig.INSERT_LEN = 210;
			PgaConfig.MIN_CTG_LEN = PgaConfig.K;
			PgaConfig.MIN_UNIQUE_COV = 10;
			PgaConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec100k"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/Ec100k.sim.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/Ec100k/";
			PgaConfig.STARTSTAGE = "scaffolding";
			
			PgaConfig.K = 21;
			PgaConfig.LOW_COV_THRESH = 5.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;

			PgaConfig.INSERT_LEN = 210;
			PgaConfig.MIN_CTG_LEN = PgaConfig.K;
			PgaConfig.MIN_UNIQUE_COV = 10;
			PgaConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec200k"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/Ec200k.sim.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/Ec200k/";
		    PgaConfig.STARTSTAGE = "scaffolding";
			
		    PgaConfig.K = 21;
		    PgaConfig.LOW_COV_THRESH = 5.0f;
		    PgaConfig.MAX_LOW_COV_LEN = 50;
		    PgaConfig.MIN_THREAD_WEIGHT = 5;

		    PgaConfig.INSERT_LEN = 210;
		    PgaConfig.MIN_CTG_LEN = PgaConfig.K;
		    PgaConfig.MIN_UNIQUE_COV = 10;
		    PgaConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec500k.21"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/Ec500k.sim.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/Ec500k.21";
			//PgaConfig.STARTSTAGE = "removeTips"; //"scaffolding";
			
			PgaConfig.K = 21;
			PgaConfig.LOW_COV_THRESH = 5.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;

			PgaConfig.INSERT_LEN = 210;
			PgaConfig.MIN_CTG_LEN = PgaConfig.K;
			PgaConfig.MIN_UNIQUE_COV = 10;
			PgaConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("Ec500k.cor.21"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/Pga/data/Ec500k.sim.cor.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/Ec500k.cor.21/";
			//PgaConfig.STARTSTAGE = "scaffolding";
			
			PgaConfig.K = 21;
			PgaConfig.LOW_COV_THRESH = 5.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;

			PgaConfig.INSERT_LEN = 210;
			PgaConfig.MIN_CTG_LEN = PgaConfig.K;
			PgaConfig.MIN_UNIQUE_COV = 10;
			PgaConfig.MAX_UNIQUE_COV = 30;
		}
		else if (dataset.equals("arbrcrd"))
		{
			PgaConfig.hadoopReadPath = "/Users/mschatz/build/schatzlab-public/pga-bio/data/arbrcrd.36.sfa";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/arbrcrd.new/";
			PgaConfig.localBasePath = PgaConfig.hadoopBasePath + "work";
			
			//PgaConfig.RESTART_SCAFF_STEP = "update";
			//PgaConfig.RESTART_SCAFF_FRONTIER = 3;
			//PgaConfig.STARTSTAGE = "scaffolding";
			//PgaConfig.STOPSTAGE = "buildInitial";
			
			PgaConfig.K = 25;
			PgaConfig.LOW_COV_THRESH = 5.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;
			
			PgaConfig.INSERT_LEN = 100;
			PgaConfig.MIN_CTG_LEN = PgaConfig.K;
			PgaConfig.MIN_UNIQUE_COV = 10.0f;
			PgaConfig.MAX_UNIQUE_COV = 30.0f;
		}
		else if (dataset.equals("10hop"))
		{
			PgaConfig.hadoopReadPath = "foo";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/10hop/";
			PgaConfig.localBasePath = PgaConfig.hadoopBasePath + "work";
			
			PgaConfig.STARTSTAGE = "scaffolding";
			PgaConfig.RESTART_SCAFF_STAGE = "update";
			PgaConfig.RESTART_SCAFF_FRONTIER = 10;
			
			PgaConfig.K = 29;
			PgaConfig.LOW_COV_THRESH = 5.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;
			
			PgaConfig.INSERT_LEN = 210;
			PgaConfig.MIN_CTG_LEN = PgaConfig.K;
			PgaConfig.MIN_UNIQUE_COV = 15.0f;
			PgaConfig.MAX_UNIQUE_COV = 40.0f;
		}
		else if (dataset.equals("15hop"))
		{
			PgaConfig.hadoopReadPath = "foo";
			PgaConfig.hadoopBasePath = "/users/mschatz/pga/15hop/";
			PgaConfig.localBasePath = PgaConfig.hadoopBasePath + "work";
			
			PgaConfig.STARTSTAGE = "scaffolding";
			PgaConfig.RESTART_SCAFF_STAGE = "update";
			PgaConfig.RESTART_SCAFF_FRONTIER = 6;
			
			PgaConfig.K = 29;
			PgaConfig.LOW_COV_THRESH = 5.0f;
			PgaConfig.MAX_LOW_COV_LEN = 50;
			PgaConfig.MIN_THREAD_WEIGHT = 5;
			
			PgaConfig.INSERT_LEN = 210;
			PgaConfig.MIN_CTG_LEN = PgaConfig.K;
			PgaConfig.MIN_UNIQUE_COV = 15.0f;
			PgaConfig.MAX_UNIQUE_COV = 40.0f;
		}
		else
		{
			PgaConfig.parseOptions(args);
		}
		
	    PgaConfig.validateConfiguration();
		
		// Setup to use a file appender
	    BasicConfigurator.resetConfiguration();
		
		TTCCLayout lay = new TTCCLayout();
		lay.setDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
	    FileAppender fa = new FileAppender(lay, PgaConfig.localBasePath+"pga.details.log", true);
	    fa.setName("File Appender");
	    fa.setThreshold(Level.INFO);
	    BasicConfigurator.configure(fa);
	    
	    logfile = new FileOutputStream(PgaConfig.localBasePath+"pga.log", true);
	    logstream = new PrintStream(logfile);
	    
		PgaConfig.printConfiguration();
		
		// Time stamp
		DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		msg("== Starting time " + dfm.format(new Date()) + "\n");
		long globalstarttime = System.currentTimeMillis();
		
		if (PgaConfig.RUN_STATS != null)
		{
			computeStats("", PgaConfig.RUN_STATS);
		}
		else if (PgaConfig.CONVERT_FA != null)
		{
			convertFasta("", PgaConfig.CONVERT_FA, PgaConfig.CONVERT_FA + ".fa");
		}
		else if (PgaConfig.PRINT_FA != null)
		{
			// Runs off the local filesystem (not in hadoop)
			Graph2Fasta g2f = new Graph2Fasta();
			g2f.printFasta(PgaConfig.PRINT_FA);
		}
		else
		{
			// Assembly Pipeline
			// PSK Commented below
			//Preprocessing is done separately prior to job start
			/*Argo Start: Comment Build graph
			Date date_GB_start = new Date(); //Argo: added
			msg("----------Category GraphBuild starts at:" + date_GB_start.toString() + "----------\n"); //Argo: added
			if (runStage("preprocess"))
			{
				preprocess(PgaConfig.hadoopReadPath, PgaConfig.hadoopBasePath, preprocess);
				checkDone();
			}
			//Argo End: Comment Build graph*/
			
		    //PSK End
			if (runStage("buildInitial"))
			{
				buildInitial(PgaConfig.hadoopBasePath, preprocess, initial, initialcmp);	
				//computeStats(PgaConfig.hadoopBasePath, initialcmp);
				checkDone();
			}
			
            //We commented the below phases since below phases are integrated inside compress chains as supersteps
			/*
			if (runStage("removeTips"))
			{
				removeTips(PgaConfig.hadoopBasePath, initialcmp, notips, notipscmp);
				computeStats(PgaConfig.hadoopBasePath, notipscmp);
				checkDone();
			}

			if (runStage("popBubbles"))
			{
				popallbubbles(PgaConfig.hadoopBasePath, notipscmp, nobubbles, nobubblescmp);
				computeStats(PgaConfig.hadoopBasePath, nobubblescmp);
				checkDone();
			}
			*/
			// Argo Start: Comment Post Build Graph
			if (runStage("lowcov"))
			{
				//Take the output directly from the compression phase
				//removelowcov(PgaConfig.hadoopBasePath, nobubblescmp, lowcov, lowcovcmp);
				removelowcov(PgaConfig.hadoopBasePath, initialcmp, lowcov, lowcovcmp);
				computeStats(PgaConfig.hadoopBasePath, lowcovcmp);
				checkDone();
			}

			if (runStage("repeats"))
			{
				resolveRepeats(PgaConfig.hadoopBasePath, lowcovcmp, repeats, repeatscmp, false);
				computeStats(PgaConfig.hadoopBasePath, repeatscmp);
				convertFasta(PgaConfig.hadoopBasePath, repeatscmp, repeatscmp + ".fa");
				checkDone();
				Date date_EC_end = new Date(); //Argo: added
				msg("----------Category ErrorCorrection ends at:" + date_EC_end.toString() + "----------\n"); //Argo: added
			}

			if (runStage("scaffolding"))
			{
				Date date_scaff_start = new Date(); //Argo: added
				msg("--------Category Scaffolding starts at:" + date_scaff_start.toString() + "--------\n"); //Argo: added
				if (PgaConfig.INSERT_LEN > 0)
				{
					resolveRepeats(PgaConfig.hadoopBasePath, repeatscmp, scaff, finalcmp, true);
					computeStats(PgaConfig.hadoopBasePath, finalcmp);
				}
				else
				{
					save_result(PgaConfig.hadoopBasePath, repeatscmp, finalcmp);
					save_result(PgaConfig.hadoopBasePath, repeatscmp+".stats", finalcmp+".stats");
				}

				checkDone();
				Date date_scaff_end = new Date(); //Argo: added
				msg("--------Category Scaffolding ends at:" + date_scaff_end.toString() + "--------\n"); //Argo: added
			}

			if (runStage("convertFasta"))
			{
				convertFasta(PgaConfig.hadoopBasePath, finalcmp, finalcmp + ".fa");
				checkDone();
			}
			//Argo End: Comment Post Build Graph*/
		}
		
		
        // Final timestamp		
		long globalendtime = System.currentTimeMillis();
		long globalduration = (globalendtime - globalstarttime)/1000;
		msg("== Ending time " + dfm.format(new Date()) + "\n");
		msg("== Duration: " + globalduration + " s, " + GLOBALNUMSTEPS + " total steps\n");
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Pga(), args);
		System.exit(res);
	}
}

