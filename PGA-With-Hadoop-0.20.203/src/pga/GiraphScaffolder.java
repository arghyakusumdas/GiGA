package pga;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import java.io.IOException;

public class GiraphScaffolder implements Tool  {
	private static boolean local = false ;//false;   //true
	private static final Logger sLogger = Logger.getLogger(GiraphScaffolder.class);
	

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//System.out.println("From Scaffolding");
		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setBoolean("giraph.useSuperstepCounters", false);
		sLogger.info("Inside main of Giraphscaffolder");
		//Configuration parameters for Scaffolding phase
		conf.set("K", args[4]);
		conf.set("min_wiggle", args[3]);
		conf.set("insert_Length",args[2]);
		
		conf.setComputationClass(pga.GraphSurf.class);
		conf.setVertexInputFormatClass(pga.GraphTextInputFormat.class);
		conf.setVertexOutputFormatClass(pga.GiraphTextOutputFormat.class);
		conf.setMasterComputeClass(pga.GraphSurfMaster.class);
		
		if (local == true) {
			conf.setWorkerConfiguration(1, 1, 100.0f);
			// Single node testing
			GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
			GiraphConstants.LOCAL_TEST_MODE.set(conf, true);
			System.err.println("running on local");
		}
		else
		{
			
			System.err.println("not running on local");
			//GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
			conf.setWorkerConfiguration(Integer.parseInt(args[5]), Integer.parseInt(args[5]), 100.0f);
		}
		GiraphJob job = new GiraphJob(conf,"GiraphScaffolder");
		cleanUnwantedFiles(job,args[0]);
		GiraphFileInputFormat.addVertexInputPath(conf, new Path(args[0]));
		GraphMerge.removeAndSetOutput(job, new Path(args[1]));
		if (job.run(true) == true) {
			conf.clear();
			return ;
		} else {
			return ;
		}		

	}
	public int run(String[] argArray) throws Exception {
		if (argArray.length < 6) {
			throw new IllegalArgumentException(
					"run: Must have 6 arguments <input path> <output path> "
							+ "<source vertex id> <# of workers>");
		}
		JobConf jobconf = new JobConf();
		PgaConfig.initializeConfiguration(jobconf);
		GiraphConfiguration conf = new GiraphConfiguration(jobconf);
		conf.setBoolean("giraph.useSuperstepCounters", false);
		//conf.setBoolean("giraph.useOutOfCoreGraph", true); //Argo Added this line
		
		sLogger.info("Inside run of GiraphScaffolder PSK");
		sLogger.info("PSK --K"+argArray[4]);
		sLogger.info("PSK --minwiggle"+argArray[3]);
		sLogger.info("PSK --insert_length"+argArray[2]);
		conf.set("K", argArray[4]);
		conf.set("min_wiggle", argArray[3]);
		conf.set("insert_Length",argArray[2]);	
		
		conf.setComputationClass(pga.GraphSurf.class);
		conf.setVertexInputFormatClass(pga.GraphTextInputFormat.class);
		conf.setVertexOutputFormatClass(pga.GiraphTextOutputFormat.class);
		//conf.setMasterComputeClass(pga.GraphSurfMaster.class);
		conf.setMasterComputeClass(pga.GraphSurfMaster.class);
		
		conf.set("giraph.zkList", PgaConfig.nodeList); //Argo Added
		conf.setInt("giraph.zkServerCount", 5); //argo added
		conf.setInt("giraph.zKMinSessionTimeout", 600000); //Argo added
		conf.setInt("giraph.zkMaxSessionTimeout", 900000); //Argo added
		conf.setInt("giraph.zkOpsMaxAttempts", 3); //Argo added
		conf.setInt("giraph.zkOpsRetryWaitMsecs", 5000); //Argo added
		conf.setInt("giraph.zkSessionMsecTimeout", 60000); //Argo added

		if (local == true) {
			conf.setWorkerConfiguration(1, 1, 100.0f);
			// Single node testing
			GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
			GiraphConstants.LOCAL_TEST_MODE.set(conf, true);
			//System.err.println("running on local");
		}
		else
		{
			
			System.err.println("not running on local");
			GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
			conf.setWorkerConfiguration(Integer.parseInt(argArray[5]), Integer.parseInt(argArray[5]), 100.0f);
		}
		GiraphJob job = new GiraphJob(conf,"GiraphScaffolder");
		cleanUnwantedFiles(job,argArray[0]);
		GiraphFileInputFormat.addVertexInputPath(conf, new Path(argArray[0]));
		GraphMerge.removeAndSetOutput(job, new Path(argArray[1]));
		sLogger.info("Starting the job PSK");
		if (job.run(true) == true) {
			conf.clear();
			return 0;
		} else {
			return -1 ;
		}
	}
	public void setConf(Configuration conf) {
	}
    public Configuration getConf() {
		
		return new Configuration();
	}
	private static void cleanUnwantedFiles(GiraphJob job, String path) {

		try {
			if (FileSystem.get(job.getConfiguration()).isDirectory(
					new Path(path))) {
				FileStatus[] stats = FileSystem.get(job.getConfiguration())
						.listStatus(new Path(path));
				for (FileStatus file : stats) {
					Path p = file.getPath();
					if (p.getName().charAt(0) == '_') {
						FileSystem.get(job.getConfiguration()).delete(p,true);
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
