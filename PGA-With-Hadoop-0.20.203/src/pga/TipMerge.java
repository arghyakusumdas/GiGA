package pga;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

public class TipMerge implements Tool {

	private static boolean local = false;
	private static final Logger sLogger = Logger.getLogger(TipMerge.class);
	public static void main(String[] argArray) throws Exception {

		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setBoolean("giraph.useSuperstepCounters", false);
		conf.set("K", argArray[3]);
		conf.setComputationClass(pga.TipComputation.class);
		conf.setVertexInputFormatClass(pga.GraphTextInputFormat.class);
		conf.setVertexOutputFormatClass(pga.GiraphTextOutputFormat.class);
		conf.setMasterComputeClass(pga.TipMaster.class);
		
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
			GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
			conf.setWorkerConfiguration(Integer.parseInt(argArray[2]), Integer.parseInt(argArray[2]), 100.0f);
		}
		GiraphJob job = new GiraphJob(conf, "TipMerge");
		cleanUnwantedFiles(job,argArray[0]);
		GiraphFileInputFormat.addVertexInputPath(conf, new Path(argArray[0]));
		TipMerge.removeAndSetOutput(job, new Path(argArray[1]));
		if (job.run(true) == true) {
			conf.clear();
			return ;
		} else {
			return;
		}

	}

	
	public void setConf(Configuration conf) {
			}

	public int run(String[] argArray) throws Exception {
		if (argArray.length < 3) {
			throw new IllegalArgumentException(
					"run: Must have 4 arguments <input path> <output path> "
							+ "<source vertex id> <# of workers>");
		}
		JobConf jobconf = new JobConf();
		PgaConfig.initializeConfiguration(jobconf);
		GiraphConfiguration conf = new GiraphConfiguration(jobconf); //giraph job configuration
		conf.setBoolean("giraph.useSuperstepCounters", false);
		conf.setComputationClass(pga.TipComputation.class);  //compression in giraph
		conf.setVertexInputFormatClass(pga.GraphTextInputFormat.class);
		conf.setVertexOutputFormatClass(pga.GiraphTextOutputFormat.class);
		conf.setMasterComputeClass(pga.TipMaster.class);  //to halt supersteps
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
			GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
			conf.setWorkerConfiguration(Integer.parseInt(argArray[2]), Integer.parseInt(argArray[2]), 100.0f);
		}
		GiraphJob job = new GiraphJob(conf, "TipMerge");
		cleanUnwantedFiles(job,argArray[0]);  //to remove previous runs of hadoop
		GiraphFileInputFormat.addVertexInputPath(conf, new Path(argArray[0]));
		removeAndSetOutput(job, new Path(argArray[1])); //to remove compressible.jva output
		if (job.run(true) == true) {
			conf.clear();
			return 0;
		} 
			return -1;

	}


	private static void cleanUnwantedFiles(GiraphJob job, String path)
	  {
	    try
	    {
	      if (FileSystem.get(job.getConfiguration()).isDirectory(new Path(path)))
	      {
	        FileStatus[] stats = FileSystem.get(job.getConfiguration()).listStatus(new Path(path));

	        for (FileStatus file : stats) {
	          Path p = file.getPath();
	          if (p.getName().charAt(0) == '_') {
	            //System.err.println("Deleteing path" + p.toString());
	            FileSystem.get(job.getConfiguration()).delete(p, true);
	          }
	        }
	      }
	    }
	    catch (IOException e) {
	      e.printStackTrace();
	    }
	  }

	  public static void removeAndSetOutput(GiraphJob job, Path outputPath)
	    throws IOException
	  {
	    FileUtils.deletePath(job.getConfiguration(), outputPath);
	    FileOutputFormat.setOutputPath(job.getInternalJob(), outputPath);
	  }

	  public Configuration getConf()
	  {
	    return new Configuration();
	  }

	
}

