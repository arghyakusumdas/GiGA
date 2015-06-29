package pga;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;



import java.io.IOException;

/**
 * Simple text based vertex output format example.
 */

public class GiraphTextOutputFormat extends
TextVertexOutputFormat<Text, Text, Text> {
	
	  /**
	   * Simple text based vertex writer
	   */
	  private class SimpleTextVertexWriter extends TextVertexWriterToEachLine {
	  
		@Override
		protected Text convertVertexToLine(
				Vertex<Text, Text, Text> vertex)
				throws IOException {
			
			return vertex.getValue();
//			if(vertex.getValue() != null)
//					return new Text(vertex.getId().toString()+"\t"+ vertex.getValue().toString());
//			else
//				return null;

			
			//			if(vertex instanceof PgaNode)
//			{
//				Node node = ((PgaNode)vertex).getNode();
//				if(node !=null)
//					return new Text(vertex.getId().toString()+"\t"+ node.toNodeMsg());
//
//			}
			//return null;
		}
	  }

	  @Override
	  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
	    throws IOException, InterruptedException {
	    return new SimpleTextVertexWriter();
	  }
	  
	}

