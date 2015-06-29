package pga;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.DefaultVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.GiraphTextInputFormat;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;




import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * unweighted graphs with long ids. Each line consists of: vertex neighbor1
 * neighbor2 ...
 */
public class GraphTextInputFormat
    extends TextVertexInputFormat<Text, Text,
    Text>
{  /** Configuration. */
    @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context)
    throws IOException {
    return new LongDoubleDoubleDoubleVertexReader();
  }

  public static void setPath(GiraphConfiguration conf,Path path)
  {
	  try {
		  GiraphTextInputFormat.setVertexInputPath(conf, path);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  
  /**
   * Vertex reader associated with
   * {@link LongDoubleDoubleTextInputFormat}.
   */
  public class LongDoubleDoubleDoubleVertexReader extends
    TextVertexInputFormat<Text, Text,
    Text>.TextVertexReader {
    @Override
    public Vertex<Text, Text, Text>
    getCurrentVertex() throws IOException, InterruptedException {
    	Vertex<Text, Text, Text>
        vertex =  getConf().createVertex();
    	vertex.setConf(getConf());
      String line=getRecordReader().getCurrentValue().toString();
      String [] items = line.split("\t");

		String nodeid_m = items[0];
    vertex.initialize(new Text(nodeid_m),new Text(line));
      //System.err.println("New recored"+vertex.getId()+"	"+ line);
      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
  
  
  
}
