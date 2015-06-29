package pga;

import org.apache.giraph.graph.DefaultVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;


public class PgaNode extends DefaultVertex<Text, Text, Text> {

	Node node = new Node();

	
	public Node getNode() {
		return node;
	}
	
	public void setup(Configuration conf) {
	    
	}

	public void setNodeNUll() {
		node=null;
		
	}

}
