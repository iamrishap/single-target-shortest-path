package comp9313.ass2;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleTargetSP {
	/*
	 * @Author: Rishap Sharma (UNSW 18s1: z5202535)
	 * Class for the "Single Target Shortest Path" which is a minor modification
	 * to the Dijikstra algorithm for map reduce. It takes a files with nodes and their distances.
	 * It first converts the input to Adjacency list format and then runs multiple pass to 
	 * get the shortest path to reach the source (specified as the third input argument) from 
	 * all the other nodes. 
	 * 
	 * Input is a file with each lines describing graph edges and vertices
	 * Output is a file with each line describing the shortest path from a node and the distance of the path
	 * 
	 * Input format : RowNo Soure Target EdgeLength(aka weight) Ex: 1 0 2 10.3
	 * Output format : TargetNode minDistance minDistancePath Ex: 29 86.4 29->74->80->116 
	 * 
	 */

	public static String OUT = "output"; // Out Initialization
	public static String IN = "input";  // In Initialization
	public static Double DMAX = Double.MAX_VALUE;  // Used for paths not yet found or for min comparison
	
	static enum MoreIterations {
		// Enum counter to find the number of values updated by a map reduce job
        updatedValues
    }
	
	public static class STMapper extends Mapper<Object, Text, Text, Text> {
		// Mapper for the Single Target Shortest Path
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			String line = value.toString(); // looks like 1	999999 2:3.0,0:10.0  Viz <Destination DistanceMin AdjListOf<From:Distance> PathForMin>
			String[] sp = line.split("\\s+");  // <DestinationNode DistanceMin AdjListOf<From:Distance> PathForMin>
			if(Double.parseDouble(sp[1]) < DMAX){	 // If not infinity
				String[] NodesInfo = sp[2].split(",");
				for (int i = 0; i < NodesInfo.length; i++) {
					if(!NodesInfo[i].equals("null") && !NodesInfo[i].equals(null)){  // If nothing in adjaceny list, no changes in the VALUE
						String [] destAndWeight = NodesInfo[i].split(":");  // Get the destination and Weight for this value
						Double distanceadd = Double.parseDouble(destAndWeight[1]) + Double.parseDouble(sp[1]);  // EmitDistance = distance to this node + AdjListNode distance
						word.set("VALUE " + distanceadd + " " + destAndWeight[0] + "<-" +  sp[3]); // VALUE distance path Ex: VALUE 19.0 1->3)
						context.write(new Text(destAndWeight[0]), word);  // destAndWeight[0] = targetnode, word = distance info Ex: VALUE 19.0 1->3
						word.clear();
					}
				}
			}
			word.set("NODES " + sp[2] + " " + sp[3] + " " + sp[1]);  // Passing on the AdjList with other info Ex: NODES AdjList MinPath MinDistance
			context.write(new Text(sp[0]), word);  // Sending the NODES info for future steps
			word.clear();
		}
	}

	public static class STReducer extends Reducer<Text, Text, Text, Text> {
		// Reducer for the Single Target Shortest Path
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String nodes = null;  // UNMODED?? TODO
			Text word = new Text();
			Double lowest = DMAX; // start at infinity
			Double lowestBefore = DMAX; // start at infinity
			String path = null;
			for (Text val : values) {
				String[] sp = val.toString().split(" ");  // splits on space
				if (sp[0].equalsIgnoreCase("NODES")) {  // If it is a NODES type emitted value Ex: NODES AdjList MinPath MinDistance
					nodes = sp[1];
					lowestBefore = Double.parseDouble(sp[3]);
					if(path==null) {  // Otherwise path and lowest would've been set in the elseif already
						path = sp[2];  // Previous minPath
						lowest = Double.parseDouble(sp[3]);  // Previous minDistance
					}
				} else if (sp[0].equalsIgnoreCase("VALUE")) {
					Double distance = Double.parseDouble(sp[1]);
					if (distance < lowest){  // IF this distance is less than the lowest value we have so far store new distance and new path
						lowest = distance;
						path = sp[2];
					}
				}
			}
			if(!lowest.equals(lowestBefore)){  // If the lowest is getting changed. This implies an update in this iteration..
				Counter updated = context.getCounter(SingleTargetSP.MoreIterations.updatedValues);
				updated.increment(1);  // Incrementing the counter
			}
			word.set(lowest + " " + nodes + " " + path);
			context.write(key, word);
			word.clear();
		}
	}

	public static class AdjListMapper extends Mapper<Object, Text, Text, Text> {
		// Mapper for the Single Target Shortest Path Adjaceny List creation
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " ");
			while (itr.hasMoreTokens()) {
				itr.nextToken();  // Skip the row number. It is useless for this implementation.
				String source = itr.nextToken();
				String target = itr.nextToken();
				String weight = itr.nextToken();
				Text k = new Text(target);
				Text v = new Text(source + ":" + weight);
				context.write(k, v);
		    }
		}
	}

	public static class AdjListReducer extends Reducer<Text, Text, Text, Text> {
		// Reducer for the Single Target Shortest Path Adjaceny List creation
		String sourceNode;
		protected void setup(Context context) throws IOException,
        InterruptedException {
		    Configuration conf = context.getConfiguration();
		    sourceNode = conf.get("stsp.dijikstra.source"); // Getting the value of the source node from the stored config value
		}
		
		@Override
		public void reduce(Text node, Iterable<Text> neighbours, Context context) throws IOException, InterruptedException {
			StringBuffer adjList = new StringBuffer();
			for (Text neighbour : neighbours) {
				adjList.append(neighbour);
				adjList.append(",");
			}
			String adjListFull = adjList.length() > 0 ? adjList.substring(0, adjList.length() - 1) : "";  // Trimming the trailing , if present
			Double distance = Double.parseDouble(sourceNode) == Double.parseDouble(node.toString())? 0 : DMAX;
			context.write(node, new Text(distance + " " + adjListFull + " " + node));  // node, distance, adjList, path
		}
	}

	public static class STSPOutMapper extends Mapper<Object, Text, IntWritable, Text> {
		// Mapper for the Single Target Shortest Path Output
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] sp = value.toString().split("\\s+");  // sp[0] is node, sp[1] is pathLenght, sp[2] is adjList, sp[3] is minPath
			Double thisPathLen = Double.parseDouble(sp[1]);
			if(!thisPathLen.equals(DMAX)){  // If no path has been found, path length will still be DMAX. We don't want to emit that.
				String requiredPath = sp[3].replace("<-", "->");
				context.write(new IntWritable(Integer.parseInt(sp[0])), new Text(sp[1] + "\t" + requiredPath));
			}
			
		}
	}

	public static class STSPOutReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// Reducer for the Single Target Shortest Path Output
		@Override
		public void reduce(IntWritable node, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text result = new Text();
			for (Text val : values) {  // Although there will be only one value, we need to iterate the received val iterator 
				result = val;
			}
			context.write(node, result);
		}
	}
	
	public static void main(String[] args) throws Exception {

		IN = args[0];  // Input path
		OUT = args[1];  // Output path
		int iteration = 0;
        String input = IN;
        String output = OUT + iteration;  // Input path for this iteration
		
        // Configure and run map reduce job to create Adjacency List from the input
		Configuration confAdj = new Configuration();
        Job jobAdj = Job.getInstance(confAdj, "AdjList Creator");
        jobAdj.getConfiguration().setStrings("stsp.dijikstra.source", args[2]);
        jobAdj.setJarByClass(SingleTargetSP.class);
        jobAdj.setMapperClass(AdjListMapper.class);
        jobAdj.setReducerClass(AdjListReducer.class);
        jobAdj.setNumReduceTasks(1);
        jobAdj.setOutputKeyClass(Text.class);
        jobAdj.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(jobAdj, new Path(IN));
        FileOutputFormat.setOutputPath(jobAdj, new Path(output));
        jobAdj.waitForCompletion(true);
        
        boolean isdone = false;
        
        double updateCount = 0.0;
		while (isdone == false) {
			
			// Configure and run the MapReduce job
			input = output;
			iteration++;
			output = OUT + iteration;
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "SingleTargetSP");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(STMapper.class);
			job.setReducerClass(STReducer.class);
			job.setNumReduceTasks(1);
			FileInputFormat.setInputPaths(job, new Path(input));  // + "/part-r-00000" ?
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			updateCount = job.getCounters().findCounter(SingleTargetSP.MoreIterations.updatedValues).getValue();
			isdone = updateCount > 0 ? false: true;
			
			// To delete the output folder in the previous step
			if (isdone == false) {
			  FileSystem hdfs = FileSystem.get(new URI(input), conf);
        	  hdfs.delete(new Path(input), true);
			}
		}
		
		// Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
		Configuration confOut = new Configuration();
        Job jobOut = Job.getInstance(confOut, "STSP Output");
        jobOut.setJarByClass(SingleTargetSP.class);
        jobOut.setMapperClass(STSPOutMapper.class);
        jobOut.setReducerClass(STSPOutReducer.class);
        jobOut.setNumReduceTasks(1);
        jobOut.setOutputKeyClass(IntWritable.class);
        jobOut.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(jobOut, new Path(input + "/part-r-00000"));
        FileOutputFormat.setOutputPath(jobOut, new Path(OUT));
        jobOut.waitForCompletion(true);
        FileSystem hdfs = FileSystem.get(new URI(input), confOut);
  	  	hdfs.delete(new Path(input), true);
	  	hdfs.delete(new Path(output), true);
	}
}
