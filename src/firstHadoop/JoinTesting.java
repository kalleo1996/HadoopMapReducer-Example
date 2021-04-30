package firstHadoop;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import firstHadoop.ReduceJoin.DeptMapper;
import firstHadoop.ReduceJoin.EmpMapper;
import firstHadoop.ReduceJoin.ReduceJoinReducer;

public class JoinTesting {

	public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split("\t", -3);
			                      //customerid                     customername
			context.write(new Text(parts[0]), new Text("cus   " + parts[1]));
		}
	}
	
	public static class OrderMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split("\t", -3);
			                 //customerid                                   //orderid1      //orderdate1
			context.write(new Text(parts[1]), new Text("ord   " + parts[0]+"   "+parts[3]));
		}
	}
	
	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String name = "";
//			double total = 0.0;
//			int count = 0;
			String orderdate = "";
			String Orderid="";
			
			for (Text t : values) {
				String parts[] = t.toString().split("  ");
				if (parts[0].equals("cus")) {
//					count++;
//					total += Float.parseFloat(parts[1]);
					name = parts[1];
					
				} else if (parts[0].equals("ord")) {
					orderdate=parts[1];
					Orderid=parts[2];
					
				}
			}
		       if(name.equals("") && !orderdate.equals(""))
		    		   {
		    	     String str = String.format("%s %s %s", name, orderdate,Orderid);
					context.write(new Text(key), new Text(str));
		    		   }
				
		
		}
	}
	
	
public static void main(String[] args) throws Exception {
		
		args = new String[] { "D:\\hadoop-env\\datafile\\input\\Joindata\\customerdata.txt", "D:\\hadoop-env\\datafile\\input\\Joindata\\Orderdata.txt", 
		"D:\\hadoop-env\\datafile\\output3cusord" };

		/* delete the output directory before running the job */
		FileUtils.deleteDirectory(new File(args[2]));

		if (args.length != 3) {
			System.err.println("Please specify the input and output path");
			System.exit(-1);
		}
		
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("Reduce-side join");
		job.setJarByClass(JoinTesting.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OrderMapper.class);
		Path outputPath = new Path(args[2]);

		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	
	
	
}
