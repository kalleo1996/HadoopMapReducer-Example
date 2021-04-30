package firstHadoop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

public class multiplication {
	
	
	public static class firstmapper extends Mapper<Object, Text, NullWritable,Text> {
//		 private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//				String record = value.toString();
//				String[] parts = record.split("\t", -3);
//				context.write(new Text(parts[0]), new Text("num1	" + parts[1]));
				
				
				 StringTokenizer itr = new StringTokenizer(value.toString());
				 while (itr.hasMoreTokens()) {
					 
				 word.set(itr.nextToken());
				 
				 String token="ft"+"\t"+ word;
				 System.out.println(token);
				 context.write(NullWritable.get(),new Text(token));
				
				
				
			}
		}
		 
	}
		

	public static class secondmapper extends Mapper<Object, Text, NullWritable, Text> {
		 private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 StringTokenizer itr = new StringTokenizer(value.toString());
			 while (itr.hasMoreTokens()) {
				 
				 word.set(itr.nextToken());
				 
				 String token="st"+"\t"+ word;
				 System.out.println(token);
				 context.write(NullWritable.get(),new Text(token));
		}
	}
	}
	
	public static class ReduceJoinReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
		
		int totalmax=0;
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			String output = String.format("%s %d", "Total",totalmax);
			context.write(NullWritable.get(), new Text(output));
		}
		
		
		
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String name = "";
			double total = 0.0;
			int count=1;
			int count2=1;
//			int Arrayft[];    //declaring array
//			Arrayft = new int[count];
			  ArrayList<Integer> Arrayft = new ArrayList<Integer>();
			
			  ArrayList<Integer> Arrayst = new ArrayList<Integer>();
			
//			int Arrayfn[];    //declaring array
//			Arrayfn = new int[count];
			  ArrayList<Integer> Arrayfn = new ArrayList<Integer>();
			
			
			// allocating memory to array
			for (Text t : values) {
				String parts[] = t.toString().split("\t");
				if (parts[0].equals("ft")) {
					
					count++;
					int ft =Integer.parseInt(parts[1]);
					Arrayft.add(ft);
					
					
				} else if (parts[0].equals("st")) {
					count2++;
					
					int st =Integer.parseInt(parts[1]);
					Arrayst.add(st);
					
				}
				
				
				
				
			}
			
			
			
			
			for(int i=0;i<Arrayft.size();i++)
			{
				
				Arrayfn.add(Arrayft.get(i)*Arrayst.get(i));
				totalmax=totalmax+Arrayfn.get(i);
				System.out.println(Arrayft.get(i)+" "+Arrayst.get(i));
				
				String str = String.format("%d %s %d %s %d ", Arrayft.get(i),"x",Arrayst.get(i),"=",Arrayfn.get(i));
				context.write(NullWritable.get(), new Text(str));
			}
			
			
			
		
				
		
		}
	}
	
public static void main(String[] args) throws Exception {
		
		args = new String[] { "D:\\hadoop-env\\datafile\\input\\multi\\first.txt", "D:\\hadoop-env\\datafile\\input\\multi\\second.txt", 
		"D:\\hadoop-env\\datafile\\output3multi" };

		/* delete the output directory before running the job */
		FileUtils.deleteDirectory(new File(args[2]));

		if (args.length != 3) {
			System.err.println("Please specify the input and output path");
			System.exit(-1);
		}
		
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("multiplication");
		job.setJarByClass(multiplication.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, firstmapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, secondmapper.class);
		Path outputPath = new Path(args[2]);

		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	
	
	


}
