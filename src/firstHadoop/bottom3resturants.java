package firstHadoop;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import firstHadoop.bottom3ongener.BottomReducer;
import firstHadoop.bottom3ongener.bottomMapper;

public class bottom3resturants {

	public static class bottomMapper extends Mapper<Object, Text, Text, Text> {
int count =0;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			count++;
	
			String line = value.toString();
			String[] cols = line.split(",");
            
			String category = cols[7].trim();
		if(count>=2)
			
			context.write(new Text(category), new Text(value));
		
			

		}
	}
	
public static class BottomReducer extends Reducer<Text, Text, NullWritable, Text> {
		
//		public int max = 0
	public int totalunits = 0;
//		double total=0.0;
		protected void setup(Context context) throws IOException, InterruptedException {
//			String country = "genere";
//			String output = String.format("%s - %s","MAX", "DATE");
//			context.write(new Text(country), new Text(output));
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
//			String output = String.format("%d - %s", totalMax, new Date().toString());
//			context.write(new Text(hm.size()+""), new Text(output));
			
//			for (String i : alltherecords) {
//				context.write(NullWritable.get(), new Text(i));
//			    }
//			for (Text t : TopRecord.descendingMap().values()) {
//				// Output our ten records to the file system with a null key
//				//alltherecords.add(t.toString());
//			String na="total sales";
//			String str2 = String.format("%s %f",na,total);
//				context.write(NullWritable.get(), new Text(str2));
//				
//				}
			
			
			
			
		}
	//private ArrayList<String> alltherecords = new ArrayList<String>();
 
		//private TreeMap<Float, Text> TopRecord = new TreeMap<Float, Text>();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  
			TreeMap<Integer, Text> TopRecordMap = new TreeMap<Integer, Text>();
			for (Text value : values) {
				 
				String line=value.toString();
				if(line.length()>0){
				String[] tokens=line.split(",");
				//split the data and fetch salary
				if(!tokens[4].equals(""))
				{
					int sale=Integer.parseInt(tokens[4]);
					//insert salary as key and Salary as value
					//tree map sort the records based on salary
					TopRecordMap.put(sale, new Text(value));
					
					if(TopRecordMap.size()>4){
						TopRecordMap.remove(TopRecordMap.lastKey());
						}
					
					
				}
				}
				
				}
			
			
			
			for (Text t : TopRecordMap.descendingMap().values()) {
				// Output our ten records to the file system with a null key
				//alltherecords.add(t.toString());
				String lines=t.toString();
				String[] tokens=lines.split(",");
				int sales=Integer.parseInt(tokens[4]);
				if(tokens[7].equals("Quick Service & Burger") || tokens[7].equals("Quick Service & Chicken") )
				{
					String str = String.format("%s %s %d", tokens[7],tokens[1],sales);
					totalunits=totalunits+sales;
					context.write(NullWritable.get(), new Text(str));
				}
				
				}
			
			
			
		}
		
		
		 public static void main(String[] args) throws Exception {

			
				 Configuration conf = new Configuration();
					Job job = Job.getInstance(conf, "videogame");
					job.setJarByClass(bottom3ongener.class);
					args = new String[] { "D:\\hadoop-env\\datafile\\input\\editeddatset.csv",
							"D:\\hadoop-env\\datafile\\outputresturant" };
					/* delete the output directory before running the job */
					FileUtils.deleteDirectory(new File(args[1]));
					if (args.length != 2) {
						System.err.println("Please specify the input and output path");
						System.exit(-1);
					}
					FileInputFormat.addInputPath(job, new Path(args[0]));
					FileOutputFormat.setOutputPath(job, new Path(args[1]));
					job.setMapperClass(bottomMapper.class);
					
					job.setReducerClass(BottomReducer.class);
					job.setNumReduceTasks(1);
					
					job.setMapOutputKeyClass(Text.class);
					job.setMapOutputValueClass(Text.class);
					job.setOutputKeyClass(NullWritable.class);
					job.setOutputValueClass(Text.class);
					
					System.exit(job.waitForCompletion(true) ? 0 : 1);
			
			 }
		
		
		
		
		
		
		
		
		
}
	
	








	
	
	
	
	
}
