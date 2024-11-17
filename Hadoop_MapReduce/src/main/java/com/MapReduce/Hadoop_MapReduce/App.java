package com.MapReduce.Hadoop_MapReduce;

import java.io.IOException;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class App extends Configured implements Tool {
	
	//prwti fasi map stelnei pair omada kai mege8os
	public static class TeamsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text mapKey = new Text();
	IntWritable mapValue = new IntWritable();
	
		@Override
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
			String record = value.toString(); //each record
			try {
				String record_fields[] = record.split(";");
				String authors[] = record_fields[1].split("\\|"); //pipeline escape

				
				mapValue.set(authors.length);
				mapKey.set(record_fields[1]);
				context.write(mapKey, mapValue);
			} catch (Exception e) {
				e.printStackTrace();
			} 
		} 
	}
	
	//prwti fasi reduce stelnei pairs omades mia fora me mege8os tous kai pli8os omadwn
	public static class TeamsReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable teams = new IntWritable();
		private IntWritable teams_size = new IntWritable();
		private int teams_counter = 0;
		private int size = 0;
		private Text totalTeamsKey = new Text("Total teams");
		private Text totalSizeKey = new Text("Total size");
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
//			int sum = 0;
//			for (IntWritable val : values) {
//				sum += val.get();
//				teams++;
//			}
			
			//Each number of "values" has the size of the team so we take the first.
			IntWritable firstValue = null;
			for(IntWritable value: values) {
				firstValue = value;
				size += value.get();
				break;
			}
			//int sum = values[0];
			teams_counter++;
			//double mo = sum/teams;
			//result.set(sum); 
			teams.set(teams_counter);
			teams_size.set(size);
			//context.write(key, result);
			context.write(key, firstValue);
			context.write(totalTeamsKey, teams); //elpizw auto na ftiaxnei mono ena idio pair
			context.write(totalSizeKey, teams_size);
			
			
		} 
		
	}
		
		public int run(String[] args) throws Exception {
			Job job = Job.getInstance(getConf());
			job.setJarByClass(App.class);
			job.setNumReduceTasks(2);
			job.setMapperClass(TeamsMapper.class);
			job.setReducerClass(TeamsReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
			int completion = job.waitForCompletion(true) ? 0 : 1;
			return(completion);
		}

	 public static void main(String[] args) throws Exception{
		 ToolRunner.run(new Configuration(), new App(), args);
	} 
}

