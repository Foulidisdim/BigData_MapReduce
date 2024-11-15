package com.MapReduce.Hadoop_MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App extends Configured implements Tool {
	public static class TeamsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text mapKey = new Text();
	IntWritable mapValue = new IntWritable();
	
		@Override
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			String record = value.toString(); //each record
			try {
				String record_fields[] = record.split(";");
				mapValue.set(1);
				//String authors[] = record_fields[1].split("\\|"); //pipeline escape

				mapKey.set(record_fields[1]);
				context.write(mapKey, mapValue);
			} catch (Exception e) {
				e.printStackTrace();
			} 
		} 
	}
	public static class TeamsReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
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

