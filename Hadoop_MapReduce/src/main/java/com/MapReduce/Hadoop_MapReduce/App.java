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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class App extends Configured implements Tool {
	
	//prwti fasi map stelnei pairs apo omada kai mege8os
	public static class TeamSizeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
	
	//prwti fasi reduce stelnei MONADIKA (oxi diplotipa) pairs omadas-megethous kai sunoliko plithos kai megethos olon ton omadon
	public static class UniqueTeamsReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
		private long size = 0;
		
		public static enum MyCounters{
			TEAM_COUNTER, //counter gia to plhthos monadikon teams
			TOTAL_SIZE //counter gia to sunoliko plhthos atomon olon ton monadikon teams
		}
		
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
			//double mo = sum/teams;
			//result.set(sum); 
			
			//teams.set(teams_counter);
			//teams_size.set(size);
			
			context.getCounter(MyCounters.TEAM_COUNTER).increment(1);
			context.getCounter(MyCounters.TOTAL_SIZE).increment(size);
		
			context.write(key, firstValue);
			//context.write(totalTeamsKey, teams); //elpizw auto na ftiaxnei mono ena idio pair
			//context.write(totalSizeKey, teams_size);
			
			
		} 
		
		
	}
		
		public int run(String[] args) throws Exception {
			Job job1 = Job.getInstance(getConf());
			job1.setJarByClass(App.class);
			job1.setNumReduceTasks(1);
			job1.setMapperClass(TeamSizeMapper.class);
			job1.setReducerClass(UniqueTeamsReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
			int completion = job1.waitForCompletion(true) ? 0 : 1;
			if (!job1.waitForCompletion(true))
				return 1; //Exit if Job1 fails
			
			long totalTeams = job1.getCounters().findCounter(UniqueTeamsReducer.MyCounters.TEAM_COUNTER).getValue();
			long totalSize = job1.getCounters().findCounter(UniqueTeamsReducer.MyCounters.TOTAL_SIZE).getValue();
			double averageTeamSize = (double) totalTeams/totalSize; //upologismos mesou orou mege8ous omadwn
			
			getConf().setDouble("average team size",averageTeamSize);
			
			
			
			return 0;
		}

	 public static void main(String[] args) throws Exception{
		 ToolRunner.run(new Configuration(), new App(), args);
	} 
}

