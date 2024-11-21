package com.MapReduce.Hadoop_MapReduce;

import java.io.IOException;
import java.util.Iterator;

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
	
	
	//!First MapReduce phase!
	
	//The first Mapper outputs ALL pairs of Author teams and their respective size. 
	//Each team may have publishes multiple papers, so duplicate entries exist!
	public static class TeamSizeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		Text mapKey = new Text();
		IntWritable mapValue = new IntWritable();
	
		@Override
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			String record = value.toString(); //A single record from the CSV input files each time. 
			
			try {
				if(record.startsWith("id")) return; //Ignore the first line in the CSV files that contain the cells' titles.
	
				String record_fields[] = record.split(";",3);//Split the record in the indicated way until the second "word" (The Authors cell). The rest of the record is not needed.
				String authors[] = record_fields[1].split("\\|"); //Split the authors field by the indicated | character. The pipe is treated as the OR operator in regex so we escape it with '\\' to treat it as a literal character.
				
				if(!record_fields[1].isEmpty()) { //Ignores potential records with an empty author field.
					mapValue.set(authors.length);
					mapKey.set(record_fields[1] + ","); //The comma after the key helps splitting the Reducer's input below.
					context.write(mapKey, mapValue);
				}

			} catch (Exception e) {
				e.printStackTrace();
			} 
		} 
	}
	
	
	//The first Reducer outputs UNIQUE pairs of Author teams and their respective size (No duplicate entries).
	//It also keeps track of the total number and size of said teams to be used as input in the second Map Reduce phase.
	public static class UniqueTeamsReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		public static enum MyCounters{
			TEAM_COUNTER, //Number of unique author teams.
			TOTAL_SIZE //Aggregated team size of all unique teams.
		}
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

			long size = 0;
			
			//Each "value" has the size of the team, so we only increment by the first one.
			IntWritable firstValue = null;
			for(IntWritable val: values) {
				firstValue = val;
				size+=val.get();
				break;
			}
			
			//calculate the total number of teams and the total size of teams
			context.getCounter(MyCounters.TEAM_COUNTER).increment(1);
			context.getCounter(MyCounters.TOTAL_SIZE).increment(size);
		
			context.write(key, firstValue);
		} 
	}
	
	
	//!Second MapReduce phase!
	
	//Second phase Mapper 
	public static class CompareMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		Text mapKey = new Text();
		IntWritable mapValue = new IntWritable();
		private double averageSize;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			//Get the average size from the run configuration
			Configuration config = context.getConfiguration();
			averageSize = config.getDouble("average team size",0.0);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) {
			String line = value.toString(); //A single record (in the format authorNames, teamSize) from the phase 1 Map Reduce output each time.
			String team_and_size[] = line.split(",\\s*");//Regular expression splitting the input at , ignoring ANY (*) leading whitespace (\\s) after it.
			
			try {
				
				int teamSize = Integer.parseInt(team_and_size[1]);

				//Just using the same "BelowAverage" string as the key now, as we are only concerned about HOW MANY teams
				//are of below average total size and not WHO these specific teams are. Using the same key for all pairs is crucial for the Reducer below.
				mapKey.set("BelowAverage"); 
				mapValue.set(1);
						
				if(teamSize < averageSize) 
					context.write(mapKey, mapValue);
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	//The second phase Reducer finally outputs the number of teams that are of below average total size.
	public static class SumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private int teamsBelowAverage = 0;
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			
			for(IntWritable val: values) teamsBelowAverage += val.get(); //Because the key in all pairs is the same and the value is 1, this essentially sums the teams that are of below average total size.
			
			//Will only save one Key-Pair value on the output file as the second phase Mapper sends many pairs with the SAME KEY.
			context.write(new Text("Number of teams below average size"), new IntWritable(teamsBelowAverage));
		}
	}
	
	
	public int run(String[] args) throws Exception {
		//first job for first MapReduce phase.
		Job job1 = Job.getInstance(getConf(), "Phase 1");
		job1.setJarByClass(App.class);
		job1.setNumReduceTasks(1);
		job1.setMapperClass(TeamSizeMapper.class);
		job1.setReducerClass(UniqueTeamsReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path firstPhaseOutput = new Path(args[1]); //For use in the second job.
		FileOutputFormat.setOutputPath(job1, firstPhaseOutput);
	
		if (!job1.waitForCompletion(true))
			return 1; //Exit if Job1 fails.
		
		//Get the total number of teams.
		long totalTeams = job1.getCounters().findCounter(UniqueTeamsReducer.MyCounters.TEAM_COUNTER).getValue();
		//Get the total size of said teams (How many total authors).
		long totalSize = job1.getCounters().findCounter(UniqueTeamsReducer.MyCounters.TOTAL_SIZE).getValue();
		//Calculate average size of teams.
		double averageTeamSize = (double) totalSize/totalTeams;
		
		//Put the average size in the run configuration so the second Mapper can use it for calculation.
		getConf().setDouble("average team size",averageTeamSize);
		
		//Second job for second MapReduce phase.
		Job job2 = Job.getInstance(getConf(), "Phase 2");
		job2.setJarByClass(App.class);
		job2.setNumReduceTasks(1);
		job2.setMapperClass(CompareMapper.class);
		job2.setReducerClass(SumReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, firstPhaseOutput);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		return job2.waitForCompletion(true) ? 0 : 1;
	}

	 public static void main(String[] args) throws Exception{
		 ToolRunner.run(new Configuration(), new App(), args);
	} 
}

