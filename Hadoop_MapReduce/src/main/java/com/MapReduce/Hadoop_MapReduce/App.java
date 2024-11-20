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
	
	
	//First MapReduce phase
	
	
	//prwti fasi map stelnei pairs apo omada kai mege8os
	public static class TeamSizeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text mapKey = new Text();
	IntWritable mapValue = new IntWritable();
	
		@Override
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
			String record = value.toString(); //each record
			
			try {
				if(record.startsWith("id")) return; //agnooume tin prwti grammi twn dyo arxeiwn poy apla onomazei ta pedia - to dokimasa to kanei ontws avoid
	
				String record_fields[] = record.split(";");
				String authors[] = record_fields[1].split("\\|"); //pipeline escape , yparxei la8os, mallon den metraei swsta tous authors giati aytoi mporei na exoun spaces kai ""
				
				if(!record_fields[1].isEmpty()) { //wste na agnoisoume eggrafes poy einai kenes sto 2o pedio. Etsi leitoyrgei ontws alla me authors.len > 0 oxi
					mapValue.set(authors.length);
					mapKey.set(record_fields[1] + ","); //gia na diaxwristoun me ,
					context.write(mapKey, mapValue);
				}

			} catch (Exception e) {
				e.printStackTrace();
			} 
		} 
	}
	
	
	//prwti fasi reduce stelnei MONADIKA (oxi diplotipa) pairs omadas-megethous kai sunoliko plithos kai megethos olon ton omadon
	public static class UniqueTeamsReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		public static enum MyCounters{
			TEAM_COUNTER, //counter gia to plhthos monadikon teams
			TOTAL_SIZE //counter gia to sunoliko plhthos atomon olon ton monadikon teams
		}
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

			long size = 0;
			//Each number of "values" has the size of the team so we take the first.
			IntWritable firstValue = null;
			for(IntWritable value: values) {
				firstValue = value;
				size += value.get();
				break;
			}
			
			//calculate the total number of teams and the total size of teams
			context.getCounter(MyCounters.TEAM_COUNTER).increment(1);
			context.getCounter(MyCounters.TOTAL_SIZE).increment(size);
		
			context.write(key, firstValue);
			
		} 
	}
	
	
	
	//Second Map Reduce phase
	
	
	
	//second phase mapper 
	public static class CompareMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		Text mapKey = new Text();
		IntWritable mapValue = new IntWritable();
		private double averageSize;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{ //idk an prepei protected i public
			//take the average size from the configuration
			Configuration config = context.getConfiguration();
			averageSize = config.getDouble("average team size",0.0);
			//for debugging
			System.out.println("average size: " + averageSize);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) {
			//take the value from output file from phase 1 MapReduce
			String line = value.toString();
			String teams_and_size[] = line.split(",\\s*");//kanoniki ekfrasi wste na kanei split me to , akolou8oumeno me miden i perissotera kena
			
				try {
					
					int teamSize = Integer.parseInt(teams_and_size[1]);

					mapKey.set("BelowAverage"); //den 8eloume tis omades mono poses einai. Ara ayto 8a boi8isei ton reducer apla na kanei sum aytou tou minadikou kleidiou
					mapValue.set(1);
							
					if(teamSize < averageSize) { //check if the team's size is below the average
						context.write(mapKey, mapValue);
					}
					
					
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			
		}
		
	}
	
	
	//second phase Reducer
	public static class SumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private int teamsBelowAverage = 0;
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			
			for(IntWritable val: values) {
				teamsBelowAverage += val.get();
			}
			
			//8a emfanisei mono ena pair key-value sto output file ka8ws o CompareMapper 8a steilei polla pairs ALLA ME TO IDIO KLEIDI
			context.write(new Text("Number of teams below average size"), new IntWritable(teamsBelowAverage));
		}
	}
	
	
	
		
		public int run(String[] args) throws Exception {
			//first job for first MapReduce phase
			Job job1 = Job.getInstance(getConf(), "Phase 1");
			job1.setJarByClass(App.class);
			job1.setNumReduceTasks(1);
			job1.setMapperClass(TeamSizeMapper.class);
			job1.setReducerClass(UniqueTeamsReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			Path firstPhaseOutput = new Path(args[1]); //this path so i can use it later in second job
			FileOutputFormat.setOutputPath(job1, firstPhaseOutput);
		
			if (!job1.waitForCompletion(true))
				return 1; //Exit if Job1 fails
			
			//take the total number of teams
			long totalTeams = job1.getCounters().findCounter(UniqueTeamsReducer.MyCounters.TEAM_COUNTER).getValue();
			//take the total size of all teams (all the authors)
			long totalSize = job1.getCounters().findCounter(UniqueTeamsReducer.MyCounters.TOTAL_SIZE).getValue();
			//calculate average size of teams
			double averageTeamSize = (double) totalSize/totalTeams;
			
			//for debugging
			System.out.println("Total teams: " + totalTeams);
			System.out.println("Total authors: " + totalSize);
			
			//put the average size in the configuration so the second mapper can take it for calculation
			getConf().setDouble("average team size",averageTeamSize);
			
			//Second job for second MapReduce phase
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

