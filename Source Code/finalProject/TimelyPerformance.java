package finalProject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class TimelyPerformance extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 
		 Job job0 = new Job(conf, "ReviewerIdFilter");
		 job0.setJarByClass(TimelyPerformance.class);
		 LazyOutputFormat.setOutputFormatClass(job0, TextOutputFormat.class);
		 MultipleOutputs.addNamedOutput(job0, "text", TextOutputFormat.class,Text.class, Text.class);
		 FileInputFormat.addInputPath(job0, new Path("/user"));
		 FileOutputFormat.setOutputPath(job0,new Path("/userid"));
		 job0.setMapperClass(ReviewerIDFilterMapper.class);
		 job0.setReducerClass(ReviewerIDFilterReducer.class);
		 job0.setMapOutputKeyClass(Text.class);
		 job0.setMapOutputValueClass(NullWritable.class);
		 job0.setOutputKeyClass(Text.class);
		 job0.setOutputValueClass(NullWritable.class);	 
		 job0.waitForCompletion(true);
		 
		 Job job = new Job(conf, "MulMapper");
		 job.setJarByClass(TimelyPerformance.class);
		 DistributedCache.addCacheFile (new Path("/userid/validID-r-00000").toUri(), job.getConfiguration());
		 MultipleInputs.addInputPath(job,new Path("/business"),TextInputFormat.class,BusinessCatMapper.class);
		 MultipleInputs.addInputPath(job,new Path("/review"),TextInputFormat.class,BusinessUserRatingMapper.class);
		 
		 FileOutputFormat.setOutputPath(job, new Path("/TimelyPerformance"));
		 job.setReducerClass(BuisnessAvgUserRatingReducer.class);
		 job.setNumReduceTasks(1);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 job.waitForCompletion(true);
		 
		 Job job2 = new Job(conf, "CatRating");
		 job2.setJarByClass(TimelyPerformance.class);
		 LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
		 MultipleOutputs.addNamedOutput(job2, "text", TextOutputFormat.class,Text.class, Text.class);
		 FileInputFormat.addInputPath(job2, new Path("/TimelyPerformance"));
		 FileOutputFormat.setOutputPath(job2,new Path("/refer"));
		 job2.setMapperClass(Mapper2.class);
		 job2.setReducerClass(Reducer2.class);
		 job2.setMapOutputKeyClass(Text.class);
		 job2.setMapOutputValueClass(Text.class);
		 job2.setOutputKeyClass(Text.class);
		 job2.setOutputValueClass(Text.class);	 
		 job2.waitForCompletion(true);
		 
		 
		 Job job3 = new Job(conf, "UserRating");
		 job3.setJarByClass(TimelyPerformance.class);
		 DistributedCache.addCacheFile (new Path("/userid/validID-r-00000").toUri(), job3.getConfiguration());
		 FileInputFormat.addInputPath(job3, new Path("/review"));
		 FileOutputFormat.setOutputPath(job3,new Path("/reviewOutput"));
		 job3.setMapperClass(ReviewMapper.class);
		 job3.setReducerClass(ReviewReducer.class);
		 job3.setMapOutputKeyClass(Text.class);
		 job3.setMapOutputValueClass(Text.class);
		 job3.setOutputKeyClass(Text.class);
		 job3.setOutputValueClass(Text.class);
		 job3.waitForCompletion(true);
//		 
		 conf.set("mapred.textoutputformat.separator", ",");
		 Job job4 = new Job(conf, "Consolidate Output");
		 job4.setJarByClass(TimelyPerformance.class);
		 LazyOutputFormat.setOutputFormatClass(job4, TextOutputFormat.class);
		 MultipleOutputs.addNamedOutput(job4, "text", TextOutputFormat.class,Text.class, Text.class);
		 DistributedCache.addCacheFile (new Path("/refer/BusCategory-r-00000").toUri(), job4.getConfiguration());
		 DistributedCache.addCacheFile (new Path("/refer/Bus-r-00000").toUri(), job4.getConfiguration());
		 FileInputFormat.addInputPath(job4, new Path("/reviewOutput"));
		 FileOutputFormat.setOutputPath(job4,new Path("/output"));
		 job4.setMapperClass(Mapper5.class);
		 job4.setReducerClass(Reducer5.class);
		 job4.setMapOutputKeyClass(Text.class);
		 job4.setMapOutputValueClass(Text.class);
		 job4.setOutputKeyClass(Text.class);
		 job4.setOutputValueClass(Text.class);	 
		 job4.waitForCompletion(true);
		 
		 return (job4.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		  int ecode = ToolRunner.run(new TimelyPerformance(), args);
		  System.exit(ecode);
		 }
}
