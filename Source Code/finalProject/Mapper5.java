package finalProject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.JSONException;
import org.json.JSONObject;


public  class Mapper5 extends Mapper<LongWritable, Text, Text, Text>{
	JobConf conf;
	public void configure(JobConf conf) {
	    this.conf = conf;
	}
	
	
	public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
		
		String line=value.toString();
		String rating=line.split("\t")[1];
		String temp=line.split("\t")[0];
		String[] bidyear=temp.split(",");
		context.write(new Text(bidyear[0]), new Text(bidyear[1]+","+rating));
		
	}
}