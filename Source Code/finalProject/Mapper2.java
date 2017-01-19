package finalProject;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public  class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
	JobConf conf;
	
	public void configure(JobConf conf) {
	    this.conf = conf;
	}
	
	public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
		String line=value.toString();
		String Category = line.split("\\t")[0];
		String ValueString = line.split("\\t")[1];
		
		context.write(new Text(Category), new Text(ValueString));
	}
}