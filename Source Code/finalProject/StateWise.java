package finalProject;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class StateWise {
	
	public static class StateCatStarMapper extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		JobConf conf;
		public void configure(JobConf conf) {
		    this.conf = conf;
		}
		
		public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
			String line=value.toString();
			JSONObject jobj = null;
			JSONArray categories=null;
			String state=null;
			float stars=0.0f;
			
			try {
				jobj = new JSONObject(line);
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			try {
				categories=jobj.getJSONArray("categories");
				state=jobj.getString("state");
				stars=(float)jobj.getDouble("stars");
				for (int i=0;i <categories.length();i++){
					context.write(new Text(state+",\""+categories.getString(i)+"\""), new FloatWritable(stars));
				}
				
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
	}

	public static class StateCatStarReducer extends Reducer<Text,FloatWritable,Text,Text>{
		public void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException{
			float sum=0.0f;
			int count=0;
			float avg = 0.0f;
			
			for(FloatWritable val:values){
				sum=sum+Float.parseFloat(val.toString());
				count=count+1;
			}
			
			avg = sum/(float)count;
			context.write(new Text(key), new Text(String.valueOf(avg)));
		}	
	}
	
	public static class StateCatStarMapper2 extends Mapper<LongWritable,Text,Text,Text>
	{
		JobConf conf;
		public void configure(JobConf conf) {
		    this.conf = conf;
		}
		
		public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
			String line=value.toString();
			String CatState = line.split("\\t")[0];
			String State = CatState.split(",")[0];
			String category = CatState.split(",")[1];
			String AvgStar = line.split("\\t")[1];
			
			context.write(new Text(State), new Text(category +","+ AvgStar));
		}
	}
	
	class ValueComparator implements Comparator<String>{
		Map<String,Float> base;
		
		public ValueComparator(Map<String,Float>base){
			this.base = base;
		}

		@Override
		public int compare(String a, String b) {
			// TODO Auto-generated method stub
			if(base.get(a)>=base.get(b)){
				return -1;
			}else{
				return 1;
			}
		}
	}
	
	public static class StateCatStarReducer2 extends Reducer<Text,Text,Text,Text>{
		static<K,V extends Comparable<? super V>> List<Entry<K,V>> entriesSortedByValues(Map<K,V> map){
			List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());
			Collections.sort(sortedEntries,new Comparator<Entry<K,V>>(){

				@Override
				public int compare(Entry<K, V> e1, Entry<K, V> e2) {
					// TODO Auto-generated method stub
					return e2.getValue().compareTo(e1.getValue());
				}
			  }
			);
			return sortedEntries;
		}
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			HashMap <String,Float>hRatings=new HashMap<String,Float>();
			
			for (Text val:values){
				String sValues = val.toString();
				String k=sValues.split(",")[0];
				Float v=Float.parseFloat(sValues.split(",")[1]);
				hRatings.put(k,v);
			}
			
			int count=0;
			StringBuffer value=new StringBuffer();
			for (Entry<String,Float>e:entriesSortedByValues(hRatings)){
	        	count=count+1;
	        	if (count>10) break;
	        	value.append(e.getKey()+","+e.getValue()+",");
	        }
			context.write(new Text(key), new Text(value.substring(0,value.length()-1)));
		}
	}
	
	public static void setTextoutputformatSeparator(final Job job, final String separator){
        final Configuration conf = job.getConfiguration(); //ensure accurate config ref

        conf.set("mapred.textoutputformat.separator", separator); //Prior to Hadoop 2 (YARN)
        conf.set("mapreduce.textoutputformat.separator", separator);  //Hadoop v2+ (YARN)
        conf.set("mapreduce.output.textoutputformat.separator", separator);
        conf.set("mapreduce.output.key.field.separator", separator);
        conf.set("mapred.textoutputformat.separatorText", separator); // ?
}
	public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf=new Configuration();
		
		Job stateRating=new Job(conf,"StateCategoryStar");
		stateRating.setJarByClass(StateWise.class); 
		FileInputFormat.addInputPath(stateRating, new Path("/business"));
		FileOutputFormat.setOutputPath(stateRating,new Path("/t2output1"));
		stateRating.setMapperClass(StateCatStarMapper.class);
		stateRating.setReducerClass(StateCatStarReducer.class);
		stateRating.setMapOutputKeyClass(Text.class);
		stateRating.setMapOutputValueClass(FloatWritable.class);
		stateRating.setOutputKeyClass(Text.class);
		stateRating.setOutputValueClass(Text.class);
		//setTextoutputformatSeparator(stateRating,",");
		//conf.set("mapred.textoutputformat.separatortext", ",");
		stateRating.waitForCompletion(true);
		
		Job stateRating2 = new Job(conf,"StateCategoryRating");
		stateRating2.setJarByClass(StateWise.class); 
		FileInputFormat.addInputPath(stateRating2, new Path("/t2output1"));
		FileOutputFormat.setOutputPath(stateRating2,new Path("/t2output2"));
		stateRating2.setMapperClass(StateCatStarMapper2.class);
		stateRating2.setReducerClass(StateCatStarReducer2.class);
		stateRating2.setMapOutputKeyClass(Text.class);
		stateRating2.setMapOutputValueClass(Text.class);
		stateRating2.setOutputKeyClass(Text.class);
		stateRating2.setOutputValueClass(TextOutputFormat.class);
		setTextoutputformatSeparator(stateRating2,",");
		//conf.set("mapred.textoutputformat.separatortext", ",");
		stateRating2.waitForCompletion(true);
	}
}
