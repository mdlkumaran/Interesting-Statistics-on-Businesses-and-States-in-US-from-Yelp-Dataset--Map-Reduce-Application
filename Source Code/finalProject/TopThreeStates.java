package finalProject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.*;

public class TopThreeStates {

	
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,FloatWritable>{
		
		JobConf conf;
		public void configure(JobConf conf) {
		    this.conf = conf;
		}
		
		public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException{
			
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
					context.write(new Text(categories.getString(i)+":"+state), new FloatWritable(stars));
				}
				
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class Reducer1 extends Reducer<Text,FloatWritable,Text,FloatWritable>{
		public void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException{
			float sum=0.0f;
			int count=0;
			for(FloatWritable val:values){
				sum=sum+Float.parseFloat(val.toString());
				count=count+1;
			}
			
			context.write(new Text(key), new FloatWritable(sum/(float)count));
			//context.write(new Text("hari"),new FloatWritable(0.0f));
		}
		
	}
	
	
public static class Mapper2 extends Mapper<LongWritable,Text,Text,Text>{
		
		JobConf conf;
		public void configure(JobConf conf) {
		    this.conf = conf;
		}
		
		public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException{
			
			String line=value.toString();
//			String input=line.split("\\t")[0];
//			String category=input.split(":")[0];
//			String state=input.split(":")[1];	
//			String rating=line.split("\\t")[1];
//			context.write(new Text(category), new Text(state+":"+rating));
			
			String category=line.split(":")[0];
			context.write(new Text(category), new Text(line.split(":")[1]));
		}
	}

class ValueComparator implements Comparator<String> {
    Map<String, Float> base;

    public ValueComparator(Map<String, Float> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with
    // equals.
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}

	public static class Reducer2 extends Reducer<Text,Text,Text,Text>{
		
		static <K,V extends Comparable<? super V>>  List<Entry<K, V>> entriesSortedByValues(Map<K,V> map) {
			
			List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());
			Collections.sort(sortedEntries, new Comparator<Entry<K,V>>() {
			        @Override
			        public int compare(Entry<K,V> e1, Entry<K,V> e2) {
			            return e2.getValue().compareTo(e1.getValue());
			        }
			    }
			);
			return sortedEntries;
		}
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			HashMap <String,Float>stateRatings=new HashMap<String,Float>();
			for (Text val:values){
				String value=val.toString();
				String k=value.split("\\t")[0];
				Float v=Float.parseFloat(value.split("\\t")[1]);
				stateRatings.put(k,v);
			}
			int count=0;
			StringBuffer value=new StringBuffer();
	        for (Entry<String,Float>e:entriesSortedByValues(stateRatings)){
	        	count=count+1;
	        	if (count>3) break;
	        	value.append(e.getKey()+","+e.getValue()+",");
	        }
	        context.write(new Text(key), new Text(value.substring(0,value.length()-1)));
			
		}
		
	}
	
	
	public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf=new Configuration();
		Job stateRating=new Job(conf,"State Rating");
		stateRating.setJarByClass(TopThreeStates.class); 
	//	FileInputFormat.addInputPath(wordCount, new Path(args[0]));//input1
		//FileOutputFormat.setOutputPath(wordCount,new Path(args[1]));//output1 & input2
		FileInputFormat.addInputPath(stateRating, new Path("/business"));
		FileOutputFormat.setOutputPath(stateRating,new Path("/job1Output"));
		stateRating.setMapperClass(Mapper1.class);
		stateRating.setReducerClass(Reducer1.class);
		stateRating.setMapOutputKeyClass(Text.class);
		stateRating.setMapOutputValueClass(FloatWritable.class);
		stateRating.setOutputKeyClass(Text.class);
		stateRating.setOutputValueClass(FloatWritable.class);
		stateRating.waitForCompletion(true);
		
		conf.set("mapred.textoutputformat.separator", ",");
		Job topThree=new Job(conf,"Top Three States");
		topThree.setJarByClass(TopThreeStates.class); 
	//	FileInputFormat.addInputPath(wordCount, new Path(args[0]));//input1
		//FileOutputFormat.setOutputPath(wordCount,new Path(args[1]));//output1 & input2
		FileInputFormat.addInputPath(topThree, new Path("/job1Output"));
		FileOutputFormat.setOutputPath(topThree,new Path("/topThreeStates"));
		topThree.setMapperClass(Mapper2.class);
		topThree.setReducerClass(Reducer2.class);
		topThree.setMapOutputKeyClass(Text.class);
		topThree.setMapOutputValueClass(Text.class);
		topThree.setOutputKeyClass(Text.class);
		topThree.setOutputValueClass(Text.class);
		topThree.waitForCompletion(true);
		System.exit(topThree.waitForCompletion(true) ? 0 : 1);
		
	}
}
