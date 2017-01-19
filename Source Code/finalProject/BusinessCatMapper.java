package finalProject;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public  class BusinessCatMapper extends Mapper<LongWritable, Text, Text, Text>{
	JobConf conf;
	
	public void configure(JobConf conf) {
	    this.conf = conf;
	}
	
	public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
		String line=value.toString();
		JSONObject jobj = null;
		JSONArray categories=null;
		String Name=null;
		String businessId = null;
		
		try {
			jobj = new JSONObject(line);
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {

			categories=jobj.getJSONArray("categories");
			Name=jobj.getString("name");
			businessId = jobj.getString("business_id");
			
			for (int i=0;i <categories.length();i++){
				context.write(new Text(businessId), new Text("*"+categories.getString(i)));
			}
			
			context.write(new Text(businessId), new Text("#"+Name));
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
}