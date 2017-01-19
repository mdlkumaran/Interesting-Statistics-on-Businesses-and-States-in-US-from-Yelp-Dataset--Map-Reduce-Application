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

public  class BusinessUserRatingMapper extends Mapper<LongWritable, Text, Text, Text>{
	JobConf conf;
	Map<String,Boolean> validIDs=new HashMap<String,Boolean>();
	String filename = null;
	public void configure(JobConf conf) {
	    this.conf = conf;
	}
	
	protected void setup(Context context) throws IOException{
		try{
			Path[] localFiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path eachPath:localFiles){
				filename=eachPath.getName().toString().trim();
				if(filename.equals("validID-r-00000")){
					initValidIds(new File("validID-r-00000"));
					break;
					
				}
			}
		}
		catch(NullPointerException e){
			System.out.println("Exception :"+e);
		}
	}
	
	public void initValidIds(File fs) throws IOException
	{
		String line;
		FileReader fr = new FileReader(fs);
		BufferedReader buff = new BufferedReader(fr);
		while((line = buff.readLine()) != null)
		{
			validIDs.put(line,true);
		}		
	}
	
	public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
		String line=value.toString();
		JSONObject jobj = null;
		String businessId = null;
		Float stars=0.0f;
		String UserId = null;
		
		try {
			jobj = new JSONObject(line);
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			businessId = jobj.getString("business_id");
			stars = (float)jobj.getDouble("stars");
			UserId = jobj.getString("user_id");
			
			try{	if(validIDs.get(UserId)){
					context.write(new Text(businessId), new Text(String.valueOf(stars)));
				}
			}
			catch(Exception e){
				System.out.println("Exception ID not found: "+e);
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
