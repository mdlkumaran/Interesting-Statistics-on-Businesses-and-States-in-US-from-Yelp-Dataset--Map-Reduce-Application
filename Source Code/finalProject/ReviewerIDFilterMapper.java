package finalProject;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.JSONException;
import org.json.JSONObject;


public  class ReviewerIDFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	JobConf conf;
	
	public void configure(JobConf conf) {
	    this.conf = conf;
	}

	public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
		String line=value.toString();
		String UserId = null;
		JSONObject jobj = null;
		int useful = 0;
		int reviewCnt = 0;
		float Stars = 0.0f;
		
		try {
			jobj = new JSONObject(line);
		}catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			JSONObject temp=jobj.getJSONObject("votes");
			useful = temp.getInt("useful");
			reviewCnt = jobj.getInt("review_count");
			Stars = (float)jobj.getDouble("average_stars");
			UserId = jobj.getString("user_id");
			
			if(useful>0 && reviewCnt>1 && Stars>1 && Stars<5){
			context.write(new Text(UserId), NullWritable.get());
			}
		}catch(JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}