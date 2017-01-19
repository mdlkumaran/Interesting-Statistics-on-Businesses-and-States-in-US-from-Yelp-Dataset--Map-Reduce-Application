package finalProject;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public  class ReviewReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		float sum=0.0f;
		int count=0;
		float avg = 0.0f;
		
		for (Text val:values){
			sum=sum+Float.parseFloat(val.toString());
			count=count+1;
		}
		
		avg = sum/(float)count;
		context.write(key, new Text(String.valueOf(avg)));
	}
}