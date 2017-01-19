package finalProject;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public  class BuisnessAvgUserRatingReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		
		ArrayList<String> categories=new ArrayList<String>();
		String BusinessName = null;
		float RatingSum = 0.0f;
		float RatingAvg = 0.0f;
		int RatingCount = 0;
		
		for(Text val:values){
			char firstLetter = (char) val.charAt(0);
			
			if(firstLetter=='*'){
				categories.add(val.toString().substring(1));
			}else if(firstLetter =='#'){
				BusinessName = val.toString().substring(1);
			}else{
				RatingSum = RatingSum + Float.parseFloat(val.toString());
				RatingCount++;
			}
		}
		
		RatingAvg = RatingSum/RatingCount;
		
		if(RatingCount>0)
		{
			for(String cat:categories)
			{
				context.write(new Text(cat), new Text(key.toString() +"%%"+ BusinessName +"##"+String.valueOf(RatingAvg)));
			}
		}
	}
}