package finalProject;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public  class ReviewerIDFilterReducer extends Reducer<Text, Text, Text, NullWritable>{
	private MultipleOutputs<Text, NullWritable> multipleOutputs;
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		//for (Text val:values){
			//context.write(new Text(key), NullWritable.get());
		//}
		multipleOutputs.write(key,NullWritable.get(), "validID");
		
	}
	
	@Override
	public void setup(Context context){
		multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
	}
	
	@Override
	public void cleanup(final Context context) throws IOException, InterruptedException{
		multipleOutputs.close();
	}
}
