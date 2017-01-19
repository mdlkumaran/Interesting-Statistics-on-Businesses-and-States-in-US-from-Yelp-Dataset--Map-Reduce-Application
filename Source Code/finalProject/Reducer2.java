package finalProject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public  class Reducer2 extends Reducer<Text, Text, Text, Text>{
	private MultipleOutputs<Text, Text> multipleOutputs;
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
	@Override
	public void setup(Context context){
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	public void cleanup(final Context context) throws IOException, InterruptedException{
		multipleOutputs.close();
	}
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		HashMap <String,Float>hRatings=new HashMap<String,Float>();
		
		for (Text val:values){
			String sValues = val.toString();
			String k=sValues.split("##")[0];
			Float v=Float.parseFloat(sValues.split("##")[1]);
			hRatings.put(k,v);
		}
		
		int count=0;
	//	StringBuffer value=new StringBuffer();
        for (Entry<String,Float>e:entriesSortedByValues(hRatings)){
        	count=count+1;
        	if (count>5) break;
        	String bid=e.getKey().split("%%")[0];
        	String bname=e.getKey().split("%%")[1];
        	multipleOutputs.write(new Text(bid),new Text(bname), "Bus");
        	multipleOutputs.write(new Text(bid),key, "BusCategory");
        	//multipleOutputs.write(key,new Text(bid), "CatBusiness");
        	//value.append(e.getKey());
        }
        
       
        
        
        
        //context.write(new Text(key), new Text(value.substring(0,value.length()-1)));
	}
}
