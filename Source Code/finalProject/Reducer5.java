package finalProject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer5  extends Reducer<Text, Text, Text, Text>{
	Map<String, String> bus=new HashMap<String,String>();
	Map<String, ArrayList<String>> busCategory=new HashMap<String,ArrayList<String>>();
	private MultipleOutputs<Text, Text> multipleOutputs;
	String filename = null;
	

	protected void setup(Context context) throws IOException{
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
		try{
			Path[] localFiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			for(Path eachPath:localFiles){
				filename=eachPath.getName().toString().trim();
				if(filename.equals("Bus-r-00000")){
					initBus(new File("Bus-r-00000"));
				}
				if(filename.equals("BusCategory-r-00000")){
					initBusCategory(new File("BusCategory-r-00000"));
				}
			}
		}
		catch(NullPointerException e){
			System.out.println("Exception :"+e);
		}
	}
	
	public void initBus(File fs) throws IOException
	{
		String line;
		FileReader fr = new FileReader(fs);
		BufferedReader buff = new BufferedReader(fr);
		while((line = buff.readLine()) != null)
		{
			String s[] =line.split("\t");
			String key = s[0];
			String value = s[1];
				if (key !=" " && value != " "){
					bus.put(key, value);
				}	
		}		
	}
	
	public void initBusCategory(File fs) throws IOException
	{
		String line;
		FileReader fr = new FileReader(fs);
		BufferedReader buff = new BufferedReader(fr);
		while((line = buff.readLine()) != null)
		{
			String s[] =line.split("\t");
			String key = s[0];
			String value = s[1];
				if (key !=" " && value != " "){

					ArrayList<String> cat=null;
					try{
						cat=busCategory.get(key);
						cat.add(value);
						busCategory.put(key,cat);
					}
					catch(NullPointerException e){
						cat=new ArrayList<String>();
						cat.add(value);
						busCategory.put(key,cat);
					}
					
				}	
		}		
	}
	
	
	@Override
	public void cleanup(final Context context) throws IOException, InterruptedException{
		multipleOutputs.close();
	}
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuffer yearWiseRating=new StringBuffer();
			for(Text val:values){
				yearWiseRating.append(val.toString()+",");
			}
			String yWRating=yearWiseRating.toString().substring(0,yearWiseRating.length()-1);
			String bName=bus.get(key.toString());
			ArrayList<String>cat=busCategory.get(key.toString());
			try{
				for (String c :cat){
					multipleOutputs.write(new Text(bName),new Text(yWRating), c);
				}
			}
			catch(NullPointerException e){
				System.out.println("Exception :"+e);
			}
			 
		}

}
