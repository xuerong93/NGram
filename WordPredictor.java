import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class WordPredictor {
	
	
	 public static class NgramMapper
     	extends Mapper<Object, Text, Text, Text>{

		 //private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 private Text result = new Text();
		
		 public void map(Object key, Text value, Context context
                  		) throws IOException, InterruptedException {
			 	String content=value.toString();
    			    
			    String[] tuple = content.split("\t");
			    if(content.equals("") || tuple.length != 2) return;
			    
			    tuple[0] = tuple[0].trim();
			    tuple[1] = tuple[1].trim();

			    String[] words = tuple[0].split(" ");
			    if(words.length == 0) return;
			    
			    for(int i=0; i<words.length; i++){
			    	if(words[i].equals("")) continue;
			    	
			    	StringBuilder s = new StringBuilder();
			    	for(int j=0; j<words[i].length()-1; j++){
			    		s.append(words[i].charAt(j));
			    		
			    		word.set(s.toString());
			    		result.set(words[i] + '\t' + '1');
			    		context.write(word, result);
			    	}
			    }
			  
		}
	 }

 
	 public static class NgramReducer 
	 	extends TableReducer<Text, Text, ImmutableBytesWritable>  {
    	 	
    	private Text result = new Text();
    	
    	public void reduce(Text key, Iterable<Text> values,
                    	Context context
                    	) throws IOException, InterruptedException {
    		HashMap<String, Integer> map = new HashMap<String, Integer>();
    		String[] selected = new String[5];
    		int leastIndex = 0;
    		int min = Integer.MIN_VALUE;
    		double total = 0;
    		for(Text t : values){
    			String s = t.toString();
    			String[] parts = s.split("\t");
    			if(parts.length < 2){
    				return;
    			}else {
    				int count = Integer.parseInt(parts[1]);
    				total += count;
    				if(map.containsKey(parts[0])){
    					map.put(parts[0], map.get(parts[0]) + count);
    					count = map.get(parts[0]) + count;
    				}else{
    					map.put(parts[0], count);
    				}
    				if(count > min){
    					selected[leastIndex] = parts[0];
    																
    					if(selected[4] == null){
    						leastIndex ++;			
    					}else{
    						int next = -1;
    						int minTmp = Integer.MAX_VALUE;
    						for(int i=0; i<selected.length; i++){
    							if(selected[i] == null) break;
    							if(map.get(selected[i]) < minTmp){
    								minTmp = map.get(selected[i]);
    								next = i;
    							}
    						}
    						min = minTmp;
    						leastIndex = next;
    					}
    				}
    			}
    		}
    		
    		Put put = new Put(Bytes.toBytes(key.toString()));
    		
    		int num = 0;
    		for(String s : selected){
    			if(s==null || s.length()==0) continue;
    			
    			put.add(Bytes.toBytes("data"), Bytes.toBytes(s), Bytes.toBytes(map.get(s)));
    			num++;
    		}
    		if(num>0){
    			context.write(null, put);
    		}
    	}
    }
    
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration config =  HBaseConfiguration.create();
		 Job  job1 = new Job(config, "wordPredictor");
		 
		 job1.setJarByClass(WordPredictor.class);

		 job1.setMapperClass(NgramMapper.class);
		 job1.setMapOutputKeyClass(Text.class);
		 job1.setMapOutputValueClass(Text.class);
		 
		 //job1.setCombinerClass(NgramCombiner.class);
		 
		 //job1.setReducerClass(NgramReducer.class);	
		 FileInputFormat.addInputPath(job1,new Path(args[0]));
		 //job1.setOutputFormatClass(TextOutputFormat.class);
		 
		 TableMapReduceUtil.initTableReducerJob(
					"wp",      // output table
					NgramReducer.class,             // reducer class
					job1);
		 
	
		 job1.waitForCompletion(true);

	}
  }


