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

public class Predictor {
	
	
	 public static class NgramMapper
     	extends Mapper<Object, Text, Text, Text>{

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
			    
			    String outputKey=words[0];
			    for(int i=1; i<words.length-1; i++){
			    	if(words[i].equals("")) continue;
			    	outputKey += (" "+words[i]);
			    }
			    String outVal = null;
			    if(words.length > 1){
			    	outVal = words[words.length-1] + '\t' + tuple[1];
			    	word.set(outputKey);
				    result.set(outVal);
				    
				    context.write(word,result);
				    
			    }else{
			    	outVal = tuple[1];
			    }
			    
			    word.set(tuple[0]);
			    result.set(tuple[1]);
			    
			    context.write(word, result);
		}
	 }

	
	 public static class NgramReducer 
	 	extends TableReducer<Text, Text, ImmutableBytesWritable>  {

    	
    	class Item implements Comparable<Item> {
    		private int count;
    		private String word;
    		private double prob;
    		
    		public Item(String text, int count){
    			this.word = text;
    			this.count = count;
    			this.prob = -1;
    		}
    		public void setProb(int total){
    			this.prob = ((double)count)/(double)total;
    		}
    		public double getProb(){
    			return prob;
    		}
    		public String getWord(){
    			return word;
    		}
			@Override
			public int compareTo(Item o) {
				// TODO Auto-generated method stub
				if(prob == o.getProb()){
					return word.compareTo(o.getWord());
				}else{
					return (o.getProb() - prob)>0 ? 1 : -1;
				}
				
			}
			public String getCount(){
				return Integer.toString(count);
			}
			public String getStringProb(){
				return Double.toString(prob);
			}
    	}
    	
    	
    	
		private Text result = new Text();

    	public void reduce(Text key, Iterable<Text> values,
                    	Context context
                    	) throws IOException, InterruptedException {
        	List<Item> tmp = new ArrayList<Item>();
    		int total = 0;
    		for(Text t : values){
    			String s = t.toString();
    			String[] parts = s.split("\t");
    			if(parts.length == 1){
    				total = Integer.parseInt(parts[0]);
    			}else if(parts.length > 1){
    				String lastWord = parts[0].trim();
    				//String txt = parts[0] + " " + lastWord;
    				int count = Integer.parseInt(parts[1]);
    				tmp.add(new Item(lastWord, count));
    			}
    		}
    		if(total == 0) return;
    		
    		
    		for(Item it : tmp){
    			it.setProb(total);
    			//queue.offer(it);
    		}
    		Collections.sort(tmp);
    		
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			int count=0;
    		for(int i=0; i<5 && i<tmp.size(); i++){
    			
    			Item it = tmp.get(i);
    			put.add(Bytes.toBytes("data"), Bytes.toBytes(it.getWord()), Bytes.toBytes(it.getStringProb()));
    			count++;
    		}
    		if(count > 0) {
    			context.write(null, put);
    		}
    	}
    }
    
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration config =  HBaseConfiguration.create();
		 Job  job1 = new Job(config, "ngramPredictor");
		 
		 job1.setJarByClass(Predictor.class);

		 job1.setMapperClass(NgramMapper.class);
		 job1.setMapOutputKeyClass(Text.class);
		 job1.setMapOutputValueClass(Text.class);
		 
		 FileInputFormat.addInputPath(job1,new Path(args[0]));
		 
		 TableMapReduceUtil.initTableReducerJob(
					"predictor",      // output table
					NgramReducer.class,             // reducer class
					job1);
		 
	
		 job1.waitForCompletion(true);

	}
  }


