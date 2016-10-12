import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class Aggregate {
	
	public boolean isAlphabet(char c){
		return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
	}

	 public static class NgramMapper
     	extends Mapper<Object, Text, Text, IntWritable>{

		 //private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 private final static IntWritable one = new IntWritable(1);

 
		 public void map(Object key, Text value, Context context
                  		) throws IOException, InterruptedException {
			 	String content=value.toString();
    			    
			    String[] array = content.split("[^a-zA-Z]");
			    
			    ArrayList<String> list = new ArrayList<String>();
			    
			    for(String s:array){
			    	if(s.equals("")) continue;
			    	list.add(s.toLowerCase());
			    }
			    
			    for(int i=0; i<list.size(); i++){
			    	String ngram = "";
			    	for(int n=0; n<5; n++){
			    		if(n+i >= list.size()) break;
			    		ngram += (list.get(n+i) + " ");
			    		word.set( ngram.substring(0, ngram.length()-1) );
			    		context.write(word,  one);
			    	}
			    }   
		}
	 }

	 public static class NgramCombiner
	 	extends Reducer<Text,IntWritable,Text,IntWritable> {
	 	
	 	private IntWritable result = new IntWritable();
	 	
	 	public void reduce(Text key, Iterable<IntWritable> values,
	                 	Context context
	                 	) throws IOException, InterruptedException {
	 		int sum=0;
	 		for(IntWritable val : values){
	 			sum += val.get();
	 		}
	 		result.set(sum);
	 		context.write(key, result);
	 	}
	 }
    public static class NgramReducer
    	extends Reducer<Text,IntWritable,Text,IntWritable> {
    	
    	private IntWritable result = new IntWritable();
    	
    	public void reduce(Text key, Iterable<IntWritable> values,
                    	Context context
                    	) throws IOException, InterruptedException {
    		int sum=0;
    		for(IntWritable val : values){
    			sum += val.get();
    		}
    		if (sum <= 2) return;
    		result.set(sum);
    		context.write(key, result);
    	}
    }
    
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf = new Configuration();
		 Job  job1 = new Job(conf, "Ngram");
		 job1.setJarByClass(Aggregate.class);
		 job1.setMapperClass(Aggregate.NgramMapper.class);
		 job1.setCombinerClass(Aggregate.NgramCombiner.class);
		 job1.setReducerClass(Aggregate.NgramReducer.class);

		 job1.setOutputKeyClass(Text.class);
		 job1.setOutputValueClass(IntWritable.class);
		 FileInputFormat.addInputPath(job1,new Path(args[0]));
		 FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		 job1.waitForCompletion(true);

    	
	}
  }


