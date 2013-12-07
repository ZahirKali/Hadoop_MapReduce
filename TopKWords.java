import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class TopKWords {
        
public static int Filter = 0;

 public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
    private Text word = new Text();
    private final static LongWritable score = new LongWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            score.set(Long.parseLong(tokenizer.nextToken()));
            context.write(score, word);
        }
    }
 } 
        
 public static class Reduce extends Reducer<LongWritable, Text, Text, LongWritable> {
	 
	  public void reduce(LongWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
		  Text donnee = new Text();
		  
	    	
	    int K =Integer.parseInt( context.getConfiguration().get("K"));
	    	
		  for (Text val : values) {
	        	donnee = new Text(val.toString());
	        }
	        
		 
		  	if (Filter < K)
		  		
		  	{
		        context.write(donnee, key);
		        Filter +=1;
		  	}
       
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("K", args[2]);
        
    Job job = new Job(conf, "topkword");
    
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    System.out.println("Hello world");    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
  
    job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}