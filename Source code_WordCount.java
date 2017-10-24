import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class WordCountMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text DocId = new Text();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {    
      String col[]=value.toString().split("\t");    
      DocId.set(col[0]);
	  StringTokenizer itr = new StringTokenizer(col[1]);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, DocId);
      }
    }
  }

  public static class WordCountReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
	private HashMap<String, Integer> map;
	
	private void add(String tag) {
	     Integer val;
	
	     if (map.get(tag) != null) {
	       val = map.get(tag);
	       map.remove(tag);
	     } else {
	       val = 0;
	     }
	
	     map.put(tag, val+1);
	 }

    public void reduce(Text key, Iterable<Text> values,
	                Context context
	                ) throws IOException, InterruptedException {
		
	map = new HashMap<String, Integer>();
	for (Text val : values) {
	add(val.toString());
	}
	////res=to_String
	String outval = "";
	
	for(String tag : map.keySet()){        	                                   
	   outval += (tag+":"+map.get(tag).toString()+" ");
	}
	
	result.set(outval);
	
	context.write(key, result);
	}
	
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}