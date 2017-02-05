package cheryl.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import cheryl.hadoop.WordCount.TokenizerMapper;
//计数器 统计日志中错误的条数
public class MyCounter{
	public static class MyCountMap  extends Mapper<LongWritable,Text,Text,Text> {
	public static Counter ct=null;
	protected void map(LongWritable key,Text value,Mapper<LongWritable,Text,Text,Text>.Context context){
		String arr_value[]=value.toString().split("/t");
		if(arr_value.length<4){
			ct=(Counter) context.getCounter("ErrorCount", "ERROR_LOGTIME");
			ct.increment(1);
		}
	}
	public static void main(String[] args) throws IOException, Exception, InterruptedException {
		Configuration conf=new Configuration();
		String[] otherargs=new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job = Job.getInstance(conf, "counter");
	    job.setJarByClass(MyCounter.class);
	    job.setMapperClass(MyCountMap.class);
		FileInputFormat.addInputPath(job, new Path(otherargs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
  }
}
