package cheryl.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cheryl.dhcc.mapreduce.GbkOutputFormat;
import cheryl.hadooputil.TransformtoUtf8;

public class WordCount {
//单词统计
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
//对输入的文件进行处理
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private FileSplit split;
    private String dirName;
//索引号 本行文本 输出上下文
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	Text next=TransformtoUtf8.transformTextToUTF8(value,"GBK");
    	split = (FileSplit)context.getInputSplit();
    	dirName=split.getPath().toString();
    	String line=next.toString();
    	line=line.replaceAll("[^(0-9\\u4e00-\\u9fa5)]"," ");
    	line=line.replaceAll("//s{2,}"," ");
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken()+"::"+dirName);
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputFormatClass(GbkOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}