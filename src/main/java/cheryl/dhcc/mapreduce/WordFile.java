package cheryl.dhcc.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cheryl.dhcc.mapreduce.InvertedIndex.Combiner;
import cheryl.dhcc.mapreduce.InvertedIndex.IntSumReducer;
import cheryl.dhcc.mapreduce.InvertedIndex.TokenizerMapper;
import cheryl.hadoop.WordCount;
import cheryl.hadooputil.TransformtoUtf8;

/*查找某个单词
 * 1.在哪些文件中出现 
 * 2.在每一个文件中出现的次数
 * */
public class WordFile {
	// 单词统计 输出的类型为text
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		// 对输入的文件进行处理
		private final static Text one = new Text();
		private Text word = new Text();
		private FileSplit split;//目的是获取当前文件lujing
		private String dirName;
		Configuration conf=null;
		// 索引号 本行文本 输出上下文
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text next = TransformtoUtf8.transformTextToUTF8(value, "GBK");
			split = (FileSplit) context.getInputSplit();
			dirName = split.getPath().toString();
			String line = next.toString();
			line = line.replaceAll("[^(0-9\\u4e00-\\u9fa5)]", " ");
			line = line.replaceAll("//s{2,}", " ");
			conf=context.getConfiguration();
			String find=conf.get("key");
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String goal=itr.nextToken() ;
				if(goal.equals(find)){
				word.set(goal+ "@" + dirName);
				one.set("1");
				context.write(word, one);
				}
			}
		}
	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum=0;
			for (Text val : values) {
		     sum += Integer.parseInt(val.toString());
			}
			String record=key.toString();
			String[] str=record.split("@");
			key.set(str[0]);
			result.set(str[1]+"*"+Integer.toString(sum));
			context.write(key, result);
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 String value =new String();
	            for(Text value1:values){
	                value += value1.toString()+",";
	            }
	            result.set(value);
	            context.write(key,result);
	        }
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("key", "支持点");
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordFile.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputFormatClass(GbkOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	} 
}
