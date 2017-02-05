package cheryl.dhcc.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output. FileOutputFormat;

// 统计 /user/dhcc/medical_data下的文件中单词的词频 
public class DhMapreduce {
	//因为hadoop默认编码为utf-8,所以在处理gbk时需要转码
	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
		value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
		e.printStackTrace();
		}
		return new Text(value);
		}
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		//输入索引和要处理的文本，输出文本和出现次数
		private final static IntWritable One = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//读出字符串  hadoop默认使用utf8 先将gbk统一成utf-8处理
			Text next=transformTextToUTF8(value,"GBK");
			String line=next.toString();
            //将非字母和汉字以及一些特殊符号（. - / : ℃）外的符号替换成空格
            line=line.replaceAll("[^(0-9\\u4e00-\\u9fa5)]"," ");
            //去除多余的空格 只保留一个空格
            line=line.replaceAll("//s{2,}"," ");
            //对本行文件作分割处理
            StringTokenizer itr=new StringTokenizer(line);
            while(itr.hasMoreTokens()){
            	word.set(itr.nextToken());
            	context.write(word, One);
            }
		}
	}
public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	private final static IntWritable result=new IntWritable(0);
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
		int sum=0;
		for(IntWritable val:values){
			sum+=val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(DhMapreduce.class);
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
