package cheryl.dhcc.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cheryl.hadoop.WordCount;
import cheryl.hadooputil.TransformtoUtf8;

//join 将两个文件里的数据进行join操作
//a文件				b文件
//id name city		id salary
//1  hc    bj			1	2000
//结合后为
//id name city salary
//1 hc bj 2000
public class MRJoin {
	// 实现分割符 分隔符为空格
	public static final String DELIMITER = "\t";

	// map 过程
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private FileSplit split;// 目的是获取当前文件lujing
		private String dirName;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 获取输入文件的路径和名称
			Text next = TransformtoUtf8.transformTextToUTF8(value, "GBK");
			split = (FileSplit) context.getInputSplit();
			dirName = split.getPath().toString();
			String line = next.toString();
			// 处理来自A的记录
			if (dirName.contains("part-r")) {
				// 安分隔符划分
				String[] values = line.split(DELIMITER);
				if (values.length < 2)
					return;
				String newkey = values[0];
				String pathInfo = values[1];
				context.write(new Text(newkey), new Text(new Text("a#" + pathInfo)));
			}

			if (dirName.contains("part-m")) {
				String[] values = line.split(DELIMITER);
				if (values.length < 2)
					return;
				String newKey = values[0];
				String count = values[1];
				context.write(new Text(newKey), new Text(new Text("b#" + count)));
			}
		}

	}

	// reduce过程 将文件存储为A、B、两部分
	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Vector<String> A = new Vector<String>();// 存放来自A的数据
			Vector<String> B = new Vector<String>();// 存放来自B的数据

			for (Text value1 : values) {
				String value = value1.toString();
				if (value.startsWith("a#")) {
					A.add(value.substring(2));
				} else if (value.startsWith("b#")) {
					B.add(value.substring(2));
				}
			}
			int sizeA = A.size();
			int sizeB = B.size();

			// 遍历两个向量
			int i, j;
			for (i = 0; i < sizeA; i++) {
				for (j = 0; j < sizeB; j++) {
					context.write(key, new Text(A.get(i) + DELIMITER + B.get(j)));
				}
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MRJoin.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputFormatClass(GbkOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
