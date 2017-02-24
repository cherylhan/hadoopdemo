package cheryl.dhcc.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cheryl.dhcc.mapreduce.MapReduceReadFile.ReadFileMapper;
import cheryl.dhcc.mapreduce.TwoMapReduce.S_IntSumReducer;
import cheryl.dhcc.mapreduce.TwoMapReduce.S_TokenizerMapper;
import cheryl.dhcc.mapreduce.WordFile.Combiner;
import cheryl.dhcc.mapreduce.WordFile.IntSumReducer;
import cheryl.hadooputil.TransformtoUtf8;

//从sequenceFile 通过多次mapreduce找到一个单词
public class MRSQFAWord {
	private static SequenceFile.Reader reader = null;
	private static Configuration conf = new Configuration();

	public static class ReadFileMapper extends Mapper<LongWritable, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			Text key1 = new Text();
			Text value1 = new Text();
			Text word = new Text();
			Text one = new Text();
			String val;
			try {
				// 从sequence file 读取key value key1为new key 稍后对value进行处理
				while (reader.next(key1, value1)) {
					val = new String(value1.getBytes(), 0, value1.getLength(), "gbk");
					String line = val;
					line = line.replaceAll("[^(0-9\\u4e00-\\u9fa5)]", " ");
					line = line.replaceAll("//s{2,}", " ");
					StringTokenizer itr = new StringTokenizer(line);
					conf = context.getConfiguration();
					while (itr.hasMoreTokens()) {
						String goal = itr.nextToken();
						String getConf = conf.get("key");
						if (goal.equals(getConf)) {
							word.set(goal + "@" + key1);
							one.set("1");
							context.write(word, one);
						}
					}

				}
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			String record = key.toString();
			String[] str = record.split("@");
			key.set(str[0]);
			result.set(str[1] + "*" + Integer.toString(sum));
			context.write(key, result);
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String value = new String();
			for (Text value1 : values) {
				value += value1.toString() + ",";
			}
			result.set(value);
			context.write(key, result);
		}
	}

	public static class S_TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		// 对输入的文件进行处理
		private final static Text one = new Text();
		private Text word = new Text();

		// 索引号 本行文本 输出上下文
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text next = TransformtoUtf8.transformTextToUTF8(value, "GBK");

			String line = next.toString();
			String change = "";
			// 先将整行的文本作为key 这里的分隔符为任意 & 只是随便取了一个
			StringTokenizer itr = new StringTokenizer(line, "&");
			while (itr.hasMoreTokens()) {
				change = itr.nextToken();
				System.out.println(change + "............");
				// 在把key进行切分 制表符前边的为key 后边的为value 到reduce时再对value进行处理
				String[] k = change.split("\t");
				word.set(k[0]);
				one.set(k[1]);
				context.write(word, one);
			}
		}
	}

	public static class S_IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		int sum = 0;
		String value = new String();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value1 : values) {
				value += value1.toString() + ",";
				String[] file = value1.toString().split(",");
				for (int i = 0; i < file.length; i++) {
					String[] count = file[i].split("\\*");
					String number = count[1];
					sum += Integer.parseInt(number);
				}

			}
			result.set(value + " " + Integer.toString(sum));
			context.write(key, result);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("key", "支持点");
		Path path = new Path("hdfs://hadoop:8020/user/dhcc/out10");
		FileSystem fs = FileSystem.get(conf);
		reader = new SequenceFile.Reader(conf, Reader.file(path));
		Job job1 = new Job(conf, "read seq file");
		job1.setJarByClass(MRSQFAWord.class);
		job1.setMapperClass(ReadFileMapper.class);
		job1.setCombinerClass(Combiner.class);
		job1.setReducerClass(IntSumReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputFormatClass(GbkOutputFormat.class);
		

		// 配置job1
		ControlledJob ctrljob1 = new ControlledJob(conf);
		ctrljob1.setJob(job1);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		// job 2
		Job job2 = Job.getInstance(conf, "job2");
		job2.setJarByClass(MRSQFAWord.class);

		job2.setMapperClass(S_TokenizerMapper.class);
		job2.setReducerClass(S_IntSumReducer.class);

		job2.setOutputFormatClass(GbkOutputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// 作业2加入控制容器
		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2);

		// 设置多个作业直接的依赖关系
		// 如下所写：
		// 意思为job2的启动，依赖于job1作业的完成

		ctrljob2.addDependingJob(ctrljob1);

		// 输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
		FileInputFormat.addInputPath(job2, new Path(args[1]));

		// 输出路径从新传入一个参数，这里需要注意，因为我们最后的输出文件一定要是没有出现过得
		// 因此我们在这里new Path(args[2])因为args[2]在上面没有用过，只要和上面不同就可以了
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		// 主的控制容器，控制上面的总的两个子作业
		JobControl jobCtrl = new JobControl("myctrl");

		// 添加到总的JobControl里，进行控制
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);

		// 在线程启动，记住一定要有这个
		Thread t = new Thread(jobCtrl);
		t.start();

		while (true) {

			if (jobCtrl.allFinished()) {// 如果作业成功完成，就打印成功作业的信息
				System.out.println(jobCtrl.getSuccessfulJobList());
				jobCtrl.stop();
				break;
			}
		}

	}
}
