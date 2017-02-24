package cheryl.dhcc.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class MapReduceReadFile {  
    
    private static SequenceFile.Reader reader = null;  
    private static Configuration conf = new Configuration();  
  
    public static class ReadFileMapper extends  
            Mapper<LongWritable, Text, Text, Text> {  
  
        /* (non-Javadoc) 
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context) 
         */  
        @Override  
        public void map(LongWritable key, Text value,Context context) throws IOException {  
        	Text key1 = new Text();
    		Text value1 = new Text();  
    		String val;
            try { 
            	//从sequence file 读取key value key1为new key 稍后对value进行处理
                while (reader.next(key1,value1)) {  
                	val = new String(value1.getBytes(), 0, value1.getLength(), "gbk");
                	value1.set(val);
                	context.write(key1, value1);  
                }  
            } catch (IOException e1) {  
                e1.printStackTrace();  
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }  
        }  
  
    }

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Path path = new Path("hdfs://hadoop:8020/user/dhcc/out10");
		FileSystem fs = FileSystem.get(conf);
		reader = new SequenceFile.Reader(conf, Reader.file(path));
		Job job = new Job(conf, "read seq file");
		job.setJarByClass(MapReduceReadFile.class);
		job.setMapperClass(ReadFileMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class); 
		job.setOutputFormatClass(GbkOutputFormat.class);
		FileInputFormat.addInputPath(job, path);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
