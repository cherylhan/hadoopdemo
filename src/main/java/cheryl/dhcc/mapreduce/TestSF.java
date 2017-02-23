package cheryl.dhcc.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;

import cheryl.hadooputil.TransformtoUtf8;

import org.apache.hadoop.io.Text;

//Sequence File 将多个小文件合并成一个大文件
public class TestSF {

	public static void main(String[] args) throws IOException, Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:8020"), conf);
		
		 //输入路径：文件夹
		 FileStatus[] files = fs.listStatus(new Path(args[0]));
		
		 Text key = new Text();
		 Text value = new Text();
		
		 //输出路径：文件
		 SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new
		 Path(args[1]),key.getClass() , value.getClass());
		 InputStream in = null;
		 byte[] buffer = null;
		 for(int i=0;i<files.length;i++){
		 key.set(files[i].getPath().getName());
		 in = fs.open(files[i].getPath());
		 buffer = new byte[(int) files[i].getLen()];
		 IOUtils.readFully(in, buffer, 0, buffer.length);
		 value.set(buffer);
		 IOUtils.closeStream(in);
		 writer.append(key,value);
		 }
		
		 IOUtils.closeStream(writer);
		 }

	}
/*
 * 读取seq文件里的内容 Path seqFile = new Path("hdfs://hadoop:8020/user/dhcc/out10");
 * SequenceFile.Reader reader = new SequenceFile.Reader(conf,
 * Reader.file(seqFile)); Text key = new Text(); Text value = new Text(); while
 * (reader.next(key, value)) {
 * System.out.println("??????????????"+key+"???????????????????");
 * System.out.println(value); } IOUtils.closeStream(reader);// 关闭read流 }
 * 
 */
