package cheryl.dhcc.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import cheryl.hadooputil.TransformtoUtf8;

import org.apache.hadoop.io.Text;

public class ReadSeqFile {
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:8020"), conf);
		Path seqFile = new Path("hdfs://hadoop:8020/user/dhcc/out10");
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seqFile));
		Text key = new Text();
		Text value = new Text();
		while(reader.next(key,value))
		{	//因为开始gbk文件没处理过 所以按照默认编码了 现在再用gbk解码就得到了原内容
			String val = new String(value.getBytes(), 0, value.getLength(), "gbk");
			System.out.println("??????????????" + key + "???????????????????");
			System.out.println("!!!!!!"+val+"!!!!");
		}
		IOUtils.closeStream(reader);
	}

}
