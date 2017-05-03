package cheryl.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//将windows下的文件上传到hdfs
public class UploadFile {

	public static void main(String[] args) throws URISyntaxException, IOException {
	 Configuration conf=new Configuration();
	 URI uri=new URI("hdfs://hadoop:8020");
	 FileSystem fs=FileSystem.get(uri, conf);
	 //本地路径
	 String localPath="c://devlist.txt";
	 String hdfsPath="/user/dhcc";
	 Path resP=new Path(localPath);
	 //hdfs路径
	 Path destP=new Path(hdfsPath);
	 if(!fs.exists(destP)){
		 fs.mkdirs(destP);
	 }
 String name=localPath.substring(localPath.lastIndexOf("/")+1,localPath.length());
	 fs.copyFromLocalFile(resP, destP);
	 System.out.println("name is"+name);
	 fs.close();
	}

}
