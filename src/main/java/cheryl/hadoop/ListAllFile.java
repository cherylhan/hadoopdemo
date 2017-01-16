package cheryl.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
//列出HDFS下面的所有文件
public class ListAllFile {
 public static void main(String[] args) throws Exception {
	String url=args[0];
	//获取配置信息
	Configuration conf=new Configuration();
	//操作HDFS
	FileSystem fs=FileSystem.get(URI.create(url), conf);
	
	Path[] paths=new Path[args.length];
	for(int i=0;i<paths.length;i++){
		paths[i]=new Path(args[i]);
	}
	//获取路径状态 
	FileStatus[] status=fs.listStatus(paths);
	Path[] listedPaths=FileUtil.stat2Paths(status);
	for(Path p:listedPaths){
		System.out.println(p);
	}
	
	
	
	
 }
}
