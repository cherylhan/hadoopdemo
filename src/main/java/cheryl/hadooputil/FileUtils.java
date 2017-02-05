package cheryl.hadooputil;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtils {
	static FileSystem fs=null;
	static URI uri;
	static Configuration conf=null;
	static{
		try {
			conf = new Configuration();
			uri = new URI("hdfs://hadoop:8020");
			fs = FileSystem.get(uri, conf);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void getFiles(File dir) throws IOException {
		String localPath = "";
		String hdfsPath = "/user/dhcc/";
		String path ="";
		Path hdf=new Path(hdfsPath);
		if(!fs.exists(hdf)){
			 fs.mkdirs(hdf);
		 }
		// 如果当前文件或目录存在
		if (dir.exists()) {
			// 如果是目录，则
			if (dir.isDirectory()) {
				// 打印当前目录的路径
				path = dir.toString();
				String[] p = path.split("\\\\");
				System.out.println(p[1]);
				for(int i=1;i<p.length;i++){
					if(i==1){
						hdfsPath=hdfsPath+p[i];
					}
					else{
						hdfsPath=hdfsPath+"/"+p[i];
					}
					Path realHFS=new Path(hdfsPath);
					if(!fs.exists(realHFS)){
						 fs.mkdirs(realHFS);
					 }
				}
				// 获取该目录下的所有文件和目录
				File[] files = dir.listFiles();
				// 递归遍历每一个字文件
				for (File file : files) {
					getFiles(file);
				}
				
				// 若是文件，则打印该文件路径及名称
			} else {
				String pt="/user/dhcc/";
				System.out.println(dir);
				localPath = dir.toString();
				String[] p=localPath.split("\\\\");
				for(int i=1;i<p.length;i++){
					pt=pt+p[i]+"/";
				} 
				pt=pt.substring(0, pt.length()-1); 
				fs.copyFromLocalFile(new Path(localPath),new Path(pt));
		
			}  
		}

	}
}
