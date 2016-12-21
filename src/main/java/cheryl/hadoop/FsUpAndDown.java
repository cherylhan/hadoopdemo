package cheryl.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

//hdfs 文件的上传与下载
public class FsUpAndDown {
	static Configuration conf=new Configuration();
	static{
		
		conf.set("fs.default.name", "hdfs://hadoop:9090");
	}
	public static void uploadFileToHdfs(String filepath,String dst) throws Exception{
		FileSystem fs=FileSystem.get(conf);
		Path srcp=new Path(filepath);
		Path dstp=new Path(dst);
		Long start=System.currentTimeMillis();
		fs.copyFromLocalFile(srcp, dstp);
		System.out.println("time:"+(System.currentTimeMillis()-start));
		System.out.println("________________________Upload to "+conf.get("fs.default.name")+"________________________");  
		fs.close();
		getDirectoryFromHdfs(dst);
	}
	/** 
     * 下载文件 
     * @param src 
     * @throws Exception 
     */  
    public static void downLoadFileFromHDFS(String src) throws Exception {  
        FileSystem fs = FileSystem.get(conf);  
        Path  srcPath = new Path(src);  
        InputStream in = fs.open(srcPath);  
        try {  
            //将文件COPY到标准输出(即控制台输出)  
            IOUtils.copyBytes(in, System.out, 4096,false);  
        }finally{  
             IOUtils.closeStream(in);  
            fs.close();  
        }  
    }  
    /** 
     * 遍历指定目录(direPath)下的所有文件 
     * @param direPath 
     * @throws Exception 
     */  
    public static void  getDirectoryFromHdfs(String direPath) throws Exception{  
          
        FileSystem fs = FileSystem.get(URI.create(direPath),conf);  
        FileStatus[] filelist = fs.listStatus(new Path(direPath));  
        for (int i = 0; i < filelist.length; i++) {  
            System.out.println("_________________***********************____________________");  
            FileStatus fileStatus = filelist[i];  
            System.out.println("Name:"+fileStatus.getPath().getName());  
            System.out.println("size:"+fileStatus.getLen());  
            System.out.println("_________________***********************____________________");  
        }  
        fs.close();  
    }  
    /** 
     * 测试方法 
     * @param args 
     */  
    public static void main(String[] args) {  
         
		try {
			// getDirectoryFromHdfs("/user");

			//uploadFileToHdfs("C:/Users/HC/Downloads/hadoop.txt", "/user");
			//getDirectoryFromHdfs("/user");
			 downLoadFileFromHDFS("hdfs://192.168.220.10:9090/user/hello.txt");
              
        } catch (Exception e) {  
            // TODO 自动生成的 catch 块  
            e.printStackTrace();  
        }  
    }  
}
