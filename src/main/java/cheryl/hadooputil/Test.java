package cheryl.hadooputil;
//将windows下文件夹中的内容全部传到hdfs按照相应的目录全部传到hdfs下
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
public class Test {
	public static void main(String[] args) throws Exception {
		File file=new File("E://medical_data");
		HFileUtils.getFiles(file);
		

	}
	
}
