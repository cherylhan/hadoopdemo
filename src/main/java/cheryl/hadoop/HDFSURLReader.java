package cheryl.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import cheryl.hadooputil.HadoopUtil;

public class HDFSURLReader {
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) {
		InputStream in=null;
		String url=HadoopUtil.URL+"/user/hello.txt";
		try {
			in=new URL(url).openStream();
			IOUtils.copyBytes(in, System.out, 1024,false);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			IOUtils.closeStream(in);
			e.printStackTrace();
		}
	}
}
