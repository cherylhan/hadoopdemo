package cheryl.hadooputil;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;

public class TransformtoUtf8 {
	//因为hadoop默认编码为utf-8,所以在处理时要将相关文件转码
		public static Text transformTextToUTF8(Text text, String encoding) {
			String value = null;
			try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
			} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			}
			return new Text(value);
			}
}
