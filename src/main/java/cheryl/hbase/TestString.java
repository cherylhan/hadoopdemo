package cheryl.hbase;

import org.apache.commons.lang.StringUtils;

/**
 * Created by cheryl on 2016/5/30.
 */
public class TestString {
    public static void main(String[] args) {
        String last = "! hello";
        last = last.toLowerCase();
        //字符串过滤
        last = last.replaceAll("[^a-z]", " ");
        //使用一个空格替换掉连续的多余的空格
        last = last.replaceAll("//s{2,}", " ");
        String[] values = last.split(" ");
        System.out.println(values.length);
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < values.length; j++) {
                if (i != j && StringUtils.isNotBlank(values[i].trim()) && StringUtils.isNotBlank(values[j])) {
                    System.out.println("[" + values[i] + "," + values[j] + "]");
                }
            }
        }
    }
}
