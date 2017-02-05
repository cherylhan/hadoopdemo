package cheryl.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by cheryl on 2016/6/6.
 * 将数据保存到HBase  hbase 主要存在版本问题 linux上为1.2.4
 */
public class WordCountHbase {
    public static class WordCountHbaseMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //读出字符串
            String last=value.toString();
            //转换成小写
            last=last.toLowerCase();
            //字符串过滤
            last =last.replaceAll("[^a-z]"," ");
            //使用一个空格替换掉连续的多余的空格
            last=last.replaceAll("//s{2,}"," ");
            String[] values = last.split(" ");
            Map map=new HashMap();
            for(int i=0;i<values.length;i++) {
                if(map.containsKey(values[i])){
                    continue;
                }else {
                    map.put(values[i], "1");
                    for (int j = 0; j < values.length; j++) {
                        if (i != j && StringUtils.isNotBlank(values[i].trim()) && StringUtils.isNotBlank(values[j])) {
                            String k = "[" + values[i] + "," + values[j] + "]";
                            word.set(k);
                            context.write(word, one);
                        }
                    }
                }

            }
        }
    }

    public static class WordCountHbaseReducer extends
            TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {// 遍历求和
                sum += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));//put实例化，每一个词存一行
            //列族为content,列修饰符为count，列值为数目
            put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);// 输出求和后的<key,value>
        }
    }

    public static void main(String[] args) throws Exception {
        String tablename = "wordcounthc";
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", "hdfs://192.168.220.10:9090");//namenode的地址
       //  conf.set("mapred.job.tracker", "hdfs://192.168.220.10.50020");//namenode的地址
       // conf.set("mapred.jar", "E:\\wordcounthbase.jar");
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tablename)){
            System.out.println("table exists!recreating.......");
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        HTableDescriptor htd = new HTableDescriptor(tablename);
        HColumnDescriptor tcd = new HColumnDescriptor("content");
        htd.addFamily(tcd);//创建列族
        admin.createTable(htd);//创建表

        Job job = new Job(conf, "WordCountHbase");
        job.setJarByClass(WordCountHbase.class);
        //使用WordCountHbaseMapper类完成Map过程；
        job.setMapperClass(WordCountHbaseMapper.class);
        TableMapReduceUtil.initTableReducerJob(tablename, WordCountHbaseReducer.class, job);
        //设置任务数据的输入路径；
        FileInputFormat.addInputPath(job,new Path("hdfs://192.168.220.10:9090/user/hello.txt"));
        //设置了Map过程和Reduce过程的输出类型，其中设置key的输出类型为Text；
        job.setOutputKeyClass(Text.class);
        //设置了Map过程和Reduce过程的输出类型，其中设置value的输出类型为IntWritable；
        job.setOutputValueClass(IntWritable.class);
        //调用job.waitForCompletion(true) 执行任务，执行成功后退出；
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
