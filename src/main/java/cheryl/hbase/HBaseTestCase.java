package cheryl.hbase;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * 列簇中每次修改数据会有version问题
 * 默认version为1 修改后便可以显示多条记录
 * HBaseAdmin 一个接口用来管理HBase数据库信息
 * 创建、删除、使表有效、无效等
 * HTableDescriptor创建表名或者列族
 * HColumnDescriptor 维护着列族信息。例如版本号，压缩设置等
 * HTable可以用来和HBase直接通信，此方法对于更新来说是非线程安全的
 * HTablePool 表的资源池 可复用
 * Result 真正的一条记录
 * 
 * 注意eclipse端的hbase和服务器端的hbase版本需要一致 否则容易出现乱码 Not a host:port pair: PBUF hbase
 * hbase-1.1.2.2.5.3.0-37.jar
 * 
 * 过滤器 在服务器端处理
 *  HTable 不是线程安全的           
 * pageFiler 分页 
 * 
 * 
 * */

public class HBaseTestCase {
	//HBaseConfiguration
	private static Configuration cfg=null;
	static HTablePool tp=null;
	static{
		cfg=HBaseConfiguration.create();
		cfg.set("hbase.rootdir","hdfs://hadoop:8020/apps/hbase/data");
		cfg.set("hbase.zookeeper.quorum", "hadoop"); 
		cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("zookeeper.znode.parent", "/hbase-unsecure");
			tp=new HTablePool(cfg,1);
	}
	//create a table
	public static void create(String tableName,String columnFamily)throws Exception{
			HBaseAdmin admin =new HBaseAdmin(cfg);
			
			if(admin.tableExists(tableName)){
				System.out.print("table exsist");
				System.exit(0);
				admin.close();
			}else{
				HTableDescriptor tableDesc=new HTableDescriptor(tableName);
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
				admin.createTable(tableDesc);
				admin.close();
				System.out.println("create table success");
			}
	}
	
	//add data
	public static void put(String tablename,String row,String columnFamily,String column,String data)throws Exception{
				HTable table=new HTable(cfg, tablename);
				Put p1=new Put(Bytes.toBytes(row));
				p1.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(data));
				table.put(p1);
				System.out.println("put '"+row+"','"+columnFamily+":"+column+"','"+data+"'");
				table.close();
	}
	
	//get data
	public static void get(String tablename,String row)throws Exception{
			HTable table=new HTable(cfg, tablename);
			Get g=new Get(Bytes.toBytes(row));
			Result result =table.get(g);
			System.out.println("Get: "+result);
			table.close();
	}
	
	//show all data
	public static void scan(String tablename,String data)throws Exception{
		HTableInterface table =getTable(tablename);
		RowFilter rf=new RowFilter(CompareOp.EQUAL,
				new SubstringComparator(data));
			Scan s=new Scan();
			s.setCaching(1000);//不设置的话每一条访问一次regionserver
			s.setFilter(rf);
			ResultScanner rs=table.getScanner(s);
			for(Result r:rs){
				System.out.println("Scan:"+r);
			}
	
	}
	
	//show all data 全表扫描
	public static void scan(String tablename,Scan scan)throws Exception{
		HTable table=new HTable(cfg, tablename);
			
			ResultScanner rs=table.getScanner(scan);
			for(Result r:rs){
				System.out.println("Scan:"+new String(r.getRow()));
				for(org.apache.hadoop.hbase.KeyValue kv:r.raw()){
					System.out.println(new String(kv.toString()));
				}
			}
			table.close();
	
	}
	
	
     //delete data
    public static boolean delete(String tablename)throws Exception{
    	HBaseAdmin admin=new HBaseAdmin(cfg);
    	if(admin.tableExists(tablename)){
    		try {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
				admin.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
    	}
    	return true;
}
    // GET HBase Table
    public static HTableInterface getTable(String tableName){
    	if(StringUtils.isEmpty(tableName)) return null;
    	return tp.getTable(getBytes(tableName));
    }
    
    //to byte array
    public static byte[] getBytes(String value){
    	if(value==null) value="";
    	return Bytes.toBytes(value);
    }
    
    //reserach data
    public static TBData getDataMapRow(String tableName, String data)throws IOException{

    	List<HbaseTask> maplist=null;
    	maplist=new LinkedList<HbaseTask>();
    	 ResultScanner scanner=null;
    	 //create page object
    	 TBData tbData=null;
    	 try {
			 //get hbase object from pool
			 HTableInterface table =getTable(tableName);
			 RowFilter filter=new RowFilter(CompareOp.EQUAL,new BinaryComparator(data.getBytes()));
			 Scan scan=new Scan();
			 scan.setFilter(filter);
			 scan.setCaching(2);
			 scan.setCacheBlocks(false);
			 scanner=table.getScanner(scan);

			 List<byte[]> rowList =new LinkedList<byte[]>();
			 for(Result result:scanner){
				 String row=toStr(result.getRow());
					 rowList.add(getBytes(row));
			 }
			 // get rowkey GetObject
			 List<Get> getList=getList(rowList);
			 Result[] results=table.get(getList);
			 
			 // the result
			 for(Result result:results){
//				 Map<byte[],byte[]> fmap=packFamilyMap(result);
//				 Map<String,String> rmap=packRowMap(fmap);
				 String id=Bytes.toString(result.getRow());
				 String count=Bytes.toString(result.getValue(getBytes("course"), getBytes("english")));
				 HbaseTask task=new HbaseTask();
				 task.setRow(id);
				 task.setCount(count);
				 maplist.add(task);
			 }
			 tbData=new TBData();
			 tbData.setResultList(maplist);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			closeScanner(scanner);
		}
    	 
    	 return tbData;
    	 
    }
 
    //get scan
    private static Scan getScan(String startRow,String stopRow){
    	Scan scan=new Scan();
    	scan.setStartRow(getBytes(startRow));
    	scan.setStopRow(getBytes(stopRow));
    	return scan;
    }
    
  
    
    private static void closeScanner(ResultScanner scanner){
    	if(scanner!=null)
    		scanner.close();
    }
    
    //data package row
    private static Map<String,String> packRowMap(Map<byte[],byte[]> dataMap){
    	Map<String,String> map=new LinkedHashMap<String,String>();
    	for(byte[] key:dataMap.keySet()){
    		byte[] value=dataMap.get(key);
    		map.put(toStr(key),toStr(value));
    	}
    	return map;
    }
    //accord rowkey get collect GET
    private static List<Get> getList(List<byte[]> rowlist){
    	List<Get> list=new LinkedList<Get>();
    	for(byte[] row:rowlist){
    		Get get=new Get(row);

    		get.addColumn(getBytes("course"), getBytes("english"));

    		list.add(get);
    	}
    	return list;
    }
    private static String toStr(byte[] bt){
    	return Bytes.toString(bt);
    }
    
    
    public static void main(String[] args) throws Exception{
    	String tablename="scores";
    	String columnFamily="course";
//   	try {
//	成功		HBaseTestCase.create(tablename, columnFamily);
    //  成功 	HBaseTestCase.put(tablename, "jim", columnFamily, "chinese", "90");
//  成功		HBaseTestCase.get(tablename, "Tom");
// 	yes		HBaseTestCase.scan(tablename, "Tom");
//	成功	if(HBaseTestCase.delete(tablename)){
//			System.out.println("DELETE table"+tablename+"success");
//		}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	
    	// 查询 scores 表 rowkey Tom 列族course 列english
      
    	
//
    	String data="Tom";
        TBData tb=HBaseTestCase.getDataMapRow(tablename,data);
        for(HbaseTask task:tb.getResultList()){
        	System.out.println("rowKey:"+task.getRow());
        	System.out.println("count:"+task.getCount());
        	
        }
    
//    	
   }

	
	
}
