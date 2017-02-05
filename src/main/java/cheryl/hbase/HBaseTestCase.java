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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseTestCase {
	//HBaseConfiguration
	private static Configuration cfg=null;
	static HTablePool tp=null;
	static{
			cfg=HBaseConfiguration.create();
			tp=new HTablePool(cfg,2);
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
				HTable table=new HTable(cfg,tablename);
				Put p1=new Put(Bytes.toBytes(row));
				p1.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(data));
				table.put(p1);
				System.out.println("put '"+row+"','"+columnFamily+":"+column+"','"+data+"'");
				table.close();
	}
	
	//get data
	public static void get(String tablename,String row)throws Exception{
			HTable table=new HTable(cfg,tablename);
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
			s.setFilter(rf);
			ResultScanner rs=table.getScanner(s);
			for(Result r:rs){
				System.out.println("Scan:"+r);
			}
	
	}
	
	//show all data
	public static void scan(String tablename,Scan scan)throws Exception{
			HTable table=new HTable(cfg,tablename);
			
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
    public static TBData getDataMapRow(String tableName,String data
    		)throws IOException{

    	List<HbaseTask> maplist=null;
    	maplist=new LinkedList<HbaseTask>();
    	 ResultScanner scanner=null;
    	 //create page object
    	 TBData tbData=null;
    	 try {
			 //get hbase object from pool
			 HTableInterface table =getTable(tableName);
			 RowFilter rf=new RowFilter(CompareOp.EQUAL,
			new SubstringComparator(data));
			 Scan scan=new Scan();
			 scan.setFilter(rf);
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
				 String count=Bytes.toString(result.getValue(getBytes("content"), getBytes("count")));
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

    		get.addColumn(getBytes("content"), getBytes("count"));

    		list.add(get);
    	}
    	return list;
    }
    private static String toStr(byte[] bt){
    	return Bytes.toString(bt);
    }
    
    
    public static void main(String[] args) throws Exception{
//    	String tablename="hbase_tb";
//    	String columnFamily="cf";
//    	try {
//			HBaseTestCase.create(tablename, columnFamily);
//			HBaseTestCase.put(tablename, "row1", columnFamily, "cl1", "data");
//			HBaseTestCase.get(tablename, "row1");
//			HBaseTestCase.scan(tablename);
//			if(HBaseTestCase.delete(tablename)){
//				System.out.println("DELETE table"+tablename+"success");
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	
 //  	try {
   		String tablename="user";
        String columnFamily="info";

        String data="lisi";


        TBData tb=HBaseTestCase.getDataMapRow(tablename, data);
        for(HbaseTask task:tb.getResultList()){
        	System.out.println("rowKey:"+task.getRow());
        	System.out.println("count:"+task.getCount());
        	
        }
    
    	
    }

	
	
}
