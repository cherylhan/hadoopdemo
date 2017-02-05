package cheryl.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//缺少驱动 没有经过测试
public class JDBCHive {
 private static String Driver="org.apache.hadoop.hive.HiveDriver";
 private static String URL="jdbc:hive//172.19.19.70:10000/sqoop_test";
 private static String name="";
 private static String password="";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
     try {
		Class.forName(Driver);
		Connection conn=DriverManager.getConnection(URL,name,password);
		Statement stat=conn.createStatement();
		String sql="show tables";
		ResultSet rs=stat.executeQuery(sql);
		while(rs.next()){
			System.out.println(rs.getString(1));
		}
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}

}
