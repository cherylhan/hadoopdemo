package cheryl.hbase;

public class HbaseTask {
	private String row;
	private String count;

	public void setRow(String row) {
		this.row = row;
	}

	public void setCount(String count) {
		this.count = count;
	}

	public String getCount() {
		return count;
	}

	public String getRow() {
		return row;
	}
	public HbaseTask(){

	}
	public HbaseTask(String row,String count){
		this.row=row;
		this.count=count;
	}
}
