package hbaseutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * @auther: CHANGGAOMING
 * @date: 2023/7/20
 * @Description: Description
 */
public class HBaseConnectUtil {
    private static Connection connection;
    // 静态代码块中代码，在加载这个类的时候，就被执行了
    static{
        try{
            Configuration configuration = HBaseConfiguration.create();
            // 2、配置ZooKeeper的参数
            configuration.set("hbase.zookeeper.quorum", "collection:2181");
            // 3、获取连接对象
            connection = ConnectionFactory.createConnection(configuration);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    // 通过该方法可以获取一个admin对象
    public static Admin getAdmin(){
        Admin admin = null;
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }

    public static Connection getConnection(){
        return connection;
    }
    public static Table getTable(String tableName){
        Table table =null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return table;
    }
    public static void printResult(Result result){
        CellScanner cellScanner = result.cellScanner(); // 获取到单元格的扫描对象
        try {
            while (cellScanner.advance()) { // 判断单元格中是否有数据
                Cell cell = cellScanner.current();// 获取到一个单元格
                // 获取到主键
                System.out.print(new String(CellUtil.cloneRow(cell)) + "\t");
                // 获取列族的名字
                System.out.print(new String(CellUtil.cloneFamily(cell)) + "\t");
                // 列的名字
                System.out.print(new String(CellUtil.cloneQualifier(cell)) + "\t");
                // 获取Cell中的数据
                //byte[] bytes = CellUtil.cloneValue(cell);// 获取单元格里面的值
                System.out.println(new String(CellUtil.cloneValue(cell)));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void applyResult(Filter columnFamilyFilter,Table table){

        Scan scan = new Scan();
        scan.setFilter(columnFamilyFilter);
        ResultScanner scanner =null;
        try {
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (Result result : scanner) {
            printResult(result);
        }
    }
public static void closeTable(Table table){
        if (table!=null) {
            try {
                table.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
}
    public static void closeAdminConnection(Admin admin){
        if(admin != null){
            try {
                admin.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
