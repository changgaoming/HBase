package hbasedemo;

import hbaseutils.HBaseConnectUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @auther: CHANGGAOMING
 * @date: 2023/7/20
 * @Description: Description
 */
public class TableExist {
    public static void main(String[] args) throws IOException {
/*        // 1、获取配置对象
        Configuration configuration = HBaseConfiguration.create();
        // 2、配置ZooKeeper的参数
        configuration.set("hbase.zookeeper.quorum", "collection:2181");
        // 3、获取连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        // 4、获取一个DDL操作的HBase客户端Admin
        Admin admin = connection.getAdmin();*/
        Admin admin = HBaseConnectUtil.getAdmin();
        Connection connection = HBaseConnectUtil.getConnection();
        // 5、判断一张表是否存在
        System.out.println(admin.tableExists(TableName.valueOf("test1:teacher")));
        // 6、关闭操作
        HBaseConnectUtil.closeAdminConnection(admin);
    }
}
