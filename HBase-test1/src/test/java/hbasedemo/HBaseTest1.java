package hbasedemo;

import hbaseutils.HBaseConnectUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @auther: CHANGGAOMING
 * @date: 2023/7/20
 * @Description: Description
 */
public class HBaseTest1 {
    Admin admin = null;
    Table table = null;
    @Before
    public void init(){ admin = HBaseConnectUtil.getAdmin();
    table=HBaseConnectUtil.getTable("test2:student");
    }

    @After
    public void destory(){
        HBaseConnectUtil.closeAdminConnection(admin);
        HBaseConnectUtil.closeTable(table);
    }

    /**
     * 创建库
     * @throws IOException
     */
    @Test
    public void testCreateNameSpace() throws IOException {
        NamespaceDescriptor build = NamespaceDescriptor.create("test2").build();
        admin.createNamespace(build);
    }

    /**
     * 打印所有库
     * @throws IOException
     */
    @Test
    public void HBaseListTest() throws IOException {

        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        Arrays.stream(namespaceDescriptors).forEach(System.out::println);
    }
    /**
     * 展示所有表
     * 不分库，甚至还带有库名
     */
    @Test
    public void testListTable() throws IOException {
        TableName[] tableNames = admin.listTableNames();
        Arrays.stream(tableNames).forEach(System.out::println);
    }

    /**
     * 展示某个库下的表
     */
    @Test
    public void testListTable2() throws IOException {
        TableName[] test1s = admin.listTableNamesByNamespace("test1");
        Arrays.stream(test1s).forEach(System.out::println);
    }

    /**
     * 修改namespace属性
     * @throws
     */
    @Test
    public void testNameSpaceAlter() throws IOException {
        NamespaceDescriptor test2 = admin.getNamespaceDescriptor("test2");
        test2.setConfiguration("auther","xiaoming");
        test2.setConfiguration("age","22");
        test2.setConfiguration("genter","mafe");
        admin.modifyNamespace(test2);
    }

    /**
     *删库
     */
    @Test
    public void testDeleteNameSpace() throws IOException {
        admin.deleteNamespace("test2");
    }



    /**
     * 创建表
     */
    @Test
    public void testCreateTable() throws IOException {
        TableName tableName = TableName.valueOf("test2:student");
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptor build1 = ColumnFamilyDescriptorBuilder.newBuilder("base_info".getBytes()).setMaxVersions(3).build();
        ColumnFamilyDescriptor build2 = ColumnFamilyDescriptorBuilder.newBuilder("addr".getBytes()).build();
        TableDescriptor build = tableDescriptorBuilder.setColumnFamilies(Arrays.asList(build1, build2)).build();
        admin.createTable(build);
    }

    /**
     * 测试修改表的列族信息
     * @throws IOException
     */
    @Test
    public void testUpdateTable() throws IOException {
        TableName tableName = TableName.valueOf("test2:student");
        TableDescriptor descriptor = admin.getDescriptor(tableName);
        ColumnFamilyDescriptor columnFamily = descriptor.getColumnFamily("addr".getBytes());
        ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily).setConfiguration("addr", "base_addr").build();
        admin.modifyColumnFamily(tableName,build);
    }
    /**
     * 新增列族
     */
    @Test
    public void testInsertLz() throws IOException {
        TableName tableName = TableName.valueOf("test2:student");
        ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder("grade".getBytes()).build();
        admin.addColumnFamily(tableName,build);
    }

    /**
     * 删除列族
     * @throws IOException
     */
    @Test
    public void testDeleteLz() throws IOException {
        TableName tableName = TableName.valueOf("test2:student");
        admin.deleteColumnFamily(tableName,"grade".getBytes());
    }

    /**
     * 删除表
     * @throws IOException
     */
    @Test
    public void testDeleteTable() throws IOException {
        TableName tableName = TableName.valueOf("test2:student");
        if (admin.tableExists(tableName)) {
            if (admin.isTableDisabled(tableName)) {
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
        }
    }
    /**
     * 添加数据
     */
    @Test
public void testAddTable() throws IOException {
    Put put = new Put("rk001".getBytes());
    put.addColumn("base_info".getBytes(),"name".getBytes(),"xiaoming".getBytes());
    put.addColumn("base_info".getBytes(),"age".getBytes(),"22".getBytes());
    put.addColumn("base_info".getBytes(),"gender".getBytes(),"male".getBytes());
    put.addColumn("addr".getBytes(),"loc".getBytes(),"zz".getBytes());
    Put put1 = new Put("rk002".getBytes());
        put1.addColumn("base_info".getBytes(),"name".getBytes(),"xiaoming1".getBytes());
        put1.addColumn("base_info".getBytes(),"age".getBytes(),"23".getBytes());
        put1.addColumn("base_info".getBytes(),"gender".getBytes(),"male".getBytes());
        put1.addColumn("addr".getBytes(),"loc".getBytes(),"zz".getBytes());
        table.put(Arrays.asList(put1,put));
}

    /**
     * 利用scan查询
     */
    @Test
    public void scanTable() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(System.out::println);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result: resultScanner){ // ResultScanner 获取到多行数据
            HBaseConnectUtil.printResult(result);
        }
    }

@Test
    public void getTableColumn() throws IOException {
    Get get = new Get("rk001".getBytes());
    get.addColumn("base_info".getBytes(),"name".getBytes());
    table.get(get);
}
    @Test
    public void getTable() throws IOException {
        Get get = new Get("rk001".getBytes());
        get.addFamily("addr".getBytes());
        table.get(get);
    }
@Test
    public void getAllTable() throws IOException {
    Get get = new Get("rk001".getBytes());
    Get get1 = new Get("rk002".getBytes());
    table.get(Arrays.asList(get,get1));
}

    // 删除指定行的数据
    @Test
    public void deleteRowTest() throws Exception{
        Delete delete = new Delete("rk001".getBytes());
        table.delete(delete);
    }

    // 删除指定行的某个单元格的数据
    @Test
    public void deleteCellTest() throws Exception{
        Delete delete = new Delete("rk001".getBytes());
        delete.addColumn("base_info".getBytes(),"age".getBytes());
        table.delete(delete);
    }


    // 删除指定行的多个单元格的数据
    @Test
    public void deleteBatchCellTest() throws Exception{
        Delete delete = new Delete("rk001".getBytes());
        delete.addColumn("base_info".getBytes(),"age".getBytes());

        Delete delete2 = new Delete("rk002".getBytes());
        delete2.addColumn("base_info".getBytes(),"gender".getBytes());

        ArrayList<Delete> list = new ArrayList<>();
        list.add(delete);
        list.add(delete2);
        table.delete(list);
    }
}
