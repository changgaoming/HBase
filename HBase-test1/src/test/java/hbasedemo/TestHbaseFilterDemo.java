package hbasedemo;

import hbaseutils.HBaseConnectUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * @auther: CHANGGAOMING
 * @date: 2023/7/21
 * @Description: Description
 */
public class TestHbaseFilterDemo {

    Table table;

    @Before
    public void init() {
        table = HBaseConnectUtil.getTable("test2:student");
    }

    @After
    public void destory() {
        HBaseConnectUtil.closeTable(table);
    }

    @Test
    public void testSingleColoumFilter() throws IOException {

        // where  family='base_info' and age < 30
        SingleColumnValueFilter singleColumnValueFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "age".getBytes(),
                        CompareOperator.LESS, // 小于的意思   less  greater
                        "30".getBytes());
        // 过滤掉 一些数据中没有年龄也查询出来的数据
        singleColumnValueFilter.setFilterIfMissing(true);
        Scan scan = new Scan();
        // select * from ns01:student
        //添加条件
        scan.setFilter(singleColumnValueFilter);
/*        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(System.out::println);


        //select * from ns01:student where  family='base_info' and age < 30
    ResultScanner resultScanner = table.getScanner(scan);
        for (Result result1 : resultScanner) {
            HBaseConnectUtil.printResult(result1);
        }*/
        ResultScanner resultScanner1 = table.getScanner(scan);
        // 迭代器 Iterator 这是一个接口类，一般集合类都实现了这个接口
        Iterator<Result> iterator = resultScanner1.iterator();//加迭代器的作用是为了判断是否为空值
        while (iterator.hasNext()) {
            Result result = iterator.next();
            HBaseConnectUtil.printResult(result);
        }
    }

    /**
     * 结构过滤器
     *
     * @throws IOException
     */
    @Test
    public void testMoreColoumFilter() throws IOException {
        SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter("base_info".getBytes(), "age".getBytes(), CompareOperator.LESS, "25".getBytes());
        SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter("base_info".getBytes(), "gender".getBytes(), CompareOperator.EQUAL, "female".getBytes());
        SingleColumnValueFilter singleColumnValueFilter3 = new SingleColumnValueFilter("extra_info".getBytes(), "country".getBytes(), CompareOperator.EQUAL, "shu".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter1);
        filterList.addFilter(singleColumnValueFilter2);
        filterList.addFilter(singleColumnValueFilter3);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        table.getScanner(scan);
    }

    //列族过滤器
    @Test
    public void testFamilyFilter() {
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("base_info".getBytes(), null, CompareOperator.EQUAL, new RegexStringComparator(".*_info$"));
        HBaseConnectUtil.applyResult(singleColumnValueFilter, table);
    }

    //列名过滤器
    @Test
    public void testQualifierFilter() {
        QualifierFilter filter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator("gender".getBytes()));
        HBaseConnectUtil.applyResult(filter, table);
    }

    /**
     * 列名前缀过滤器
     */
    @Test
    public void testColumnPrefixFilter() {
        // 获取列名以ge开头的数据
        ColumnPrefixFilter qualifierFilter = new ColumnPrefixFilter("na".getBytes());
        HBaseConnectUtil.applyResult(qualifierFilter, table);
    }

    /**
     * 多列名前缀过滤器
     */
    @Test
    public void testMultipleColumnPrefixFilter() {
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(new byte[][]{"a".getBytes(), "c".getBytes()});
        HBaseConnectUtil.applyResult(multipleColumnPrefixFilter, table);
    }

    /**
     * 列值有范围查找
     */
    @Test
    public void testColumnRangeFilter() {
        ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter("age".getBytes(), true, "gender".getBytes(), false);
        HBaseConnectUtil.applyResult(columnRangeFilter, table);
    }

    /**
     * 行值过滤器
     */
    @Test
    public void testRoweFilter() {
        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("rk[0-9]{3}[147]$"));
        HBaseConnectUtil.applyResult(rowFilter, table);
    }

    /**
     * 单列行值过滤器
     */
    @Test
    public void testFirstKeyOnlyFilter() {
        FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();
        HBaseConnectUtil.applyResult(firstKeyOnlyFilter, table);
    }

    /**
     * 分页过滤器
     */
    @Test
    public void testPageFilter() throws IOException {
        PageFilter pageFilter = new PageFilter(5);
        Scan scan = new Scan();
        scan.withStartRow("rk1006".getBytes());
        scan.setFilter(pageFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            HBaseConnectUtil.printResult(result);
        }
    }

    /**
     * 优化后的分页过滤器
     */
    @Test
    public void testPageFilter2() throws IOException {
        // 分页查询
        PageFilter pageFilter = new PageFilter(4);// 每一页是多少条数据
        Scan scan = new Scan().setFilter(pageFilter);

        byte[] beginKey = null;
        while (true) {
            int count = 0;// 累加器
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                HBaseConnectUtil.printResult(result);
                count++;
                beginKey = result.getRow();
            }
            if (count < 3) {
                break; // 在这里跳出死循环，将不会再结束后多打印一条分隔线
            }
            System.out.println("------------------分割线---------------------");
            // 第二个参数的意思是是否包含当前的key值   rk1005
            scan.withStartRow(beginKey, false);
        }
    }
}
