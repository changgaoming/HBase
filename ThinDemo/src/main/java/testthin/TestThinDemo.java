package testthin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @auther: CHANGGAOMING
 * @date: 2023/7/24
 * @Description: Description
 */
public class TestThinDemo {
    public static void main(String[] args) throws Exception{
        // 创建链接
        String url = "jdbc:phoenix:thin:url=http://collection:8765;serialization=PROTOBUF";
        Connection connection = DriverManager.getConnection(url);
        // 获取Statement
        // PreparedStatement 比 Statement 更加安全，可以有效方式sql注入的风险
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");
        // sql语句的执行
        ResultSet resultSet = preparedStatement.executeQuery();
        while(resultSet.next()){
            System.out.println(resultSet.getString("name"));
            System.out.println(resultSet.getString("id"));
            System.out.println(resultSet.getString("addr"));
        }
        // 关闭连接等
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
