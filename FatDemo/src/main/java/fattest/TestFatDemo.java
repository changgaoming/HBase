package fattest;

import java.sql.*;

/**
 * @auther: CHANGGAOMING
 * @date: 2023/7/24
 * @Description: Description
 */
public class TestFatDemo {
    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:phoenix:collection:2181");
        PreparedStatement preparedStatement = connection.prepareStatement("select *from student");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            System.out.println(resultSet.getString("name"));
            System.out.println(resultSet.getString("id"));
            System.out.println(resultSet.getString("addr"));
        }
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
