package com.ll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ThreadConnectionManager {
    // ThreadLocal을 사용하여 각 쓰레드마다 독립적인 커넥션을 유지
    private static ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();

    // 현재 쓰레드의 커넥션을 가져옵니다.
    public static Connection getConnection(String url, String username, String password) {
        Connection conn = threadLocalConnection.get();

        if(conn == null) {
            try {
                conn = DriverManager.getConnection(url, username, password);
                threadLocalConnection.set(conn);
            } catch(SQLException e){
                e.printStackTrace();
            }
        }

        return conn;
    }

    // 커넥션을 명시적으로 종료합니다.
    public static void closeConnection() {
        Connection connection = threadLocalConnection.get();
        if (connection != null) {
            try {
                connection.close();  // 커넥션을 닫습니다.
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                threadLocalConnection.remove();  // ThreadLocal에서 커넥션을 제거
            }
        }
    }
}
