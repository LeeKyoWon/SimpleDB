package com.ll;

import lombok.Setter;

import java.sql.Connection;
import java.sql.SQLException;

@Setter
public class SimpleDb {
    private final String username;
    private final String password;
    private final String url;
    private boolean devMode = false;

    public SimpleDb(String host, String username, String password, String dbName) {
        int port = 3306;

        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?useUnicode=true&characterEncoding=utf8&autoReconnect=true&serverTimezone=Asia/Seoul&useOldAliasMetadataBehavior=true&zeroDateTimeBehavior=convertToNull";

        this.url = url;
        this.username = username;
        this.password = password;
    }

    public void run(String query, Object... args) {
        Sql sql = genSql();

        sql.append(query, args);
        sql.execute();
    }

    public Sql genSql() {
        Connection conn = ThreadConnectionManager.getConnection(url, username, password);
        return new Sql(url, username, password, conn);
    }

    public void closeConnection() {
        ThreadConnectionManager.closeConnection();
    }


    public void startTransaction() {
        try{
            Connection conn = ThreadConnectionManager.getConnection(url, username, password);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void rollback() {
        try{
            Connection conn = ThreadConnectionManager.getConnection(url, username, password);
            if(conn != null)
                conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void commit() {
        try{
            Connection conn = ThreadConnectionManager.getConnection(url, username, password);
            if(conn != null)
                conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
