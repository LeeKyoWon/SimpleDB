package com.ll;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class Sql {
    private StringBuffer sql = new StringBuffer();
    private List<Object> argsList = new ArrayList<>();
    private String url;
    private String username;
    private String password;

    Sql(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public void execute() {
        try (Connection conn = getConnect();
             PreparedStatement pstmt = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {

            setArgsToPreparedStatement(pstmt, argsList);
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long insert() {
        int id = 0;

        try (Connection conn = getConnect();
             PreparedStatement pstmt = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {

            setArgsToPreparedStatement(pstmt, argsList);
            pstmt.executeUpdate();

            try (ResultSet rs = pstmt.getGeneratedKeys()) {
                if (rs.next()) {
                    id = rs.getInt(1);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public long update() {
        int affectedRowNum = 0;
        try(Connection conn = getConnect();
            PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
        {
            setArgsToPreparedStatement(pstmt, argsList);
            affectedRowNum = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return affectedRowNum;
    }

    public long delete() {
        int affectedRowNum = 0;
        try(Connection conn = getConnect();
            PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
        {
            setArgsToPreparedStatement(pstmt, argsList);
            affectedRowNum = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return affectedRowNum;
    }

    private void setArgsToPreparedStatement(PreparedStatement pstmt, List<Object> argsList) throws  SQLException {
        for(int i=1; i<=argsList.size(); i++) {
            Object cur = argsList.get(i-1);
            if(cur instanceof String) {
                pstmt.setString(i, (String)cur);
            } else if (cur instanceof Integer) {
                pstmt.setInt(i, (Integer)cur);
            } else if (cur instanceof  Boolean) {
                pstmt.setBoolean(i, (Boolean)cur);
            }
        }
    }

    private Connection getConnect() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    public Sql append(String query) {
        sql.append(query).append(" ");
        return this;
    }

    public Sql append(String query, String arg) {
        append(query);
        argsList.add(arg);
        return this;
    }

    public Sql append(String query, Object... args) {
        append(query);
        argsList.addAll(Arrays.stream(args)
                .toList());
        return this;
    }

    public Sql appendIn(String query, int... args) {
        append(query);
        argsList.addAll(IntStream.of(args)
                .boxed()
                .toList());
        return this;
    }

}

