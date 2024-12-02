package com.ll;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
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

    public List<Map<String, Object>> selectRows() {
        List<Map<String,Object>> selectedRows = new ArrayList<>();
        try(Connection conn = getConnect();
            PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
        {
            try(ResultSet rs = pstmt.executeQuery()) {
                while(rs.next()) {
                    selectedRows.add(getSelectedRow(rs));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return selectedRows;
    }

    public Map<String, Object> selectRow() {
        try(Connection conn = getConnect();
            PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
        {
            try(ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return getSelectedRow(rs);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, Object> getSelectedRow(ResultSet rs) {
        Map<String, Object> row = new HashMap<>();
        try {
            ResultSetMetaData rsMetaData = rs.getMetaData();
            int columnCount = rsMetaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsMetaData.getColumnName(i);
                int columnType = rsMetaData.getColumnType(i);

                switch (columnType) {
                    case Types.INTEGER:
                        row.put(columnName, rs.getLong(i));
                        break;
                    case Types.TIMESTAMP:
                        row.put(columnName, rs.getTimestamp(i).toLocalDateTime());
                        break;
                    case Types.BIT:
                        row.put(columnName, false);
                        break;
                    default:
                        row.put(columnName, rs.getString(i));
                }
            }
        }catch(SQLException e){
                e.printStackTrace();
        }
        return row;
    }

    public LocalDateTime selectDatetime() {
        try (Connection conn = getConnect();
             PreparedStatement pstmt = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {

            try(ResultSet rs = pstmt.executeQuery()){
                if(rs.next()) {
                    return rs.getTimestamp(1).toLocalDateTime();
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
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

