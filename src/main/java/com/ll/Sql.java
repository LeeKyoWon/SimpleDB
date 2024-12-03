package com.ll;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class Sql {
    private StringBuffer sql = new StringBuffer();
    private List<Object> argsList = new ArrayList<>();
    private String url;
    private String username;
    private String password;
    private Connection conn;

    Sql(String url, String username, String password, Connection conn) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.conn = conn;
    }

    public void execute() {
        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {

            setArgsToPreparedStatement(pstmt, argsList);
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long insert() {
        int id = 0;

        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {

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
        try(PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
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
        try(PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
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
        try(PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
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

    public <T> List<T> selectRows(Class<T> clazz) {
        List<T> entityRows = new ArrayList<>();
        try(PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
        {
            try(ResultSet rs = pstmt.executeQuery()) {
                while(rs.next()) {
                    entityRows.add(getSelectedRow(clazz, rs));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return entityRows;
    }

    public <T> T selectRow(Class<T> clazz) {
        try(PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
        {
            try(ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return getSelectedRow(clazz, rs);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, Object> selectRow() {
        try(PreparedStatement pstmt = conn.prepareStatement(sql.toString()))
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

    public <T> T getSelectedRow(Class<T> clazz, ResultSet rs) {
        Map<String, Object> row = new HashMap<>();
        try {
            Field[] fields = clazz.getDeclaredFields();

            // 생성자를 초기화 할 데이터 저장
            Object[] fieldValues = new Object[fields.length];
            // 생성자의 파라미터 정보 저장
            Class<?>[] paramTypes = Util.getConstructorParamTypes(fields);

            Constructor<T> constructor = clazz.getConstructor(paramTypes);

            ResultSetMetaData rsMetaData = rs.getMetaData();
            int columnCount = rsMetaData.getColumnCount();

            // Column 을 1개씩 읽어서 객체의 생성자 인자로 사용할 fieldValues[]에 저장
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsMetaData.getColumnName(i);
                int columnType = rsMetaData.getColumnType(i);

                Field field = fields[i-1];
                field.setAccessible(true);
                Object value = null;

                switch (columnType) {
                    case Types.INTEGER:
                        value = rs.getLong(i);
                        break;
                    case Types.TIMESTAMP:
                        value = rs.getTimestamp(i).toLocalDateTime();
                        break;
                    case Types.BIT:
                        value = rs.getBoolean(i);
                        break;
                    default:
                        value = rs.getString(i);
                }
                row.put(columnName, value);
                fieldValues[i-1] = value;
            }

            return constructor.newInstance(fieldValues);
        }catch(SQLException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e){
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
        return selectOne(rs -> rs.getTimestamp(1).toLocalDateTime());
    }

    public Long selectLong() {
        return selectOne(rs -> rs.getLong(1));
    }

    public String selectString() {
        return selectOne(rs -> rs.getString(1));
    }

    public Boolean selectBoolean() {
        return selectOne(rs -> rs.getBoolean(1));
    }

    public <T> T selectOne(ResultSetExtractor<T> extractor) {

        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {

            setArgsToPreparedStatement(pstmt, argsList);

            try(ResultSet rs = pstmt.executeQuery()){
                if(rs.next()) {
                    return extractor.extract(rs);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Long> selectLongs() {
        return selectMultiple(rs -> rs.getLong(1));
    }

    public <T> List<T> selectMultiple(ResultSetExtractor<T> extractor) {
        List<T> results = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {

            setArgsToPreparedStatement(pstmt, argsList);

            try(ResultSet rs = pstmt.executeQuery()){
                while(rs.next()) {
                    results.add(extractor.extract(rs));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return results;
    }

    @FunctionalInterface
    public interface ResultSetExtractor<T> {
        T extract(ResultSet rs) throws SQLException;
    }

    private void setArgsToPreparedStatement(PreparedStatement pstmt, List<Object> argsList) throws  SQLException {
        for(int i=1; i<=argsList.size(); i++) {
            Object cur = argsList.get(i-1);
            if(cur instanceof String) {
                pstmt.setString(i, (String)cur);
            } else if (cur instanceof Integer) {
                pstmt.setInt(i, (Integer)cur);
            } else if(cur instanceof Long) {
                pstmt.setLong(i, (Long)cur);
            } else if (cur instanceof  Boolean) {
                pstmt.setBoolean(i, (Boolean)cur);
            }
        }
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

    public Sql appendIn(String query, Object... args) {
        StringBuffer sb = new StringBuffer();
        sb.append("?");
        for (int i = 1; i < args.length; i++) {
            sb.append(", ?");
        }
        query = query.replace("?", sb.toString());
        append(query);

        argsList.addAll(Arrays.asList(args));
        return this;
    }
}



