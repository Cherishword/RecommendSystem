package ezr.bigdata.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangxmPC on 2016-05-04.
 * 数据库基本操作
 */
public class DBHelper {
    private String _connetctionString = "";
    public  Connection conn = null;

    public DBHelper(String connectionString){
        this._connetctionString = connectionString;
    }

    public Connection getConnection() {
        try {
            String driver = "com.mysql.jdbc.Driver";
            String url = this._connetctionString;
            Class.forName(driver);

            if (null == conn || conn.isClosed()) {
                conn = DriverManager.getConnection(url);
            }
        } catch (ClassNotFoundException e) {
            System.out.println("Sorry,can't find the Driver!");
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public ResultSet query(String sql){
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
        } catch (SQLException err) {
            err.printStackTrace();
            free(rs, stmt, conn);
        }
        return rs;
    }

    public int executeNonQuery(String sql) {
        int result = 0;
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            result = stmt.executeUpdate(sql);
        } catch (SQLException err) {
            err.printStackTrace();
            free(null, stmt, conn);
        } finally {
            free(null, stmt, conn);
        }
        return result;
    }

    public static void free(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }

        } catch (SQLException err) {
            err.printStackTrace();
        }
    }

    public static void free(Statement st) {
        try {
            if (st != null) {
                st.close();
            }
        } catch (SQLException err) {
            err.printStackTrace();
        }
    }

    public static void free(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException err) {
            err.printStackTrace();
        }
    }

    public static void free(ResultSet rs, Statement st, Connection conn) {
        free(rs);
        free(st);
        free(conn);
    }

    /**
     * 通过key获得查询结果
     * @param rs
     * @param field
     * @return 返回String 结果
     */
    public String getStringResult(ResultSet rs, String field){
        String result = "";
        try {
            while (rs.next()) {
                result = rs.getString(field);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            DBHelper.free(rs);
            DBHelper.free(conn);
        }
        return result;
    }

    /**
     * 通过key获得查询结果
     * @param rs
     * @param field
     * @return 返回Int 结果
     */
    public int getIntResult(ResultSet rs, String field){
        int result = -100000;
        try {
            while (rs.next()) {
                result = rs.getInt(field);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            DBHelper.free(rs);
            DBHelper.free(conn);
        }
        return result;
    }

    /**
     * ResultSet 里取出 多个int结果放到List里
     * @param rs
     * @param field
     * @return List<Integer>
     */
    public List<Integer> getIntResultList(ResultSet rs, String field){
        List<Integer> intResultList = new ArrayList<Integer>();
        try {
            while (rs.next()) {
                intResultList.add(rs.getInt(field));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            DBHelper.free(rs);
            DBHelper.free(conn);
        }
        return intResultList;
    }
}
