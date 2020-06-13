package com.qimok.migrate.jdbc.facade;

import com.qimok.migrate.jdbc.JdbcService;
import org.springframework.stereotype.Component;
import java.sql.*;

@Component
public class JdbcFacade implements JdbcService {

    @Override
    public Connection getConnection(String url, String username, String password) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = null;
        try {
            url += "&serverTimezone=Asia/Shanghai&useServerPrepStmts=false&useSSL=true&rewriteBatchedStatements=true";
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public void close(Connection sourceConn, Connection targetConn,
                      Statement stmt, ResultSet resultSet,
                      PreparedStatement pstmt) {
        if (sourceConn != null) {
            try {
                sourceConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (targetConn != null) {
            try {
                targetConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
