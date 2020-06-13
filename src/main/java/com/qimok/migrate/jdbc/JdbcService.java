package com.qimok.migrate.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author qimok
 * @since 2020-06-1 10:07
 */
public interface JdbcService {

    /**
     * 获取 JDBC 连接
     */
    Connection getConnection(String url, String username, String password);

    /**
     * 关闭 JDBC 连接
     */
    void close(Connection sourceConn, Connection targetConn,
               Statement stmt, ResultSet resultSet,
               PreparedStatement pstmt);

}
