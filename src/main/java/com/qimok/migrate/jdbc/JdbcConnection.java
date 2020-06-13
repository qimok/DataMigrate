package com.qimok.migrate.jdbc;

import com.qimok.migrate.jdbc.facade.JdbcFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author qimok
 * @description 获取数据库连接
 * @since 2020-06-1 10:07
 */
@Component
public class JdbcConnection {

    @Value("${db.source.url}")
    private String sourceUrl;

    @Value("${db.source.username}")
    private String sourceUsername;

    @Value("${db.source.password}")
    private String sourcePassword;

    @Value("${spring.datasource.url}")
    private String targetUrl;

    @Value("${spring.datasource.username}")
    private String targetUsername;

    @Value("${spring.datasource.password}")
    private String targetPassword;

    @Autowired
    private JdbcFacade jdbcFacade;

    public Connection getTargetConn() {
        return jdbcFacade.getConnection(targetUrl, targetUsername, targetPassword);
    }

    public Connection getSourceConn() {
        return jdbcFacade.getConnection(sourceUrl, sourceUsername, sourcePassword);
    }

    public void closeConn(Connection sourceConn, Connection targetConn,
                          Statement stmt, ResultSet resultSet,
                          PreparedStatement pstmt) {
        jdbcFacade.close(sourceConn, targetConn, stmt, resultSet, pstmt);
    }

}
