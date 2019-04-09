/**
 * Copyright (C), 2011-2019.
 */
package com.jony.common.intercept;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * 分析sql语句是否触发全表扫描，如果触发全表扫描，打印错误日志和堆栈信息
 *
 * @author jony 2019/3/25 - 14:25.
 */
@Profile({"dev", "test"})
@Intercepts(@Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}))
@Slf4j
@Service
public class SqlIndexAnalysisInterceptor implements Interceptor {

    @Autowired
    private DruidDataSource druidDataSource;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {

        //分析sql是否命中索引
        sqlIndexAnalysis(invocation);

        return invocation.proceed();
    }

    @Override
    public Object plugin(Object target) {
        if (target instanceof Executor) {
            return Plugin.wrap(target, this);
        }
        return target;
    }

    @Override
    public void setProperties(Properties properties) {
    }

    /**
     * 分析当前sql是否命中索引，如果是全表扫描，中断当前执行
     *
     * @param invocation
     * @throws SQLException
     */
    @Async
    void sqlIndexAnalysis(Invocation invocation) throws SQLException {

        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {

            MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
            Object parameter = null;
            if (invocation.getArgs().length > 1) {
                parameter = invocation.getArgs()[1];
            }
            BoundSql boundSql = mappedStatement.getBoundSql(parameter);
            Configuration configuration = mappedStatement.getConfiguration();
            String explainSql = "explain " + this.getSql(configuration, boundSql);

            conn = druidDataSource.getConnection();
            statement = conn.createStatement();
            resultSet = statement.executeQuery(explainSql);
            while (resultSet.next()) {
                //当前sql触发全表扫描，打印日志
                if (resultSet.getString(5) != null && resultSet.getString(5).equals("ALL")) {
                    log.error(String.format("系统错误[errorCode%s]: ", 10000),new RuntimeException("当前sql触发全表扫描，请立即优化! sql: " + explainSql));
                }
            }

        } catch (Exception e) {
            log.error("execute explain plan sql exception!", e);
        } finally {
            if (conn != null) {
                conn.close();
            }

            if (statement != null) {
                statement.close();
                ;
            }

            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    /**
     * 获取完整的sql语句
     *
     * @param configuration
     * @param boundSql
     * @return
     */
    private String getSql(Configuration configuration, BoundSql boundSql) {
        // 输入sql字符串空判断
        String sql = boundSql.getSql();
        if (StringUtils.isEmpty(sql)) {
            return "";
        }
        return formatSql(sql, configuration, boundSql);
    }

    /**
     * 将占位符替换成参数值
     *
     * @param sql
     * @param configuration
     * @param boundSql
     * @return
     */
    private String formatSql(String sql, Configuration configuration, BoundSql boundSql) {

        sql = beautifySql(sql);

        //填充占位符
        Object parameterObject = boundSql.getParameterObject();
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();

        List<String> parameters = new ArrayList<>();
        if (parameterMappings != null) {
            MetaObject metaObject = parameterObject == null ? null : configuration.newMetaObject(parameterObject);
            for (int i = 0; i < parameterMappings.size(); i++) {
                ParameterMapping parameterMapping = parameterMappings.get(i);
                if (parameterMapping.getMode() != ParameterMode.OUT) {
                    //  参数值
                    Object value;
                    String propertyName = parameterMapping.getProperty();
                    //  获取参数名称
                    if (boundSql.hasAdditionalParameter(propertyName)) {
                        // 获取参数值
                        value = boundSql.getAdditionalParameter(propertyName);
                    } else if (parameterObject == null) {
                        value = null;
                    } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
                        // 如果是单个值则直接赋值
                        value = parameterObject;
                    } else {
                        value = metaObject == null ? null : metaObject.getValue(propertyName);
                    }

                    if (value instanceof Number) {
                        parameters.add(String.valueOf(value));
                    } else {
                        StringBuilder builder = new StringBuilder();
                        builder.append("'");
                        if (value instanceof Date) {
                            builder.append(dateTimeFormatter.get().format((Date) value));
                        } else if (value instanceof String) {
                            builder.append(value);
                        }
                        builder.append("'");
                        parameters.add(builder.toString());
                    }
                }
            }
        }

        for (String value : parameters) {
            sql = sql.replaceFirst("\\?", value);
        }
        return sql;
    }


    public static String beautifySql(String sql) {
        sql = sql.replaceAll("[\\s\n ]+", " ");
        return sql;
    }

    private static ThreadLocal<SimpleDateFormat> dateTimeFormatter = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };
}

