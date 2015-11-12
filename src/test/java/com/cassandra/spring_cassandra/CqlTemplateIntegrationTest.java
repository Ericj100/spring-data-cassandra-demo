package com.cassandra.spring_cassandra;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.CqlOperations;

import com.cassandra.spring_cassandra.domain.AlarmSn;
import com.cassandra.spring_cassandra.mapper.AlarmRowMapper;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CqlTemplateIntegrationTest extends BaseIntegrationTest{

	private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
    @Autowired
    private CqlOperations cqlTemplate;

    @Test
    public void allowsExecutingCqlStatements() {
    	//insert data
        insertAlarmSnUsingCqlString();
        insertAlarmSnUsingStatementBuildWithQueryBuilder();
        insertAlarmUsingPreparedStatement();

        //query data
        ResultSet resultSet1 = cqlTemplate.query("select * from alarm where sn='sn_spring4' and begin_date=20151112");
        
        List<AlarmSn> alarmSnList = cqlTemplate.process(resultSet1, new AlarmRowMapper());
        for(AlarmSn alarmSn : alarmSnList){
        	System.err.println(alarmSn);
        }
        
        //update data
        cqlTemplate.execute("update alarm set account = 'Eric' where sn='sn_spring4' and begin_date=20151112 and begin_time='" + df.format(alarmSnList.get(0).getBeginTime()) + "'");
    }
    
    /**
     * 使用cql语言插入数据
     * @author Eric
     * @date 2015年11月12日
     */
    private void insertAlarmSnUsingCqlString() {
    	String cql = "insert into alarm (sn, begin_date, begin_time) values ('sn_spring4', 20151112, '" + df.format(new Date()) + "')";
        cqlTemplate.execute(cql);
    }

    /**
     * 使用QueryBuilder插入数据
     * @author Eric
     * @date 2015年11月12日
     */
    private void insertAlarmSnUsingStatementBuildWithQueryBuilder() {
        Insert insertStatement = QueryBuilder.insertInto("alarm").value("sn", "sn_spring4").value("begin_date", 20151112)
                .value("begin_time", new Date());
        cqlTemplate.execute(insertStatement);
    }

    /**
     * 使用PreparedStatement插入数据
     * @author Eric
     * @date 2015年11月12日
     */
    private void insertAlarmUsingPreparedStatement() {
        PreparedStatement preparedStatement = cqlTemplate.getSession().prepare("insert into alarm (sn, begin_date, begin_time) values  (?, ?, ?)");
        Statement insertStatement = preparedStatement.bind("sn_spring4", 20151112, new Date());
        cqlTemplate.execute(insertStatement);
        
    }
}
