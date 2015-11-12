package com.certusnet.cassandra.spring_cassandra;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.certusnet.cassandra.spring_cassandra.domain.AlarmSn;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:spring-cassandra.xml"})
public class CqlTemplateIntegrationTest {

	private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Autowired
    private CqlOperations cqlTemplate;

    @Test
    public void allowsExecutingCqlStatements() {
//        insertEventUsingCqlString();
//        insertEventUsingStatementBuildWithQueryBuilder();
//        insertEventUsingPreparedStatement();

        ResultSet resultSet1 = cqlTemplate.query("select * from ott_alarm where sn='sn_spring2' and begin_date=20151105");
        System.err.println("result1 size : " + resultSet1.all().size());

        List<AlarmSn> alarmSnList = cqlTemplate.processList(resultSet1, AlarmSn.class);
        for(AlarmSn alarmSn : alarmSnList){
        	 System.err.println(alarmSn);
        }
//        Select select = QueryBuilder.select().from("ott_alarm").where(QueryBuilder.eq("sn", "sn_spring2")).and(QueryBuilder.eq("begin_date", 20151105)).limit(10);
//        ResultSet resultSet2 = cqlTemplate.query(select);
//
//        System.err.println("result2 size : " + resultSet2.all().size());
        
    }

    private void insertEventUsingCqlString() {
    	String cql = "insert into ott_alarm (sn, begin_date, begin_time) values ('sn_spring2', 20151105, '" + df.format(new Date()) + "')";
        cqlTemplate.execute(cql);
    }

    private void insertEventUsingStatementBuildWithQueryBuilder() {
//        Insert insertStatement = QueryBuilder.insertInto("ott_alarm").value("sn", "sn_spring2").value("begin_date", 20151105)
//                .value("begin_time", new Date());
//        cqlTemplate.execute(insertStatement);
    }

    private void insertEventUsingPreparedStatement() {
        PreparedStatement preparedStatement = cqlTemplate.getSession().prepare("insert into ott_alarm (sn, begin_date, begin_time) values  (?, ?, ?)");
        Statement insertStatement = preparedStatement.bind("sn_spring2", 20151105, new Date());
        cqlTemplate.execute(insertStatement);
        
    }
}
