package com.certusnet.cassandra.spring_cassandra;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.hamcrest.core.IsEqual;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraConverterRowCallback;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.certusnet.cassandra.spring_cassandra.domain.AlarmSn;
import com.certusnet.cassandra.spring_cassandra.mapper.AlarmRowMapper;
import com.certusnet.cassandra.spring_cassandra.utils.CassandraPaging;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:spring-cassandra.xml"})
public class CassandraTemplateIntegrationTest {

	private static final Logger logger = LoggerFactory.getLogger(CassandraTemplateIntegrationTest.class);
	
    @Autowired
    private CassandraOperations cassandraTemplate;
    
    @Autowired
    private CassandraPaging cassandraPaging;

    @Test
    public void supportsPojoToCqlMappings() {
    	AlarmSn alarmSn = new AlarmSn();
    	alarmSn.setSn("sn_spring1");
    	alarmSn.setBeginDate(20151105);
    	alarmSn.setBeginTime(new Date());
        cassandraTemplate.insert(alarmSn);
        System.err.println("insert alarmSn : " + alarmSn);
        
        Select select = QueryBuilder.select().from("ott_alarm").where(QueryBuilder.eq("sn", "sn_spring1")).and(QueryBuilder.eq("begin_date", 20151105)).limit(1);

        AlarmSn retrievedAlarmSn = cassandraTemplate.selectOne(select, AlarmSn.class);
        System.err.println("selectOne retrievedAlarmSn : " + retrievedAlarmSn);
        
        System.err.println("retrievedAlarmSn is equal alarmSn : " + retrievedAlarmSn.equals(alarmSn));

        List<AlarmSn> retrievedAlarmSns = cassandraTemplate.select(select, AlarmSn.class);
        System.err.println("select retrievedAlarmSns : " + retrievedAlarmSns);
        
    }
    
    @Test
    public void supportPaginAutoByPreStatement(){
    	Session session = cassandraTemplate.getSession();
    	
    	session.getCluster().getConfiguration().getQueryOptions().setFetchSize(10);
    	PreparedStatement pst = session.prepare("select * from ott_alarm where sn=? and begin_date=?");
    	BoundStatement bound = pst.bind("sn1", 20151016);
    	ResultSet rs = session.execute(bound);
    	PagingState pagingState = rs.getExecutionInfo().getPagingState();
    	int remaining = rs.getAvailableWithoutFetching();
    	while(rs.iterator().hasNext()){
    		System.err.println(rs.iterator().next().getObject("begin_time"));
    		if (--remaining == 0) {
    	        break;
    	    }
    	}
    	
    	//最后一页时，pagingState为null
//    	if(pagingState != null){
//    		pagingState.toString();
//    	}
    	
    	String string = pagingState.toString();
    	PagingState pagingStateNext = PagingState.fromString(string);
    	
    	PreparedStatement pstNext = session.prepare("select * from ott_alarm where sn=? and begin_date=?");
    	BoundStatement boundNext = new BoundStatement(pstNext);
    	boundNext.bind("sn1", 20151016);
    	
    	boundNext.setPagingState(pagingStateNext);
    	ResultSet rsNext = session.execute(boundNext);
    	int remainingNext = rsNext.getAvailableWithoutFetching();
    	while(rsNext.iterator().hasNext()){
    		System.out.println(rsNext.iterator().next().getObject("begin_time"));
    		if (--remainingNext == 0) {
				break;
			}
    	}
    	
    }
    
    @Test
    public void supportPaging(){
    	Session session = cassandraTemplate.getSession();
    	
    	session.getCluster().getConfiguration().getQueryOptions().setFetchSize(10);
//    	Statement st = session.newSimpleStatement("select * from ott_alarm where sn='sn1' and begin_date=20151016");
    	Statement st = new SimpleStatement("select * from ott_alarm where sn='sn1' and begin_date=20151016");
    	//Statement st = session.execute("select * from ott_alarm where sn='sn1' and begin_date=20151016");
    	//Statement st = QueryBuilder.select().from("ott_alarm").where(QueryBuilder.eq("sn", "sn1")).and(QueryBuilder.eq("begin_date", 20151016));
//    	st.setFetchSize(10);
    	ResultSet rs = session.execute(st);
    	PagingState pagingState = rs.getExecutionInfo().getPagingState();
    	int remaining = rs.getAvailableWithoutFetching();
    	while(rs.iterator().hasNext()){
    		System.err.println(rs.iterator().next().getObject("begin_time"));
    		if (--remaining == 0) {
    	        break;
    	    }
    	}
    	
    	//最后一页时，pagingState为null
    	if(pagingState != null){
    		pagingState.toString();
    	}
    	
    	String string = pagingState.toString();
    	PagingState pagingStateNext = PagingState.fromString(string);
//    	Statement stNext = session.newSimpleStatement("select * from ott_alarm where sn='sn1' and begin_date=20151016");
    	Statement stNext = new SimpleStatement("select * from ott_alarm where sn='sn1' and begin_date=20151016");
    	//Statement stNext = QueryBuilder.select().from("ott_alarm").where(QueryBuilder.eq("sn", "sn1")).and(QueryBuilder.eq("begin_date", 20151016));
    	//PreparedStatement st = session.prepare("select * from ott_alarm where sn='sn1' and begin_date=20151016");
    	stNext.setPagingState(pagingStateNext);
//    	stNext.setFetchSize(10);
    	ResultSet rsNext = session.execute(stNext);
    	int remainingNext = rsNext.getAvailableWithoutFetching();
    	while(rsNext.iterator().hasNext()){
    		System.out.println(rsNext.iterator().next().getObject("begin_time"));
    		if (--remainingNext == 0) {
				break;
			}
    	}
    }
    
    @Test
    public void supportPaginAutoByUtils(){
    	Session session = cassandraTemplate.getSession();
    	PreparedStatement pst = session.prepare("select * from ott_alarm where sn=? and begin_date=?");
    	BoundStatement bound = pst.bind("sn1", 20151016);
    	bound.setFetchSize(10);
    	
    	CassandraPaging cp = new CassandraPaging(session);
    	List<AlarmSn> alarmSnList = cp.fetchRowsWithPage(bound, null, new AlarmRowMapper());
    	for(AlarmSn alarmSn : alarmSnList){
    		System.err.println(alarmSn);
    	}
    	
    	String string = cp.getPagingState().toString();
    	PagingState pagingStateNext = PagingState.fromString(string);
    	
    	
    	List<AlarmSn> alarmSnNextList = cp.fetchRowsWithPage(bound, pagingStateNext, new AlarmRowMapper());
    	for(AlarmSn alarmSn : alarmSnNextList){
    		System.out.println(alarmSn);
    	}
    	
    }
    
    @Test
    public void supportPaginAutoByUtils2(){
    	List<AlarmSn> result1 = cassandraPaging.fetchRowsWithPage("select * from ott_alarm where sn=? and begin_date=?", new Object[]{"sn1", 20151016}, 1, AlarmSn.class);
    	for(int i = 0; i < result1.size(); i++){
    		System.err.println(result1.get(i));
    	}
    	
    	List<AlarmSn> result2 = cassandraPaging.fetchRowsWithPage("select * from ott_alarm where sn=? and begin_date=?", new Object[]{"sn1", 20151016}, 2, AlarmSn.class);
    	for(AlarmSn alarmSn : result2){
    		System.out.println(alarmSn);
    	}
    	
    	
    	
    	
//    	Session session = cassandraTemplate.getSession();
//    	PreparedStatement pst = session.prepare("select * from ott_alarm where sn=? and begin_date=?");
//    	BoundStatement bound = pst.bind("sn1", 20151016);
//    	
//    	CassandraPaging cp = new CassandraPaging(session);
//    	List<Row> alarmSnList = cp.fetchRowsWithPage(bound, null);
//    	CassandraConverterRowCallback<AlarmSn> readRowCallback = new CassandraConverterRowCallback<AlarmSn>(cassandraTemplate.getConverter(), AlarmSn.class);
//    	List<AlarmSn> result = new ArrayList<AlarmSn>();
//    	for (Row row : alarmSnList) {
//			result.add(readRowCallback.doWith(row));
//		}
//    	
//    	String string = cp.getPagingState().toString();
//    	PagingState pagingStateNext = PagingState.fromString(string);
//    	
//    	
//    	List<AlarmSn> alarmSnNextList = cp.fetchRowsWithPage(bound, pagingStateNext, new AlarmRowMapper());
//    	for(AlarmSn alarmSn : alarmSnNextList){
//    		System.err.println(alarmSn);
//    	}
    	
    }
    
    public void supportPaging2AlarmSn(){
    	
    	
    }
}
