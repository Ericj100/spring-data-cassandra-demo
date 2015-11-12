package com.cassandra.spring_cassandra;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.cassandra.spring_cassandra.domain.AlarmSn;
import com.cassandra.spring_cassandra.mapper.AlarmRowMapper;
import com.cassandra.spring_cassandra.utils.CassandraPaging;
import com.cassandra.spring_cassandra.utils.CassandraRedisPaging;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

public class CassandraTemplateIntegrationTest extends BaseIntegrationTest{

    @Autowired
    private CassandraOperations cassandraTemplate;
    
    @Autowired
    private CassandraRedisPaging cassandraRedisPaging;

    @Test
    public void supportsPojoToCqlMappings() {
    	AlarmSn alarmSn = new AlarmSn();
    	alarmSn.setSn("sn_spring1");
    	alarmSn.setBeginDate(20151112);
    	alarmSn.setBeginTime(new Date());
        cassandraTemplate.insert(alarmSn);
        
        Select select = QueryBuilder.select().from("alarm").where(QueryBuilder.eq("sn", "sn_spring1")).and(QueryBuilder.eq("begin_date", 20151112)).limit(1);

        AlarmSn retrievedAlarmSn = cassandraTemplate.selectOne(select, AlarmSn.class);
        System.err.println("selectOne retrievedAlarmSn : " + retrievedAlarmSn);
        
        System.err.println("retrievedAlarmSn is equal alarmSn : " + retrievedAlarmSn.equals(alarmSn));

    }
    
    /**
     * 通过PreparedStatement和PagingState，分页查询
     * @author Eric
     * @date 2015年11月12日
     *
     */
    @Test
    public void supportPaginByPreStatement(){
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
    
    /**
     * 通过SimpleStatement和PagingState，分页查询
     * @author Eric
     * @date 2015年11月12日
     *
     */
    @Test
    public void supportPaging(){
    	//init data
    	insert2Paging();
    	
    	Session session = cassandraTemplate.getSession();
    	Statement st = new SimpleStatement("select * from alarm where sn='sn_spring1' and begin_date=20151112");
    	st.setFetchSize(10);
    	ResultSet rs = session.execute(st);
    	PagingState pagingState = rs.getExecutionInfo().getPagingState();
    	//print first page
    	int remaining = rs.getAvailableWithoutFetching();
    	while(rs.iterator().hasNext()){
    		System.err.println(rs.iterator().next().getObject("begin_time"));
    		if (--remaining == 0) {
    	        break;
    	    }
    	}
    	
    	//get paginState for next page
    	String string = pagingState.toString();
    	PagingState pagingStateNext = PagingState.fromString(string);
    	
    	Statement stNext = new SimpleStatement("select * from alarm where sn='sn_spring1' and begin_date=20151112");
    	stNext.setPagingState(pagingStateNext);
    	stNext.setFetchSize(10);
    	ResultSet rsNext = session.execute(stNext);
    	//print next page
    	int remainingNext = rsNext.getAvailableWithoutFetching();
    	while(rsNext.iterator().hasNext()){
    		System.out.println(rsNext.iterator().next().getObject("begin_time"));
    		if (--remainingNext == 0) {
				break;
			}
    	}
    }
    
    /**
     * 通过CassandraPaging分页
     * @author Eric
     * @date 2015年11月12日
     *
     */
    @Test
    public void supportPaginAutoByUtils(){
    	//init data
    	insert2Paging();
    	
    	Session session = cassandraTemplate.getSession();
    	PreparedStatement pst = session.prepare("select * from alarm where sn=? and begin_date=?");
    	BoundStatement bound = pst.bind("sn_spring1", 20151112);
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
    
    /**
     * 通过CassandraRedisPaging分页
     * @author Eric
     * @date 2015年11月12日
     *
     */
    @Test
    public void supportPaginAutoByUtils2(){
    	//init data
    	insert2Paging();
    	
    	//查询第一页
    	List<AlarmSn> result1 = cassandraRedisPaging.fetchRowsWithPage("select * from alarm where sn=? and begin_date=?", new Object[]{"sn_spring1", 20151112}, 1, AlarmSn.class);
    	for(int i = 0; i < result1.size(); i++){
    		System.err.println(result1.get(i));
    	}
    	
    	//查询第二页
    	List<AlarmSn> result2 = cassandraRedisPaging.fetchRowsWithPage("select * from alarm where sn=? and begin_date=?", new Object[]{"sn_spring1", 20151112}, 2, AlarmSn.class);
    	for(AlarmSn alarmSn : result2){
    		System.out.println(alarmSn);
    	}
    	
    }
    
    /**
     * init data for paging
     * @author Eric
     * @date 2015年11月12日
     *
     */
    private void insert2Paging(){
    	Date date = new Date();
    	for(int i = 0; i < 20; i++){
    		AlarmSn alarmSn = new AlarmSn();
        	alarmSn.setSn("sn_spring1");
        	alarmSn.setBeginDate(20151112);
        	alarmSn.setBeginTime(DateUtils.addSeconds(date, i));
            cassandraTemplate.insert(alarmSn);
    	}
    }
}
