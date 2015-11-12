package com.certusnet.cassandra.spring_cassandra.utils;
import io.netty.buffer.ByteBufUtil;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
 










import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.RowMapper;
import org.springframework.cassandra.support.exception.CassandraTypeMismatchException;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraConverterRowCallback;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Component;

import com.certusnet.cassandra.spring_cassandra.domain.AlarmSn;
import com.certusnet.cassandra.spring_cassandra.domain.Pager;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
 
/**
 * 
 * The solution of skipping rows is that use page state rather than iterator
 * rows one by one.
 *
 */
public class CassandraPaging {
 
	private CassandraOperations cassandraTemplate;
	
	private RedisCommons redisCommons;
	
	private Integer fetchSize = 100;
	
	private Integer pageSize = 10;
	
	private Session session;
	
	private PagingState pagingState;
	 
	public CassandraPaging() {
    }
	
	
    public CassandraPaging(Session session) {
        this.session = session;
    }
	
    public <T> List<T> fetchRowsWithPage(Statement statement, PagingState pagingState, RowMapper<T> rowMapper) {
    	if(pagingState != null){
    		statement.setPagingState(pagingState);
    	}
    	ResultSet rs = session.execute(statement);
    	setPagingState(rs.getExecutionInfo().getPagingState());
    	
    	List<T> mappedRows = new ArrayList<T>();
    	int remainingNext = rs.getAvailableWithoutFetching();
    	int rowNum = 0;
    	while(rs.iterator().hasNext()){
    		mappedRows.add(rowMapper.mapRow(rs.iterator().next(), rowNum++));
    		if (--remainingNext == 0) {
				break;
			}
    	}
    	
    	return mappedRows;
    }
    
    public List<Row> fetchRowsWithPage(Statement statement, PagingState pagingState) {
    	if(pagingState != null){
    		statement.setPagingState(pagingState);
    	}
    	ResultSet rs = session.execute(statement);
    	
    	List<Row> mappedRows = new ArrayList<Row>();
    	int remainingNext = rs.getAvailableWithoutFetching();
    	while(rs.iterator().hasNext()){
    		mappedRows.add(rs.iterator().next());
    		if (--remainingNext == 0) {
				break;
			}
    	}
    	
    	setPagingState(rs.getExecutionInfo().getPagingState());
    	
    	return mappedRows;
    }
    
    public <T> List<T> fetchRowsWithPage(String cql, Object[] params, Integer pageIndex, Class<T> type) {
    	
    	Session session = cassandraTemplate.getSession();
    	PreparedStatement pst = session.prepare("select * from ott_alarm where sn=? and begin_date=?");
    	BoundStatement bound = pst.bind(params);
    	bound.setFetchSize(fetchSize);
    	//获取Statement的hash值
    	byte[] bytes = hash(bound);
    	String key = new String(bytes);
    	
    	Integer i = pageIndex * pageSize / fetchSize;
    	if(redisCommons.exist(key + ".list" + i)){
    		return redisCommons.lrange(key + ".list" + i, pageIndex % pageSize == 0 ? pageSize : pageIndex % pageSize, pageSize);
    	}
    	
    	if(i != 0){
    		String pagingStatString = redisCommons.get(key + ".pagingState");
    		bound.setPagingState(PagingState.fromString(pagingStatString));
    	}
    	
    	ResultSet rs = session.execute(bound);
    	PagingState paingState = rs.getExecutionInfo().getPagingState();
    	System.err.println(paingState);
    	CassandraConverterRowCallback<T> readRowCallback = new CassandraConverterRowCallback<T>(cassandraTemplate.getConverter(), type);
    	int remainingNext = rs.getAvailableWithoutFetching();
    	while(rs.iterator().hasNext()){
    		T t = readRowCallback.doWith(rs.iterator().next());
    		redisCommons.rpush(key + ".list" + i, t);
//    		result.add(readRowCallback.doWith(rs.iterator().next()));
    		if (--remainingNext == 0) {
				break;
			}
    	}
    	redisCommons.expire(key + ".list" + i, 10, TimeUnit.MINUTES);
    	redisCommons.set(key + ".pagingState", paingState.toString(), 10, TimeUnit.MINUTES);
    	
    	return redisCommons.lrange(key + ".list" + i, pageIndex % pageSize == 0 ? pageSize : pageIndex % pageSize, pageSize);
    }
    
    
    public <T> List<T> fetchRowsWithPage(Statement statement, Integer pageIndex, Class<T> type) {
    	
    	List<T> result = new ArrayList<T>();
    	
    	Session session = cassandraTemplate.getSession();
    	//获取Statement的hash值
    	byte[] bytes = hash(statement);
    	String key = new String(bytes);
    	
    	Integer i = pageIndex * pageSize / fetchSize;
    	if(redisCommons.exist(key + ".list" + i)){
    		return redisCommons.lrange(key + ".list" + i, pageIndex % pageSize == 0 ? pageSize : pageIndex % pageSize, pageSize);
    	}
    	
    	if(i != 0){
    		String pagingStatString = redisCommons.get(key + ".pagingState");
    		statement.setPagingState(PagingState.fromString(pagingStatString));
    	}
    	
    	ResultSet rs = session.execute(statement);
    	PagingState paingState = rs.getExecutionInfo().getPagingState();
    	System.err.println(paingState);
    	CassandraConverterRowCallback<T> readRowCallback = new CassandraConverterRowCallback<T>(cassandraTemplate.getConverter(), type);
    	int remainingNext = rs.getAvailableWithoutFetching();
    	while(rs.iterator().hasNext()){
    		T t = readRowCallback.doWith(rs.iterator().next());
    		redisCommons.rpush(key + ".list" + i, t);
//    		result.add(readRowCallback.doWith(rs.iterator().next()));
    		if (--remainingNext == 0) {
				break;
			}
    	}
    	redisCommons.expire(key + ".list" + i, 10, TimeUnit.MINUTES);
    	redisCommons.set(key + ".pagingState", paingState.toString(), 10, TimeUnit.MINUTES);
    	
    	return redisCommons.lrange(key + ".list" + i, pageIndex % pageSize == 0 ? pageSize : pageIndex % pageSize, pageSize);
    }
    
    
 
    /**
     * 对Statement进行hash
     * @param statement
     * @return
     */
    @SuppressWarnings("null")
	private byte[] hash(Statement statement) {
        byte[] digest;
        ByteBuffer[] values = null;
        MessageDigest md;
        assert !(statement instanceof BatchStatement);
        try {
            md = MessageDigest.getInstance("MD5");
            if (statement instanceof BoundStatement) {
                BoundStatement bs = ((BoundStatement)statement);
                String queryString = bs.preparedStatement().getQueryString();
                int paramSize = StringUtils.split(queryString, "?").length - 1;
                md.update(queryString.getBytes());
                for(int i = 0; i < paramSize; i ++){
                	md.update(String.valueOf(bs.getObject(i)).getBytes());
                }
            } else {
                //it is a RegularStatement since Batch statements are not allowed
                RegularStatement rs = (RegularStatement)statement;
                md.update(rs.getQueryString().getBytes());
                values = rs.getValues(ProtocolVersion.V3);
            }
            if (values != null) {
                for (ByteBuffer value : values) {
                    md.update(value.duplicate());
                }
            }
            digest = md.digest();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 doesn't seem to be available on this JVM", e);
        }
        return digest;
    }
    
    
    public PagingState getPagingState() {
		return pagingState;
	}

	public void setPagingState(PagingState pagingState) {
		this.pagingState = pagingState;
	}

	public CassandraOperations getCassandraTemplate() {
		return cassandraTemplate;
	}

	public void setCassandraTemplate(CassandraOperations cassandraTemplate) {
		this.cassandraTemplate = cassandraTemplate;
	}

	public RedisCommons getRedisCommons() {
		return redisCommons;
	}

	public void setRedisCommons(RedisCommons redisCommons) {
		this.redisCommons = redisCommons;
	}

	public <T> List<T> process(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException {
		List<T> mappedRows = new ArrayList<T>();
		try {
			int i = 0;
			for (Row row : resultSet.all()) {
				mappedRows.add(rowMapper.mapRow(row, i++));
			}
		} catch (DriverException dx) {
			new CassandraTypeMismatchException(dx.getMessage(), dx);
		}
		return mappedRows;
	}
    
}