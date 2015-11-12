package com.cassandra.spring_cassandra.utils;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.springframework.cassandra.core.RowMapper;
import org.springframework.cassandra.support.exception.CassandraTypeMismatchException;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraConverterRowCallback;
import org.springframework.data.cassandra.core.CassandraOperations;

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
 
/**
 * 
 * The solution of skipping rows is that use page state rather than iterator
 * rows one by one.
 *
 */
public class CassandraPaging {
 
	
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
    
 
    
    public PagingState getPagingState() {
		return pagingState;
	}

	public void setPagingState(PagingState pagingState) {
		this.pagingState = pagingState;
	}
    
}