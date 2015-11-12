package com.cassandra.spring_cassandra;

import java.util.HashMap;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.cassandra.spring_cassandra.domain.AlarmSn;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:spring-cassandra.xml"})
public abstract class BaseIntegrationTest {

	@Autowired
    private CassandraOperations cassandraTemplate;

    @Before
    public void resetKeySpace() {
    	CassandraAdminTemplate adminOperations = new CassandraAdminTemplate(cassandraTemplate.getSession(), cassandraTemplate.getConverter());
    	
    	try {
    		//when table is not exist, this operation will throw a exception
			adminOperations.dropTable(CqlIdentifier.cqlId("alarm"));
		} catch (Exception e) {
			e.printStackTrace();
		}
    	adminOperations.createTable(true, CqlIdentifier.cqlId("alarm"), AlarmSn.class, new HashMap<String, Object>());
    }
}
