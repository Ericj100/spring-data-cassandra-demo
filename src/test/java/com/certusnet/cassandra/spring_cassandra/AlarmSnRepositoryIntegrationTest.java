package com.certusnet.cassandra.spring_cassandra;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.swing.plaf.ListUI;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.certusnet.cassandra.spring_cassandra.domain.AlarmSn;
import com.certusnet.cassandra.spring_cassandra.domain.AlarmSnRepository;
import com.certusnet.cassandra.spring_cassandra.utils.DateUtil;
import com.google.common.collect.ImmutableSet;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:spring-cassandra.xml"})
public class AlarmSnRepositoryIntegrationTest {

    @Autowired
    private AlarmSnRepository alarmSnRepository;
    
//    @Autowired
//    private AlarmPageRepository alarmPageRepository;
    
    @Autowired
    private CassandraOperations cassandraTemplate;
    
    @Test
    public void repositoryStoresAndRetrievesAlarmSns() {
    	AlarmSn alarmSn1 = new AlarmSn();
    	alarmSn1.setSn("sn_spring3");
    	alarmSn1.setBeginDate(20151105);
    	alarmSn1.setBeginTime(DateUtil.getDateByString("2015-11-05 15:39:10"));
    	AlarmSn alarmSn2 = new AlarmSn();
    	alarmSn2.setSn("sn_spring3");
    	alarmSn2.setBeginDate(20151105);
    	alarmSn2.setBeginTime(DateUtil.getDateByString("2015-11-05 15:39:11"));
    	
    	alarmSnRepository.save(ImmutableSet.of(alarmSn1, alarmSn2));
    	
//        Iterable<AlarmSn> alarmSns = alarmSnRepository.findBySnAndTime("sn_spring3", DateUtil.transDateToInt(new Date()));
//
//        assertThat(alarmSns, hasItem(alarmSn1));
//        assertThat(alarmSns, hasItem(alarmSn2));
    }

    @Test
    public void repositoryDeletesStoredEvents() {
    	AlarmSn alarmSn1 = new AlarmSn();
    	alarmSn1.setSn("sn_spring3");
    	alarmSn1.setBeginDate(20151105);
    	alarmSn1.setBeginTime(DateUtil.getDateByString("2015-11-05 15:39:10"));
    	AlarmSn alarmSn2 = new AlarmSn();
    	alarmSn2.setSn("sn_spring3");
    	alarmSn2.setBeginDate(20151105);
    	alarmSn2.setBeginTime(DateUtil.getDateByString("2015-11-05 15:39:11"));
    	
    	alarmSnRepository.save(ImmutableSet.of(alarmSn1, alarmSn2));

    	alarmSnRepository.delete(alarmSn1);
    	alarmSnRepository.delete(alarmSn2);

        Iterable<AlarmSn> alarmSns = alarmSnRepository.findBySnAndTime("sn_spring3", DateUtil.transDateToInt(new Date()));

        assertThat(alarmSns, not(hasItem(alarmSn1)));
        assertThat(alarmSns, not(hasItem(alarmSn2)));
    }
    
    @Test
    public void repositoryPaging(){
    	//首页
    	List<AlarmSn> alarmSnList = alarmSnRepository.findFirstPageBySnAndTime("sn1", 20151016, DateUtil.getDateByString("2015-10-16 00:00:00"), DateUtil.getDateByString("2015-10-16 23:59:59"));
    	for(AlarmSn as : alarmSnList){
    		System.err.println(as);
    	}
    	
    	//下一页
    	List<AlarmSn> alarmSnNext1List = alarmSnRepository.findNextPageBySnAndTime("sn1", 20151016, alarmSnList.get(9).getBeginTime(), DateUtil.getDateByString("2015-10-16 23:59:59"));
    	for(AlarmSn as : alarmSnNext1List){
    		System.out.println(as);
    	}
    	
    	//下一页
    	List<AlarmSn> alarmSnNext2List = alarmSnRepository.findNextPageBySnAndTime("sn1", 20151016, alarmSnNext1List.get(9).getBeginTime(), DateUtil.getDateByString("2015-10-16 23:59:59"));
    	for(AlarmSn as : alarmSnNext2List){
    		System.err.println(as);
    	}
    	
    	//上一页
    	List<AlarmSn> alarmSnsPre = alarmSnRepository.findPrePageBySnAndTime("sn1", 20151016, DateUtil.getDateByString("2015-10-16 00:00:00"), alarmSnNext2List.get(0).getBeginTime());
    	Collections.reverse(alarmSnsPre);
    	for(AlarmSn as : alarmSnsPre){
    		System.out.println(as);
    	}
    	
    }
    
    
    
    
    @Test
    public void repositoryPagingByAuto(){
    	cassandraTemplate.getSession().getCluster().getConfiguration().getQueryOptions().setFetchSize(10);
    	
    	List<AlarmSn> alarmSnList = alarmSnRepository.findFirstPageBySnAndTime("sn1", 20151016, DateUtil.getDateByString("2015-10-16 00:00:00"), DateUtil.getDateByString("2015-10-16 23:59:59"));
    	for(AlarmSn as : alarmSnList){
    		System.err.println(as);
    	}
    	
    	
    }
    
    @Test
    public void repositoryPagingByPageRepository(){
    	//neither slice nor page queries are supported yet
    	//目前还不支持
    	//System.err.println(alarmPageRepository);
    }
}
