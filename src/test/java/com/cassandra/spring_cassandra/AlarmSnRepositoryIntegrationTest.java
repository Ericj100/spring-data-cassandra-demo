package com.cassandra.spring_cassandra;

import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.cassandra.spring_cassandra.domain.AlarmSn;
import com.cassandra.spring_cassandra.repository.AlarmSnRepository;
import com.cassandra.spring_cassandra.utils.DateUtil;
import com.google.common.collect.ImmutableSet;

public class AlarmSnRepositoryIntegrationTest extends BaseIntegrationTest{

    @Autowired
    private AlarmSnRepository alarmSnRepository;
    
//    @Autowired
//    private AlarmPageRepository alarmPageRepository;
    
    @Autowired
    private CassandraOperations cassandraTemplate;
    
    /**
     * 保存并获取数据
     * @author Eric
     * @date 2015年11月12日
     *
     */
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
    	
    	//save
    	alarmSnRepository.save(ImmutableSet.of(alarmSn1, alarmSn2));
    	
    	//query
        List<AlarmSn> alarmSns = alarmSnRepository.findBySnAndTime("sn_spring3", 20151105);
        for(AlarmSn alarmSn : alarmSns){
        	System.err.println(alarmSn);
        }
        
        //update
        alarmSn2.setAccount("Eric");
        alarmSnRepository.save(alarmSn2);
    }

    /**
     * 删除数据
     * @author Eric
     * @date 2015年11月12日
     *
     */
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
    	
        List<AlarmSn> alarmSns = alarmSnRepository.findBySnAndTime("sn_spring3", DateUtil.transDateToInt(new Date()));
        System.err.println("retrieve size is : " + alarmSns.size());
    }
    
//    @Test
    public void repositoryPaging(){
    	//首页
//    	List<AlarmSn> alarmSnList = alarmSnRepository.findFirstPageBySnAndTime("sn1", 20151016, DateUtil.getDateByString("2015-10-16 00:00:00"), DateUtil.getDateByString("2015-10-16 23:59:59"));
//    	for(AlarmSn as : alarmSnList){
//    		System.err.println(as);
//    	}
//    	
//    	//下一页
//    	List<AlarmSn> alarmSnNext1List = alarmSnRepository.findNextPageBySnAndTime("sn1", 20151016, alarmSnList.get(9).getBeginTime(), DateUtil.getDateByString("2015-10-16 23:59:59"));
//    	for(AlarmSn as : alarmSnNext1List){
//    		System.out.println(as);
//    	}
//    	
//    	//下一页
//    	List<AlarmSn> alarmSnNext2List = alarmSnRepository.findNextPageBySnAndTime("sn1", 20151016, alarmSnNext1List.get(9).getBeginTime(), DateUtil.getDateByString("2015-10-16 23:59:59"));
//    	for(AlarmSn as : alarmSnNext2List){
//    		System.err.println(as);
//    	}
//    	
//    	//上一页
//    	List<AlarmSn> alarmSnsPre = alarmSnRepository.findPrePageBySnAndTime("sn1", 20151016, DateUtil.getDateByString("2015-10-16 00:00:00"), alarmSnNext2List.get(0).getBeginTime());
//    	Collections.reverse(alarmSnsPre);
//    	for(AlarmSn as : alarmSnsPre){
//    		System.out.println(as);
//    	}
    	
    }
    
    
    
    
    @Test
    public void repositoryPagingByAuto(){
//    	cassandraTemplate.getSession().getCluster().getConfiguration().getQueryOptions().setFetchSize(10);
//    	
//    	List<AlarmSn> alarmSnList = alarmSnRepository.findFirstPageBySnAndTime("sn1", 20151016, DateUtil.getDateByString("2015-10-16 00:00:00"), DateUtil.getDateByString("2015-10-16 23:59:59"));
//    	for(AlarmSn as : alarmSnList){
//    		System.err.println(as);
//    	}
    	
    	
    }
    
}
