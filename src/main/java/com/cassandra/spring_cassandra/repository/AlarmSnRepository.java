package com.cassandra.spring_cassandra.repository;

import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;

import com.cassandra.spring_cassandra.domain.AlarmSn;

public interface AlarmSnRepository extends CassandraRepository<AlarmSn> {

	@Query("select * from ott_alarm where sn = ?0 and begin_date=?1")
    List<AlarmSn> findBySnAndTime(String sn, Integer beginDate);
	
//	@Query("select * from ott_alarm where sn = ?0 and begin_date=?1 and begin_time >= ?2 and begin_time <= ?3 limit 10")
//	List<AlarmSn> findFirstPageBySnAndTime(String sn, Integer beginDate, Date beginTime1, Date beginTime2);
//	
//	@Query("select * from ott_alarm where sn = ?0 and begin_date=?1 and begin_time > ?2 and begin_time <= ?3 limit 10")
//	List<AlarmSn> findNextPageBySnAndTime(String sn, Integer beginDate, Date beginTime1, Date beginTime2);
//	
//	@Query("select * from ott_alarm where sn = ?0 and begin_date=?1 and begin_time >= ?2 and begin_time < ?3 order by begin_time desc limit 10")
//	List<AlarmSn> findPrePageBySnAndTime(String sn, Integer beginDate, Date beginTime1, Date beginTime2);
	
}
