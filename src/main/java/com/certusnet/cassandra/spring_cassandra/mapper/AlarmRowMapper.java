package com.certusnet.cassandra.spring_cassandra.mapper;

import java.util.List;

import org.springframework.cassandra.core.RowMapper;

import com.certusnet.cassandra.spring_cassandra.domain.AlarmSn;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;

public class AlarmRowMapper implements RowMapper<AlarmSn> {

	@Override
	public AlarmSn mapRow(Row row, int rowNum) throws DriverException {
		AlarmSn alarmSn = new AlarmSn();
		alarmSn.setSn(row.getString("sn"));
		alarmSn.setBeginDate(row.getInt("begin_date"));
		alarmSn.setBeginTime(row.getDate("begin_time"));
		return alarmSn;
	}

}
