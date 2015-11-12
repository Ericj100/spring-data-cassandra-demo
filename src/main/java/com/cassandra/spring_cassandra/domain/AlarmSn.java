package com.cassandra.spring_cassandra.domain;

import java.io.Serializable;
import java.util.Date;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

@Table("alarm")
public class AlarmSn implements Serializable{

	private static final long serialVersionUID = 6435825007045975300L;

	@PrimaryKeyColumn(name = "begin_date", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private Integer beginDate;
	
	@PrimaryKeyColumn(name = "sn", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
	private String sn;
	
	@Column("district_id")
	private Integer districtId;
	
	@Column("platform_id")
	private Integer platformId;
	
	@Column("account")
	private String account;
	
	@PrimaryKeyColumn(name = "begin_time", ordinal = 3, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
	private Date beginTime;
	
	@Column("alarm_type")
	private Integer alarmType;
	
	@Column("alarm_desc")
	private String alarmDesc;

	public Integer getBeginDate() {
		return beginDate;
	}

	public void setBeginDate(Integer beginDate) {
		this.beginDate = beginDate;
	}

	public Integer getDistrictId() {
		return districtId;
	}

	public void setDistrictId(Integer districtId) {
		this.districtId = districtId;
	}

	public Integer getPlatformId() {
		return platformId;
	}

	public void setPlatformId(Integer platformId) {
		this.platformId = platformId;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public String getSn() {
		return sn;
	}

	public void setSn(String sn) {
		this.sn = sn;
	}

	public Date getBeginTime() {
		return beginTime;
	}

	public void setBeginTime(Date beginTime) {
		this.beginTime = beginTime;
	}

	public Integer getAlarmType() {
		return alarmType;
	}

	public void setAlarmType(Integer alarmType) {
		this.alarmType = alarmType;
	}

	public String getAlarmDesc() {
		return alarmDesc;
	}

	public void setAlarmDesc(String alarmDesc) {
		this.alarmDesc = alarmDesc;
	}

	@Override
	public String toString() {
		return "AlarmSn [beginDate=" + beginDate + ", sn=" + sn
				+ ", districtId=" + districtId + ", platformId=" + platformId
				+ ", account=" + account + ", beginTime=" + beginTime
				+ ", alarmType=" + alarmType + ", alarmDesc=" + alarmDesc + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((beginTime == null) ? 0 : beginTime.hashCode());
		result = prime * result + ((sn == null) ? 0 : sn.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AlarmSn other = (AlarmSn) obj;
		if (beginTime == null) {
			if (other.beginTime != null)
				return false;
		} else if (!beginTime.equals(other.beginTime))
			return false;
		if (sn == null) {
			if (other.sn != null)
				return false;
		} else if (!sn.equals(other.sn))
			return false;
		return true;
	}
	
}
