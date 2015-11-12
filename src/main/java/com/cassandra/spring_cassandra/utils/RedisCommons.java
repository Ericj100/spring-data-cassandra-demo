package com.cassandra.spring_cassandra.utils;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.BoundValueOperations;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * redis公共类.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class RedisCommons {

	private RedisTemplate redisTemplate;
	
	
	/**
     * 添加redis缓存
     * @author jijun
     * @date 2014年7月10日
     * @param redisKey
     * @param object
     */
    public void set(String redisKey, String object, long timeout, TimeUnit unit){
    	BoundValueOperations operations = redisTemplate.boundValueOps(redisKey);
    	operations.set(object, timeout, unit);
    }
    
    /**
     * 获取redis非list缓存
     * @author jijun
     * @date 2014年7月10日
     * @param redisKey
     * @param clazz
     * @return
     */
    public String get(String redisKey){
    	String object = (String) redisTemplate.opsForValue().get(redisKey);
    	if(StringUtils.isBlank(object)){
			return null;
		}
    	return object;
    }
	
	
    /**
     * 添加redis的List缓存
     * @author jijun
     * @date 2014年12月29日
     * @param redisKey
     * @param timeout 失效时间
     * @param unit 时间单位
     * @param objects
     */
    public void rpush(String redisKey, Object object){
    	BoundListOperations operations = redisTemplate.boundListOps(redisKey);
		operations.rightPush(object);
		
    }
    
    public void expire(String key, final long timeout, final TimeUnit unit){
    	redisTemplate.expire(key, timeout, unit);
    }
    
    /**
     * 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 end 指定:[0, 10]
     * @author jijun
     * @param <T>
     * @date 2014年12月29日
     * @param redisKey
     * @param object
     */
    public <T> List<T> lrange(String redisKey, long pageIndex, long pageSize){
    	long start = (pageIndex-1)*pageSize;
		long end = start + pageSize - 1;
		BoundListOperations operations = redisTemplate.boundListOps(redisKey);
		return operations.range(start, end);
    }
    
    /**
     * 是否存在key
     * @param key
     * @return
     */
    public boolean exist(String key){
    	return redisTemplate.hasKey(key);
    }

    //end list api
    public RedisTemplate getRedisTemplate() {
		return redisTemplate;
	}

	public void setRedisTemplate(RedisTemplate redisTemplate) {
		this.redisTemplate = redisTemplate;
	}
}
