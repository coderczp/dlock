package com.czp.opensource;

import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * 基于redis的分布式锁,尽量设置为全局对象,注意这个类不会管理Jedis的生命周期
 * <pre>
 *      Jedis redis = null;
		DistributedLock lock = new DistributedLock(redis);
		lock.doIfGetLock("test", new LockCallback<String>() {

			@Override
			public String exec(Jedis redis, Object... args) {
				return null;
			}
			
		}, "arg1","arg2");
 * </pre>
 * <li>创建人：Jeff.cao</li>
 * <li>创建时间：2017年6月19日 上午11:29:53</li>
 * 
 * @version 0.0.1
 */
public class DistributedLock {

	/**注意这个类不会管理Jedis的生命周期*/
	private Jedis redis;

	/**key前缀*/
	private static final String LOCK_KEY_PREFIX = "dlock_";
	
	/**key默认超时时间*/
	private static final int DEFAUL_TIMEOUT_SECONDS = 120;

	private static final Logger LOG = LoggerFactory.getLogger(DistributedLock.class);

	/***
	 * 注意这个类不会管理Jedis的生命周期
	 * @param redis
	 */
	public DistributedLock(Jedis redis) {
		this.redis = redis;
	}

	/**
	 * 带锁执行,自动释放锁
	 * 
	 * @param lockName
	 * @param task
	 * @param throwErrWhenLockFail
	 * @return
	 */
	public <T> T doIfGetLock(String lockKey, LockCallback<T> callBack,Object ...args) {
		return doIfGetLock(lockKey, DEFAUL_TIMEOUT_SECONDS, false,callBack,args);
	}

	/***
	 * 带锁执行,自动释放锁
	 * 
	 * @param lockKey
	 * @param task
	 * @param timeOut
	 * @param throwErrWhenLockFail
	 * @return
	 */
	public  <T> T doIfGetLock(String lockKey,int timeOut, boolean throwErrWhenLockFail,LockCallback<T> callBack,Object ...args) {
		boolean hasLock = false;
		long now = System.currentTimeMillis();
		String realKey = String.format("%s%s", LOCK_KEY_PREFIX, lockKey);

		byte[] key = realKey.getBytes();
		byte[] val = ByteBuffer.allocate(8).putLong(now).array();
		try {
			hasLock = (redis.setnx(key, val) == 1l);
			if (hasLock) {
				redis.expire(key, timeOut);
			} else {
				hasLock = checkIsExpired(redis, now, key, val, timeOut);
			}

			LOG.info("get lock:{} {}", realKey, hasLock);

			if (hasLock)
				return callBack.exec(redis, args);

			if (throwErrWhenLockFail)
				throw new IllegalStateException("fail to get lock:" + lockKey);

			return null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (hasLock) {
				redis.del(key);
			}
		}
	}

	/***
	 * 如果客户端端拿到锁后异常断开,需要检查是否已经超时<br>
	 * <ul>
	 * <li>0. 如果分布式锁写入的key没有超时则返回</li>
	 * <li>1. watch写入的key</li>
	 * <li>2. 开启事物并执行set和expire</li>
	 * <li>3. multi.exec()提交事物,因为步骤1 watch key,此时任何客户端修改key值将会导致事物失败</li>
	 * <li>4. 检查multi.exec()返回值</li>
	 * </ul>
	 */
	private boolean checkIsExpired(Jedis conn, long now, byte[] key, byte[] newVal, int timeOut) {
		long writeTime = ByteBuffer.wrap(conn.get(key)).getLong();
		if (now - writeTime < timeOut)
			return false;
		
		conn.watch(key);
		Transaction multi = conn.multi();
		multi.set(key, newVal);
		multi.expire(key, timeOut);
		List<Object> res = multi.exec();
		return res != null && res.size() > 0 && res.get(0).equals(Boolean.TRUE);
	}
}
