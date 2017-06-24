package com.itrip.umc.common.lock;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 基于redis的分布式锁
 * <li>创建人：Jeff.cao</li>
 * <li>创建时间：2017年6月19日 上午11:29:53</li>
 * 
 * @version 0.0.1
 */
@Component
public class DistributedLock {

	@Autowired
	private RedisTemplate<String, Object> redis;

	public static final int DEFAUL_TIME_OUT_SECONDS = 120;

	public static final String LOCK_KEY_PREFIX = "dlock_";

	private static final Logger LOG = LoggerFactory.getLogger(DistributedLock.class);

	/**
	 * 带锁执行,自动释放锁
	 * 
	 * @param lockName
	 * @param task
	 * @param throwErrWhenLockFail
	 * @return
	 */
	public <T> T doIfGetLock(String lockName, Callable<T> task) {
		return doIfGetLock(lockName, task, DEFAUL_TIME_OUT_SECONDS, false);
	}

	/***
	 * 带锁执行,自动释放锁
	 * 
	 * @param lockName
	 * @param task
	 * @param timeOut
	 * @param throwErrWhenLockFail
	 * @return
	 */
	public <T> T doIfGetLock(String lockName, Callable<T> task, int timeOut, boolean throwErrWhenLockFail) {
		return redis.execute(new RedisCallback<T>() {

			@Override
			public T doInRedis(RedisConnection conn) throws DataAccessException {

				boolean hasLock = false;
				long now = System.currentTimeMillis();
				String realKey = String.format("%s%s", LOCK_KEY_PREFIX, lockName);

				byte[] key = realKey.getBytes();
				byte[] val = ByteBuffer.allocate(8).putLong(now).array();
				try {
					hasLock = conn.setNX(key, val);
					if (hasLock) {
						conn.expire(key, timeOut);
					} else {
						hasLock = checkIsExpired(conn, now, key, val, timeOut);
					}

					LOG.info("get lock:{} {}", realKey, hasLock);

					if (hasLock)
						return task.call();

					if (throwErrWhenLockFail)
						throw new IllegalStateException("fail to get lock:" + lockName);

					return null;
				} catch (Exception e) {
					throw new RuntimeException(e);
				} finally {
					if (hasLock) {
						conn.del(key);
					}
				}
			}

			private boolean checkIsExpired(RedisConnection conn, long now, byte[] key, byte[] newVal, int timeOut) {
				long oldTime = ByteBuffer.wrap(conn.get(key)).getLong();
				if (now - oldTime < timeOut)
					return false;
				conn.watch(key);
				conn.multi();
				conn.set(key, newVal);
				conn.expire(key, timeOut);
				List<Object> exec = conn.exec();
				return exec != null && exec.size() > 0 && exec.get(0).equals(Boolean.TRUE);
			}
		});
	}

}
