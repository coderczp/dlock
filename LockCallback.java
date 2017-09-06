package com.itrip.common.util.lock;

import redis.clients.jedis.Jedis;

/**
 * 获取分布式锁后执行的回调
 * <li> 创建人：Jeff.cao </li>
 * <li> 创建时间：2017年9月5日 下午6:47:16 </li>
 * @version 0.0.1
 */

public interface LockCallback<T> {

	/***
	 * @param redis:当前redis
	 * @param args:业务需要的参数
	 * @return
	 */
	T exec(Jedis redis,Object ...args);
}
