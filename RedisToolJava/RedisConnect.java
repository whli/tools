package mx.j2.recommend_spark.utils;

import java.util.ArrayList;
import java.util.List;

import mx.j2.recommend_spark.utils.redisTool.ListTranscoder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by yue.wang on 17/3/8.
 */
public class RedisConnect {

	private static Jedis jedis;
	private static JedisPool jedisPool = null;
	private static final int MAXIDLE = 5; // pool内最大有几个idle的redis实例
	private static final long WAITTIME = 3000l; // 获取连接时候最大等待毫秒数
	static ListTranscoder realTimeClickTranscoder;

	public RedisConnect() {
		/*
		 */
		init();
	}

	/*
	 * public static Jedis getJedis() { init(); return jedis; }
	 */

	public static void init() {
		if (jedisPool == null) {
			JedisPoolConfig configRedis = new JedisPoolConfig();
			configRedis.setMaxIdle(MAXIDLE);
			configRedis.setMaxWaitMillis(WAITTIME);
			configRedis.setTestOnBorrow(true);
			jedisPool = new JedisPool(configRedis, "", 6379);
			realTimeClickTranscoder = new ListTranscoder<String>();
		}
	}

	public static void setTotal(String key, String docid) {

		try {
			init();
			jedis = jedisPool.getResource();
			String clickCount = jedis.hget(key, docid);
			if (clickCount != null) {
				int newCount = Integer.valueOf(clickCount) + 1;
				jedis.hset(key, docid, String.valueOf(newCount));
			} else {
				jedis.hset(key, docid, "1");
			}
		} catch (Exception e) {
			jedisPool.returnResource(jedis);
			jedis = null;
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				jedisPool.returnResource(jedis);
			}
		}

	}

	public static void setUserClickItem(String userKeyFix, int expireTime, String docId) {
		try {
			init();
			jedis = jedisPool.getResource();
			jedis.setex(userKeyFix, expireTime, docId);
		} catch (Exception e) {
			jedisPool.returnResource(jedis);
			jedis = null;
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				jedisPool.returnResource(jedis);
			}
		}
	}

	public static String getUserClickItem(String userKeyFix) {
		String clickItem = "";
		try {
			init();
			jedis = jedisPool.getResource();
			if (jedis.exists(userKeyFix)) {
				clickItem = jedis.get(userKeyFix);
			}
		} catch (Exception e) {
			jedisPool.returnResource(jedis);
			jedis = null;
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				jedisPool.returnResource(jedis);
			}
			return clickItem;
		}

	}

	public static List<String> getRealTimeClicks(String key) {
		List<String> docIdlist = new ArrayList<>();
		try {
			init();
			jedis = jedisPool.getResource();
			byte[] value = jedis.get(key.getBytes());
			if (value != null) {
				docIdlist.addAll(realTimeClickTranscoder.deserialize(value));
			}
		} catch (Exception e) {
			jedisPool.returnResource(jedis);
			jedis = null;
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				jedisPool.returnResource(jedis);
			}
			return docIdlist;
		}
	}

	public static void setRealTimeClicks(String key, int expireTime, List<String> value) {
		try {
			init();
			jedis = jedisPool.getResource();
			jedis.setex(key.getBytes(), expireTime, realTimeClickTranscoder.serialize(value));
		} catch (Exception e) {
			jedisPool.returnResource(jedis);
			jedis = null;
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				jedisPool.returnResource(jedis);
			}
		}
	}

}
