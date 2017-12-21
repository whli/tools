package mx.j2.recommend_spark.utils;

import mx.j2.recommend_spark.utils.redisTool.ListTranscoder;
import org.apache.log4j.Logger;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 亚马逊ElasticCache 服务接口
 *
 * @author zhuowei
 *
 */
public class ElasticCacheUtil {

	private static Logger logger = Logger.getLogger(ElasticCacheUtil.class);
	private ListTranscoder<String> realTimeClickTranscoder;
	private JedisCluster jc;

	/**
	 * 构造函数
	 *
	 * @param
	 */
	public ElasticCacheUtil() {
		init();
	}

	/**
	 * 内部静态类
	 */

	private static class Inner {
		static ElasticCacheUtil elasticCacheUtil = new ElasticCacheUtil();
	}

	public static ElasticCacheUtil getInstance() {
		return ElasticCacheUtil.Inner.elasticCacheUtil;
	}

	/**
	 * 初始化
	 *
	 * @param
	 */
	public void init() {
		if (jc == null) {
			Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
			jedisClusterNode.add(new HostAndPort("", 6379));
			jc = new JedisCluster(jedisClusterNode);
		}
		realTimeClickTranscoder = new ListTranscoder<String>();
		logger.info("[ElasticCacheSource init successfully]");
	}

	public String get(String key) {
		return jc.get(key);
	}

	public void set(String key, String value) {
		jc.set(key, value);
		return;
	}

	public void setex(String key, int expireTime, String value) {
		jc.setex(key, expireTime, value);
		return;
	}

	public void setRealTimeClick(String key, int expireTime, List<String> value) {
		jc.setex(key.getBytes(), expireTime, realTimeClickTranscoder.serialize(value));
		return;
	}

	public List<String> getClickDocids(String key) {

		byte[] value = jc.get(key.getBytes());
		if (value != null) {
			return realTimeClickTranscoder.deserialize(value);
		}
		return new ArrayList<String>();

	}

}
