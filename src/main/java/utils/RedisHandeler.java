package main.java.utils;

import org.apache.commons.lang.BooleanUtils;

import redis.clients.jedis.Jedis;

public class RedisHandeler {

	private static final String REDIS_SERVER = "localhost";
	
	private RedisHandeler() {}
	
	public static long increseRecivedState(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final long incr = jedis.hincrBy(boltId, "received", 1);
		jedis.close();
		return incr;
	}
	
	public static long getRecivedState(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final long recived = Long.valueOf(jedis.hget(boltId, "received"));
		jedis.close();
		return recived;
	}
	
	public static long increseEmitedState(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final long incr = jedis.hincrBy(boltId, "emited", 1);
		jedis.close();
		return incr;
	}
	
	public static long getEmitedState(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final long emited = Long.valueOf(jedis.hget(boltId, "emited"));
		jedis.close();
		return emited;
	}
	
	public static void setStateFinished(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		jedis.hset(boltId, "finished", Boolean.TRUE.toString());
		jedis.close();
	}
	
	public static boolean getStateFinished(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final boolean finishState = BooleanUtils.toBoolean(jedis.hget(boltId, "finished"));
		jedis.close();
		return finishState;
	}

}
