package main.java.utils.redis;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import main.java.utils.cartesianindex.CartesianIndex;

import org.apache.commons.lang.BooleanUtils;

import redis.clients.jedis.Jedis;

public class RedisHandeler {

	private static final String REDIS_SERVER = "localhost";

	private RedisHandeler() {
	}

	public static long increseRecivedState(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final long incr = jedis.hincrBy(boltId, "received", 1);
		jedis.close();
		return incr;
	}

	public static long getRecivedState(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final String receivedString = jedis.hget(boltId, "received");
		final long recived = receivedString == null ? 0L : Long.valueOf(receivedString);
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
		final String emitedString = jedis.hget(boltId, "emited");
		final long emited = emitedString == null ? 0L : Long.valueOf(emitedString);
		jedis.close();
		return emited;
	}

	public static void setStateFinished(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		jedis.hset(boltId, "finished", Boolean.TRUE.toString());
		jedis.close();
	}

	public static boolean getFinishedState(final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final boolean finishState = BooleanUtils.toBoolean(jedis.hget(boltId, "finished"));
		jedis.close();
		return finishState;
	}

	public static void saveCartesianIndexObject(final String boltId, final WorkberchTuple input) throws RedisException {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final ByteArrayOutputStream streamByteSerialize = new ByteArrayOutputStream();
		ObjectOutputStream streamObjectSerialize;
		try {
			streamObjectSerialize = new ObjectOutputStream(streamByteSerialize);
			streamObjectSerialize.writeObject(input);
			jedis.lpush((boltId + "_tuples").getBytes(), streamByteSerialize.toByteArray());
			streamObjectSerialize.close();
		} catch (final IOException e) {
			throw new RedisException(e);
		} finally {
			jedis.close();
		}
	}

	public static Map<CartesianIndex, WorkberchTuple> loadCartesianIndexObjects(final String boltId) throws RedisException {
		final Map<CartesianIndex, WorkberchTuple> cartesianMap = new HashMap<CartesianIndex, WorkberchTuple>();
		final Jedis jedis = new Jedis(REDIS_SERVER);
		try {
			byte[] serializedCartesianIndex = jedis.lpop((boltId + "_tuples").getBytes());
			int dimentions = 0;
			while (serializedCartesianIndex != null && serializedCartesianIndex.length != 0) {
				final ByteArrayInputStream streamByteSerialize = new ByteArrayInputStream(serializedCartesianIndex);
				final ObjectInputStream streamObjectSerialize = new ObjectInputStream(streamByteSerialize);
				final WorkberchTuple tuple = (WorkberchTuple) streamObjectSerialize.readObject();

				final CartesianIndex index = (CartesianIndex) tuple.getValues().get(INDEX_FIELD);
				if (dimentions == 0) {
					dimentions = index.getNodes().size();
				}
				cartesianMap.put(index, tuple);

				streamObjectSerialize.close();
				serializedCartesianIndex = jedis.lpop((boltId + "_tuples").getBytes());
			}
		} catch (final IOException e) {
			throw new RedisException(e);
		} catch (final ClassNotFoundException e) {
			throw new RedisException(e);
		} finally {
			jedis.close();
		}
		return cartesianMap;
	}

}
