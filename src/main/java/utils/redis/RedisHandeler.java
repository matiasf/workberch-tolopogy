package main.java.utils.redis;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;
import static main.java.utils.constants.WorkberchConstants.NAME_DELIMITER;
import static main.java.utils.constants.WorkberchConstants.PROVENANCE_FIELD_E;
import static main.java.utils.constants.WorkberchConstants.PROVENANCE_FIELD_R;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import main.java.utils.cartesianindex.CartesianIndex;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;

public class RedisHandeler {

	private static final String REDIS_SERVER = "localhost";

	private RedisHandeler() {
	}
	
	private static byte[] serializeValue(final Object value) throws RedisException {
		final ByteArrayOutputStream streamByteSerialize = new ByteArrayOutputStream();
		ObjectOutputStream streamObjectSerialize;
		try {
			streamObjectSerialize = new ObjectOutputStream(streamByteSerialize);
			streamObjectSerialize.writeObject(value);
			return streamByteSerialize.toByteArray();
		} catch (final IOException e) {
			throw new RedisException(e);
		}
	}
	
	private static WorkberchTuple deserializeValue(final byte[] value) throws RedisException {
		final ByteArrayInputStream streamByteSerialize = new ByteArrayInputStream(value);
		ObjectInputStream streamObjectSerialize;
		try {
			streamObjectSerialize = new ObjectInputStream(streamByteSerialize);
			final WorkberchTuple tuple = (WorkberchTuple) streamObjectSerialize.readObject();
			streamObjectSerialize.close();
			return tuple;
		} catch (final Exception e) {
			throw new RedisException(e);
		}
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
		jedis.lpush((boltId + "_tuples").getBytes(), serializeValue(input));
		jedis.close();
	}

	public static Map<CartesianIndex, WorkberchTuple> loadCartesianIndexObjects(final String boltId) throws RedisException {
		final Map<CartesianIndex, WorkberchTuple> cartesianMap = new HashMap<CartesianIndex, WorkberchTuple>();
		final Jedis jedis = new Jedis(REDIS_SERVER);
		byte[] serializedTuple = jedis.lpop((boltId + "_tuples").getBytes());
		while (serializedTuple != null && serializedTuple.length != 0) {
			final WorkberchTuple tuple = deserializeValue(serializedTuple);

			final CartesianIndex index = (CartesianIndex) tuple.getValues().get(INDEX_FIELD);
			cartesianMap.put(index, tuple);

			serializedTuple = jedis.lpop((boltId + "_tuples").getBytes());
		}
		jedis.close();
		return cartesianMap;
	}

	public static void setProvenanceReceivedInfo(final String guid, final String nodeId, final List<String> outputFields, final List<Object> tuple) throws RedisException {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final Iterator<Object> iterTuple = tuple.iterator();
		final Iterator<String> iterFields = outputFields.iterator();
		String indexValue = StringUtils.EMPTY;
		String indexField = StringUtils.EMPTY;
		do {
			final Object indexObject = iterTuple.next();
			indexField = iterFields.next();
			if (StringUtils.equals(indexField, INDEX_FIELD) && !(indexObject instanceof CartesianIndex)) {
				indexValue = String.valueOf(indexObject);
			}
			else {
				indexValue = "CARTESIANO!";
			}
		} while(iterFields.hasNext() && !StringUtils.equals(indexField, INDEX_FIELD));
		
		final Iterator<Object> iterTupleReturn = tuple.iterator();
		for (final String field : outputFields) {
			jedis.hset((guid + NAME_DELIMITER + nodeId).getBytes(), (PROVENANCE_FIELD_R + indexValue + NAME_DELIMITER + field).getBytes(), serializeValue(iterTupleReturn.next()));
		}
		jedis.close();
	}
	
	public static void setProvenanceEmitedInfo(final String guid, final String nodeId, final List<String> outputFields, final List<Object> tuple) throws RedisException {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		final Iterator<Object> iterTuple = tuple.iterator();
		final Iterator<String> iterFields = outputFields.iterator();
		String indexValue = StringUtils.EMPTY;
		String indexField = StringUtils.EMPTY;
		do {
			final Object indexObject = iterTuple.next();
			indexField = iterFields.next();
			if (StringUtils.equals(indexField, INDEX_FIELD) && !(indexObject instanceof CartesianIndex)) {
				indexValue = String.valueOf(indexObject);
			}
			else {
				indexValue = "CARTESIANO!";
			}
		} while(iterFields.hasNext() && !StringUtils.equals(indexField, INDEX_FIELD));
		
		final Iterator<Object> iterTupleReturn = tuple.iterator();
		for (final String field : outputFields) {
			jedis.hset((guid + NAME_DELIMITER + nodeId).getBytes(), (PROVENANCE_FIELD_E + indexValue + NAME_DELIMITER + field).getBytes(), serializeValue(iterTupleReturn.next()));
		}
		jedis.close();
	}
	
	public static void addOutput(final String guid, final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		jedis.incr(guid + "_outputs");
		jedis.close();
	}
	
	public static void addOutputFinished(final String guid, final String boltId) {
		final Jedis jedis = new Jedis(REDIS_SERVER);
		jedis.incr(guid + "_outputs_finished");
		jedis.close();
	}

}
