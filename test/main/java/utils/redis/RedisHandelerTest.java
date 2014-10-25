package main.java.utils.redis;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import main.java.utils.cartesianindex.CartesianIndex;
import main.java.utils.cartesianindex.CartesianLeaf;
import main.java.utils.cartesianindex.CartesianNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class RedisHandelerTest {

	private static final String TEST_BOLT = "dummyBolt";
	
	private RedisServer server;

	@Before
	public void before() throws IOException {
		server = new RedisServer(new File("/home/vectorns/Desarrollo/redis-2.8.17/src/redis-server"), 6379);
		server.start();
	}

	@Test
	public void handelBoltStateRecivedIncresed() {
		RedisHandeler.increseRecivedState(TEST_BOLT);
		assertEquals(RedisHandeler.getRecivedState(TEST_BOLT), 1L);
	}

	@Test
	public void handelBoltStateRecivedWithOutIncrese() {
		assertEquals(RedisHandeler.getRecivedState(TEST_BOLT), 0L);
	}
	
	@Test
	public void handelBoltStateEmitedIncresed() {
		RedisHandeler.increseEmitedState(TEST_BOLT);
		assertEquals(RedisHandeler.getEmitedState(TEST_BOLT), 1L);
	}

	@Test
	public void handelBoltStateEmitedWithOutIncrese() {
		assertEquals(RedisHandeler.getEmitedState(TEST_BOLT), 0L);
	}
	
	@Test
	public void handelBoltStateFinishedSet() {
		RedisHandeler.setStateFinished(TEST_BOLT);
		assertTrue(RedisHandeler.getFinishedState(TEST_BOLT));
	}

	@Test
	public void handelBoltStateFinishedWithoutSet() {
		assertFalse(RedisHandeler.getFinishedState(TEST_BOLT));
	}

	@Test
	public void saveAndLoadWorkberchTuplesWithPlainIndex() throws RedisException {
		final CartesianLeaf leaf1 = new CartesianLeaf(1L);
		final CartesianLeaf leaf2 = new CartesianLeaf(2L);

		final List<CartesianIndex> leafs = new ArrayList<CartesianIndex>();
		leafs.add(leaf1);
		leafs.add(leaf2);
		final CartesianNode node = new CartesianNode(leafs);

		final List<String> tupleFields = new ArrayList<String>();
		tupleFields.add("dummyField");
		tupleFields.add(INDEX_FIELD);

		final Fields testFields = mock(Fields.class);
		given(testFields.toList()).willReturn(tupleFields);

		final Tuple testTuple = mock(Tuple.class);
		given(testTuple.getFields()).willReturn(testFields);
		given(testTuple.getValueByField("dummyField")).willReturn("dummyValue");
		given(testTuple.getValueByField(INDEX_FIELD)).willReturn(node);

		final WorkberchTuple testWTuple = new WorkberchTuple(testTuple);

		RedisHandeler.saveCartesianIndexObject(TEST_BOLT, testWTuple);
		final Map<CartesianIndex, WorkberchTuple> cartesianIndexMap = RedisHandeler.loadCartesianIndexObjects(TEST_BOLT);

		assertTrue("The map should return a Cartesian key with the same value provided", cartesianIndexMap.containsKey(node));
		assertFalse("The fields shouldn't be empty", testWTuple.getFields().isEmpty());
		assertFalse("The tuple should have values", testWTuple.getValues().isEmpty());
	}

	@Test
	public void saveAndLoadWorkberchTuplesWithNotPlainIndex() throws RedisException {
		final CartesianLeaf leaf1 = new CartesianLeaf(1L);
		final CartesianLeaf leaf2 = new CartesianLeaf(2L);
		final CartesianLeaf leaf3 = new CartesianLeaf(3L);
		final CartesianLeaf leaf4 = new CartesianLeaf(4L);

		final List<CartesianIndex> leafsDeep = new ArrayList<CartesianIndex>();
		leafsDeep.add(leaf3);
		leafsDeep.add(leaf4);
		final CartesianNode nodeDeep = new CartesianNode(leafsDeep);

		final List<CartesianIndex> leafs = new ArrayList<CartesianIndex>();
		leafs.add(leaf1);
		leafs.add(leaf2);
		leafs.add(nodeDeep);
		final CartesianNode node = new CartesianNode(leafs);

		final List<String> tupleFields = new ArrayList<String>();
		tupleFields.add("dummyField");
		tupleFields.add(INDEX_FIELD);

		final Fields testFields = mock(Fields.class);
		given(testFields.toList()).willReturn(tupleFields);

		final Tuple testTuple = mock(Tuple.class);
		given(testTuple.getFields()).willReturn(testFields);
		given(testTuple.getValueByField("dummyField")).willReturn("dummyValue");
		given(testTuple.getValueByField(INDEX_FIELD)).willReturn(node);

		final WorkberchTuple testWTuple = new WorkberchTuple(testTuple);

		RedisHandeler.saveCartesianIndexObject(TEST_BOLT, testWTuple);
		final Map<CartesianIndex, WorkberchTuple> cartesianIndexMap = RedisHandeler.loadCartesianIndexObjects(TEST_BOLT);

		assertTrue("The map should return a Cartesian key with the same value provided", cartesianIndexMap.containsKey(node));
		assertFalse("The fields shouldn't be empty", testWTuple.getFields().isEmpty());
		assertFalse("The tuple should have values", testWTuple.getValues().isEmpty());
	}

	@Test
	public void saveAndLoadTwoWorkberchTuplesWithPlainIndex() throws RedisException {
		final CartesianLeaf leaf11 = new CartesianLeaf(1L);
		final CartesianLeaf leaf12 = new CartesianLeaf(2L);

		final List<CartesianIndex> leafs1 = new ArrayList<CartesianIndex>();
		leafs1.add(leaf11);
		leafs1.add(leaf12);
		final CartesianNode node1 = new CartesianNode(leafs1);

		final List<String> tupleFields = new ArrayList<String>();
		tupleFields.add("dummyField");
		tupleFields.add(INDEX_FIELD);

		final Fields testFields = mock(Fields.class);
		given(testFields.toList()).willReturn(tupleFields);

		final Tuple testTuple1 = mock(Tuple.class);
		given(testTuple1.getFields()).willReturn(testFields);
		given(testTuple1.getValueByField("dummyField")).willReturn("dummyValue");
		given(testTuple1.getValueByField(INDEX_FIELD)).willReturn(node1);

		final WorkberchTuple testWTuple1 = new WorkberchTuple(testTuple1);

		RedisHandeler.saveCartesianIndexObject("testBolt1", testWTuple1);

		final CartesianLeaf leaf21 = new CartesianLeaf(0L);
		final CartesianLeaf leaf22 = new CartesianLeaf(4L);

		final List<CartesianIndex> leafs2 = new ArrayList<CartesianIndex>();
		leafs2.add(leaf21);
		leafs2.add(leaf22);
		final CartesianNode node2 = new CartesianNode(leafs2);

		final Tuple testTuple2 = mock(Tuple.class);
		given(testTuple2.getFields()).willReturn(testFields);
		given(testTuple2.getValueByField("dummyField")).willReturn("dummyValue");
		given(testTuple2.getValueByField(INDEX_FIELD)).willReturn(node2);

		final WorkberchTuple testWTuple2 = new WorkberchTuple(testTuple2);

		RedisHandeler.saveCartesianIndexObject("testBolt2", testWTuple2);

		final Map<CartesianIndex, WorkberchTuple> cartesianIndexMap1 = RedisHandeler.loadCartesianIndexObjects("testBolt1");
		final Map<CartesianIndex, WorkberchTuple> cartesianIndexMap2 = RedisHandeler.loadCartesianIndexObjects("testBolt2");
		final Map<CartesianIndex, WorkberchTuple> cartesianIndexMap3 = RedisHandeler.loadCartesianIndexObjects("testBolt3");

		assertTrue("The map should return a Cartesian key with the same value provided", cartesianIndexMap1.containsKey(node1));
		assertTrue("The map should return a Cartesian key with the same value provided", cartesianIndexMap2.containsKey(node2));
		assertTrue("The map should be empty", cartesianIndexMap3.isEmpty());
	}

	@After
	public void after() throws InterruptedException {
		final Jedis jedis = new Jedis("localhost");
		jedis.flushAll();
		jedis.close();
		server.stop();
	}

}
