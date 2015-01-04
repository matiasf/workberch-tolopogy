package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import main.java.utils.cartesianindex.CartesianIndex;
import main.java.utils.cartesianindex.CartesianLeaf;
import main.java.utils.cartesianindex.CartesianNode;
import main.java.utils.redis.RedisHandeler;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ RedisHandeler.class })
public class WorkberchOrderBoltTest {

	private static final String DUMMY_FIELD = "dummy-field";
	private static final String DUMMY_VALUE = "dummy-value";

	@Before
	public void before() {
		mockStatic(RedisHandeler.class);
	}

	@Test
	public void testAlreadyOrderedFlowReceived() {
		final WorkberchOrderBolt orderedBolt = spy(new WorkberchOrderBolt("mockGuid", new ArrayList<String>(), true) {
			private static final long serialVersionUID = 1L;

			@Override
			public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues) {}
		});

		final BasicOutputCollector mockCollector = mock(BasicOutputCollector.class);
		final Fields fields = new Fields(INDEX_FIELD, DUMMY_FIELD);
		final Tuple stormTuple1 = mock(Tuple.class);
		final Tuple stormTuple2 = mock(Tuple.class);
		final Tuple stormTuple3 = mock(Tuple.class);
		final Tuple stormTuple4 = mock(Tuple.class);
		final Tuple stormTuple5 = mock(Tuple.class);

		given(stormTuple1.getFields()).willReturn(fields);
		given(stormTuple2.getFields()).willReturn(fields);
		given(stormTuple3.getFields()).willReturn(fields);
		given(stormTuple4.getFields()).willReturn(fields);
		given(stormTuple5.getFields()).willReturn(fields);

		given(stormTuple1.getValueByField(INDEX_FIELD)).willReturn(0L);
		given(stormTuple2.getValueByField(INDEX_FIELD)).willReturn(1L);
		given(stormTuple3.getValueByField(INDEX_FIELD)).willReturn(2L);
		given(stormTuple4.getValueByField(INDEX_FIELD)).willReturn(3L);
		given(stormTuple5.getValueByField(INDEX_FIELD)).willReturn(4L);

		given(stormTuple1.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple2.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple3.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple4.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple5.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);

		final WorkberchTuple workberchTuple1 = new WorkberchTuple(stormTuple1);
		final WorkberchTuple workberchTuple2 = new WorkberchTuple(stormTuple2);
		final WorkberchTuple workberchTuple3 = new WorkberchTuple(stormTuple3);
		final WorkberchTuple workberchTuple4 = new WorkberchTuple(stormTuple4);
		final WorkberchTuple workberchTuple5 = new WorkberchTuple(stormTuple5);

		orderedBolt.executeProvenance(workberchTuple5, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple3, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple2, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple4, mockCollector, false);

		verify(orderedBolt, never()).executeOrdered(any(WorkberchTuple.class), any(BasicOutputCollector.class), anyBoolean());

		orderedBolt.executeProvenance(workberchTuple1, mockCollector, false);

		final InOrder inOrder = inOrder(orderedBolt);
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple1), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple2), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple3), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple4), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple5), any(BasicOutputCollector.class), anyBoolean());
	}

	@Test
	public void testNotOrderedButPlainTreeIndexOfTwoSourcesFromCartesian() throws IOException {
		final WorkberchOrderBolt orderedBolt = spy(new WorkberchOrderBolt("mockGuid", new ArrayList<String>(), false) {
			private static final long serialVersionUID = 1L;

			@Override
			public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues) {}
		});

		final OngoingStubbing<Boolean> returnChainCall = PowerMockito.when(RedisHandeler.getFinishedState(anyString()));
		returnChainCall.thenReturn(Boolean.FALSE).thenReturn(Boolean.FALSE).thenReturn(Boolean.FALSE).thenReturn(Boolean.FALSE)
				.thenReturn(Boolean.FALSE).thenReturn(Boolean.TRUE);

		final BasicOutputCollector mockCollector = mock(BasicOutputCollector.class);
		final Fields fields = new Fields(INDEX_FIELD, DUMMY_FIELD);
		final Tuple stormTuple1 = mock(Tuple.class);
		final Tuple stormTuple2 = mock(Tuple.class);
		final Tuple stormTuple3 = mock(Tuple.class);
		final Tuple stormTuple4 = mock(Tuple.class);
		final Tuple stormTuple5 = mock(Tuple.class);
		final Tuple stormTuple6 = mock(Tuple.class);

		final CartesianIndex index11 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(0L), new CartesianLeaf(0L))));
		final CartesianIndex index12 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(0L), new CartesianLeaf(1L))));
		final CartesianIndex index13 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(0L), new CartesianLeaf(2L))));
		final CartesianIndex index21 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(1L), new CartesianLeaf(0L))));
		final CartesianIndex index22 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(1L), new CartesianLeaf(1L))));
		final CartesianIndex index23 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(1L), new CartesianLeaf(2L))));

		given(stormTuple1.getFields()).willReturn(fields);
		given(stormTuple2.getFields()).willReturn(fields);
		given(stormTuple3.getFields()).willReturn(fields);
		given(stormTuple4.getFields()).willReturn(fields);
		given(stormTuple5.getFields()).willReturn(fields);
		given(stormTuple6.getFields()).willReturn(fields);

		given(stormTuple1.getValueByField(INDEX_FIELD)).willReturn(index11);
		given(stormTuple2.getValueByField(INDEX_FIELD)).willReturn(index12);
		given(stormTuple3.getValueByField(INDEX_FIELD)).willReturn(index13);
		given(stormTuple4.getValueByField(INDEX_FIELD)).willReturn(index21);
		given(stormTuple5.getValueByField(INDEX_FIELD)).willReturn(index22);
		given(stormTuple6.getValueByField(INDEX_FIELD)).willReturn(index23);

		given(stormTuple1.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple2.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple3.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple4.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple5.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple6.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);

		final WorkberchTuple workberchTuple1 = new WorkberchTuple(stormTuple1);
		final WorkberchTuple workberchTuple2 = new WorkberchTuple(stormTuple2);
		final WorkberchTuple workberchTuple3 = new WorkberchTuple(stormTuple3);
		final WorkberchTuple workberchTuple4 = new WorkberchTuple(stormTuple4);
		final WorkberchTuple workberchTuple5 = new WorkberchTuple(stormTuple5);
		final WorkberchTuple workberchTuple6 = new WorkberchTuple(stormTuple6);

		final Map<CartesianIndex, WorkberchTuple> indexMap = new HashMap<CartesianIndex, WorkberchTuple>();
		indexMap.put(index11, workberchTuple1);
		indexMap.put(index12, workberchTuple2);
		indexMap.put(index13, workberchTuple3);
		indexMap.put(index21, workberchTuple4);
		indexMap.put(index22, workberchTuple5);
		indexMap.put(index23, workberchTuple6);
		PowerMockito.when(RedisHandeler.loadCartesianIndexObjects(anyString())).thenReturn(indexMap);

		orderedBolt.executeProvenance(workberchTuple5, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple3, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple2, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple4, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple6, mockCollector, false);

		verify(orderedBolt, never()).executeOrdered(any(WorkberchTuple.class), any(BasicOutputCollector.class), anyBoolean());

		orderedBolt.executeProvenance(workberchTuple1, mockCollector, true);

		final InOrder inOrder = inOrder(orderedBolt);
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple1), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple2), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple3), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple4), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple5), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple6), any(BasicOutputCollector.class), anyBoolean());
	}

	@Ignore
	public void testNotOrderedNotPlainIndexOfTwoSourcesFromCartesian() throws IOException {
		final WorkberchOrderBolt orderedBolt = spy(new WorkberchOrderBolt("mockGuid", new ArrayList<String>(), false) {
			private static final long serialVersionUID = 1L;

			@Override
			public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues) {}
		});

		final OngoingStubbing<Boolean> returnChainCall = PowerMockito.when(RedisHandeler.getFinishedState(anyString()));
		returnChainCall.thenReturn(Boolean.FALSE).thenReturn(Boolean.FALSE).thenReturn(Boolean.FALSE).thenReturn(Boolean.FALSE)
				.thenReturn(Boolean.FALSE).thenReturn(Boolean.FALSE).thenReturn(Boolean.FALSE).thenReturn(Boolean.TRUE);

		final BasicOutputCollector mockCollector = mock(BasicOutputCollector.class);
		final Fields fields = new Fields(INDEX_FIELD, DUMMY_FIELD);
		final Tuple stormTuple1 = mock(Tuple.class);
		final Tuple stormTuple2 = mock(Tuple.class);
		final Tuple stormTuple3 = mock(Tuple.class);
		final Tuple stormTuple4 = mock(Tuple.class);
		final Tuple stormTuple5 = mock(Tuple.class);
		final Tuple stormTuple6 = mock(Tuple.class);
		final Tuple stormTuple7 = mock(Tuple.class);
		final Tuple stormTuple8 = mock(Tuple.class);

		final CartesianIndex index11 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(0L), new CartesianLeaf(0L))));
		final CartesianIndex index12 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(0L), new CartesianLeaf(0L))));
		final CartesianIndex index21 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(0L), new CartesianLeaf(0L))));
		final CartesianIndex index22 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(new CartesianLeaf(0L), new CartesianLeaf(0L))));

		final CartesianIndex index111 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(index11, new CartesianLeaf(0L))));
		final CartesianIndex index112 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(index11, new CartesianLeaf(1L))));
		final CartesianIndex index121 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(index12, new CartesianLeaf(0L))));
		final CartesianIndex index122 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(index12, new CartesianLeaf(1L))));
		final CartesianIndex index211 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(index21, new CartesianLeaf(0L))));
		final CartesianIndex index212 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(index21, new CartesianLeaf(1L))));
		final CartesianIndex index221 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(index22, new CartesianLeaf(0L))));
		final CartesianIndex index222 = new CartesianNode(new ArrayList<CartesianIndex>(Arrays.asList(index22, new CartesianLeaf(1L))));

		given(stormTuple1.getFields()).willReturn(fields);
		given(stormTuple2.getFields()).willReturn(fields);
		given(stormTuple3.getFields()).willReturn(fields);
		given(stormTuple4.getFields()).willReturn(fields);
		given(stormTuple5.getFields()).willReturn(fields);
		given(stormTuple6.getFields()).willReturn(fields);
		given(stormTuple7.getFields()).willReturn(fields);
		given(stormTuple8.getFields()).willReturn(fields);

		given(stormTuple1.getValueByField(INDEX_FIELD)).willReturn(index111);
		given(stormTuple2.getValueByField(INDEX_FIELD)).willReturn(index112);
		given(stormTuple3.getValueByField(INDEX_FIELD)).willReturn(index121);
		given(stormTuple4.getValueByField(INDEX_FIELD)).willReturn(index122);
		given(stormTuple5.getValueByField(INDEX_FIELD)).willReturn(index211);
		given(stormTuple6.getValueByField(INDEX_FIELD)).willReturn(index212);
		given(stormTuple7.getValueByField(INDEX_FIELD)).willReturn(index221);
		given(stormTuple8.getValueByField(INDEX_FIELD)).willReturn(index222);

		given(stormTuple1.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple2.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple3.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple4.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple5.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple6.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple7.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);
		given(stormTuple8.getValueByField(DUMMY_FIELD)).willReturn(DUMMY_VALUE);

		final WorkberchTuple workberchTuple1 = new WorkberchTuple(stormTuple1);
		final WorkberchTuple workberchTuple2 = new WorkberchTuple(stormTuple2);
		final WorkberchTuple workberchTuple3 = new WorkberchTuple(stormTuple3);
		final WorkberchTuple workberchTuple4 = new WorkberchTuple(stormTuple4);
		final WorkberchTuple workberchTuple5 = new WorkberchTuple(stormTuple5);
		final WorkberchTuple workberchTuple6 = new WorkberchTuple(stormTuple6);
		final WorkberchTuple workberchTuple7 = new WorkberchTuple(stormTuple7);
		final WorkberchTuple workberchTuple8 = new WorkberchTuple(stormTuple8);

		final Map<CartesianIndex, WorkberchTuple> indexMap = new HashMap<CartesianIndex, WorkberchTuple>();
		indexMap.put(index111, workberchTuple1);
		indexMap.put(index112, workberchTuple2);
		indexMap.put(index121, workberchTuple3);
		indexMap.put(index122, workberchTuple4);
		indexMap.put(index211, workberchTuple5);
		indexMap.put(index212, workberchTuple6);
		indexMap.put(index221, workberchTuple7);
		indexMap.put(index222, workberchTuple8);
		PowerMockito.when(RedisHandeler.loadCartesianIndexObjects(anyString())).thenReturn(indexMap);

		orderedBolt.executeProvenance(workberchTuple5, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple3, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple2, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple4, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple6, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple7, mockCollector, false);
		orderedBolt.executeProvenance(workberchTuple8, mockCollector, false);

		verify(orderedBolt, never()).executeOrdered(any(WorkberchTuple.class), any(BasicOutputCollector.class), anyBoolean());

		orderedBolt.executeProvenance(workberchTuple1, mockCollector, false);

		final InOrder inOrder = inOrder(orderedBolt);
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple1), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple2), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple3), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple4), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple5), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple6), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple7), any(BasicOutputCollector.class), anyBoolean());
		inOrder.verify(orderedBolt).executeOrdered(eq(workberchTuple8), any(BasicOutputCollector.class), anyBoolean());
	}

}
