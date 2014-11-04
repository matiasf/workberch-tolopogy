package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import main.java.utils.cartesianindex.CartesianIndex;
import main.java.utils.cartesianindex.CartesianLeaf;
import main.java.utils.cartesianindex.CartesianNode;
import main.java.utils.redis.RedisHandeler;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ RedisHandeler.class })
public class WorkberchCartesianBoltTest {

	private class IndexMatcher extends ArgumentMatcher<List<Object>> {
		private final CartesianIndex templateIndex;

		public IndexMatcher(final CartesianIndex templateIndex) {
			this.templateIndex = templateIndex;
		}

		@Override
		public boolean matches(final Object argument) {
			@SuppressWarnings("unchecked")
			final List<Object> values = (List<Object>) argument;
			final Iterator<Object> iterV = values.iterator();
			while (iterV.hasNext()) {
				final Object value = iterV.next();
				if (value instanceof CartesianIndex) {
					return !iterV.hasNext() && ((CartesianIndex) value).equals(templateIndex);
				}
			}
			return false;
		}
	}

	@Before
	public void before() {
		mockStatic(RedisHandeler.class);
	}

	@Test
	public void testReceivedFieldsFromTwoDifferentSources() {
		final List<String> recivedFields = new ArrayList<String>();
		recivedFields.add("dummyField1");
		recivedFields.add("dummyField2");
		recivedFields.add(INDEX_FIELD);

		final WorkberchCartesianBolt cartesianBolt = spy(new WorkberchCartesianBolt(recivedFields));

		final Tuple stormTuple1 = mock(Tuple.class);
		final Tuple stormTuple2 = mock(Tuple.class);

		given(stormTuple1.getFields()).willReturn(new Fields("dummyField1", INDEX_FIELD));
		given(stormTuple2.getFields()).willReturn(new Fields("dummyField2", INDEX_FIELD));

		given(stormTuple1.getValueByField(INDEX_FIELD)).willReturn(0L);
		given(stormTuple2.getValueByField(INDEX_FIELD)).willReturn(0L);

		given(stormTuple1.getValueByField("dummyField1")).willReturn("dummyValue1");
		given(stormTuple2.getValueByField("dummyField2")).willReturn("dummyValue2");

		final CartesianIndex leaf1 = new CartesianLeaf(0L);
		final CartesianIndex leaf2 = new CartesianLeaf(0L);
		final List<CartesianIndex> leafs = new ArrayList<CartesianIndex>();
		leafs.add(leaf1);
		leafs.add(leaf2);
		final CartesianIndex index = new CartesianNode(leafs);

		final GlobalStreamId stream1 = mock(GlobalStreamId.class);
		given(stream1.get_componentId()).willReturn("dummySource1");

		final GlobalStreamId stream2 = mock(GlobalStreamId.class);
		given(stream2.get_componentId()).willReturn("dummySource2");

		final Set<GlobalStreamId> keyStreams = new HashSet<GlobalStreamId>();
		keyStreams.add(stream1);
		keyStreams.add(stream2);

		@SuppressWarnings("unchecked")
		final Map<GlobalStreamId, Grouping> contextMap = mock(Map.class);
		given(contextMap.keySet()).willReturn(keyStreams);

		final TopologyContext context = mock(TopologyContext.class);
		given(context.getThisComponentId()).willReturn("dummyComponent");
		given(context.getThisSources()).willReturn(contextMap);

		cartesianBolt.prepare(mock(Map.class), context);
		
		given(RedisHandeler.getEmitedState("dummySource1")).willReturn(1L);
		given(RedisHandeler.getEmitedState("dummySource2")).willReturn(0L);

		cartesianBolt.execute(stormTuple1, mock(BasicOutputCollector.class));

		verify(cartesianBolt, never()).emitTuple(anyListOf(Object.class), any(BasicOutputCollector.class), anyBoolean());
		
		verifyStatic();
        RedisHandeler.increseRecivedState(anyString());
        
		given(RedisHandeler.getEmitedState("dummySource2")).willReturn(1L);

		cartesianBolt.execute(stormTuple2, mock(BasicOutputCollector.class));
		
		verifyStatic(times(2));
        RedisHandeler.increseRecivedState(anyString());

		verify(cartesianBolt).emitTuple(argThat(new IndexMatcher(index)), any(BasicOutputCollector.class), anyBoolean());
	}

}
