package storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import storm.config.FieldNames;
import storm.tools.ItemPair;


/**
 * calculate the support degree of pairs: frequency
 */
public class SupportComputeBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8776034202913455949L;

	private OutputCollector collector;
	
	private Map<ItemPair, Integer> pairCounts;
	int pairTotalCount;

	@Override
	public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		pairCounts = new HashMap<>();
		pairTotalCount = 0;
	}

	/**
	 * differtiate the tuples comes from PairTotalCountBolt, PairCountBolt, or CommandSpout
	 * @param tuple [description]
	 */
	@Override
	public void execute(Tuple tuple) {
		// comes from PairTotalCountBolt
		if ( tuple.getFields().get(0).equals(FieldNames.TOTAL_COUNT) ) {
			pairTotalCount = tuple.getIntegerByField(FieldNames.TOTAL_COUNT);
		}
		// comes from PairCountBolt
		else if ( tuple.getFields().size() == 3 ) {
			String item1 = tuple.getStringByField(FieldNames.ITEM1);
			String item2 = tuple.getStringByField(FieldNames.ITEM2);
			int pairCount = tuple.getIntegerByField(FieldNames.PAIR_COUNT);
			pairCounts.put(new ItemPair(item1, item2), pairCount);
		}
		// comes from CommandSpout
		// calculate the support degree
		else if ( tuple.getFields().get(0).equals(FieldNames.COMMAND) ) {
			for ( ItemPair itemPair : pairCounts.keySet() ) {
				double itemSupport = (double)(pairCounts.get(itemPair).intValue()) / pairTotalCount;
				collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemSupport));
			}
		}
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.ITEM1,
				FieldNames.ITEM2,
				FieldNames.SUPPORT
		));
	}
}
