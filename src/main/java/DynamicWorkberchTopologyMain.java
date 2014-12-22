package main.java;

import static main.java.utils.constants.WorkberchConstants.GUID_REPLACE;

import java.io.IOException;

import main.java.parser.taverna.WorkberchTavernaParser;
import main.java.utils.constants.WorkberchConstants;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import uk.org.taverna.scufl2.api.io.ReaderException;
import backtype.storm.Config;
import backtype.storm.LocalCluster;

public class DynamicWorkberchTopologyMain {

	public static void main(final String[] args) throws ReaderException, IOException {
		final Jedis jedis = new Jedis("localhost");
		jedis.flushAll();
		jedis.close();
		
		WorkberchConstants.GUID = args[0];
		WorkberchConstants.WORKFLOW_PATH = StringUtils.replace(args[1], GUID_REPLACE, WorkberchConstants.GUID);
		WorkberchConstants.INPUT_PATH = StringUtils.replace(args[2], GUID_REPLACE, WorkberchConstants.GUID);
		WorkberchConstants.OUTPUT_PATH = StringUtils.replace(args[3], GUID_REPLACE, WorkberchConstants.GUID);

		if (StringUtils.isNotEmpty(WorkberchConstants.GUID) && StringUtils.isNotEmpty(WorkberchConstants.WORKFLOW_PATH)
				&& StringUtils.isNotEmpty(WorkberchConstants.OUTPUT_PATH) && StringUtils.isNotEmpty(WorkberchConstants.INDEX_FIELD)) {
			final WorkberchTavernaParser parser = new WorkberchTavernaParser();

			final Config conf = new Config();
			conf.setDebug(true);
			conf.setMaxTaskParallelism(1);

			final LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("workberch", conf, parser.parse());
		} else {
			throw new RuntimeException("Workflow can't be initialized");
		}
	}

}
