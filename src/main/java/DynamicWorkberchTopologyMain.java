package main.java;

import java.io.IOException;

import main.java.parser.taverna.WorkberchTavernaParser;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import uk.org.taverna.scufl2.api.io.ReaderException;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

public class DynamicWorkberchTopologyMain {

	public static void main(final String[] args) throws ReaderException, IOException, AlreadyAliveException, InvalidTopologyException {
		final Jedis jedis = new Jedis("localhost");
		jedis.flushAll();
		jedis.close();

		final String guid = args[0];
		final String workflowPath = args[1];
		final String inputPath = args[2];
		final String outputPath = args[3];
		final String mode = args[4];

		if (StringUtils.isNotEmpty(guid) && StringUtils.isNotEmpty(workflowPath) && StringUtils.isNotEmpty(inputPath)
				&& StringUtils.isNotEmpty(outputPath) && StringUtils.isNotEmpty(mode)) {
			final WorkberchTavernaParser parser = new WorkberchTavernaParser();
			parser.setGuid(guid);
			parser.setWorkflowPath(workflowPath);
			parser.setInputPath(inputPath);
			parser.setOutputPath(outputPath);

			if (mode.equals("local")) {
				final Config conf = new Config();
				conf.setDebug(true);
				conf.setMaxTaskParallelism(1);

				final LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("workberch", conf, parser.parse());
			} else if (mode.equals("remote")) {
				final Config conf = new Config();
				conf.setNumWorkers(20);
				conf.setMaxSpoutPending(5000);

				StormSubmitter.submitTopology(guid, conf, parser.parse());
			} else {
				throw new IllegalArgumentException("Mode " + mode + " doen't exists");
			}
		} else {
			throw new IllegalArgumentException("Workflow can't be initialized");
		}
	}
}
