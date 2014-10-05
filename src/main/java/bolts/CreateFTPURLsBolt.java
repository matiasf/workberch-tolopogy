package main.java.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class CreateFTPURLsBolt extends WorkberchGenericBolt {

	public CreateFTPURLsBolt(final List<String> inputFields, final List<String> outputFields) {
		super(outputFields);
	}

	@Override
	public void executeLogic(final WorkberchTuple tuple, final BasicOutputCollector collector) {
		final Map<String, Object> values = tuple.getValues();
		final String runAccessionID = (String) values.get("runAccessionID");
		final String ftpURLInput = (String) values.get("ftpURLInput");

		final List<Object> emitTuple = new ArrayList<Object>();

		final String runType = runAccessionID.substring(0, 3);
		emitTuple.add(runType);

		final String volume = runAccessionID.substring(0, 6);
		emitTuple.add(volume);

		final String runAccesionIDOutput = runAccessionID;
		emitTuple.add(runAccesionIDOutput);

		final String ftpURLOutput = ftpURLInput + runType + "/" + volume + "/" + runAccessionID + "/" + runAccessionID + ".sra";
		emitTuple.add(ftpURLOutput);

		collector.emit(emitTuple);

		System.out.println(ftpURLOutput);

	}

}
