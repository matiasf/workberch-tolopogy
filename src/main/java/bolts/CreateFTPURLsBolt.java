package main.java.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.BaseTuple;
import backtype.storm.topology.BasicOutputCollector;

public class CreateFTPURLsBolt extends BaseBolt{

	public CreateFTPURLsBolt(List<String> inputFields, List<String> outputFields) {
		super(inputFields, outputFields);
	}

	@Override
	public void executeLogic(BasicOutputCollector collector, BaseTuple tuple) {
		Map<String, Object> values = tuple.getValues();
		String runAccessionID = (String) values.get("runAccessionID");
		String ftpURLInput = (String) values.get("ftpURLInput");
		
		List<Object> emitTuple = new ArrayList<Object>();
		
		String runType = runAccessionID.substring(0, 3);
		emitTuple.add(runType);
		
		String volume = runAccessionID.substring(0, 6);
		emitTuple.add(volume);

		String runAccesionIDOutput = runAccessionID;
		emitTuple.add(runAccesionIDOutput);

		String ftpURLOutput = ftpURLInput+runType+"/"+volume+"/"+runAccessionID+"/"+runAccessionID+".sra";
		emitTuple.add(ftpURLOutput);
		
		collector.emit(emitTuple);
		
		System.out.println(ftpURLOutput);

		
	}

}
