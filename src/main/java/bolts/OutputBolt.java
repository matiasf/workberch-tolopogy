package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.GUID_REPLACE;
import static main.java.utils.constants.WorkberchConstants.XML_EXT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import main.java.utils.WorkberchTuple;
import main.java.utils.redis.RedisHandeler;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;

public class OutputBolt extends WorkberchOrderBolt {

	private static final long serialVersionUID = 1L;

	private final String OUTPUT_PATH;

	private final List<WorkberchTuple> tuplesToWrite = new ArrayList<WorkberchTuple>();

	public OutputBolt(final String guid, final String outputPath, final Boolean ordered) {
		super(new ArrayList<String>(), ordered);
		OUTPUT_PATH = StringUtils.replace(outputPath, GUID_REPLACE, guid);

		try {
			FileUtils.deleteDirectory(new File(OUTPUT_PATH));
		} catch (final IOException e) {
			throw new RuntimeException("Se borra el directorio de salida atendido " + OUTPUT_PATH, e);
		}

		try {
			FileUtils.forceMkdir(new File(OUTPUT_PATH));
		} catch (final IOException e) {
			throw new RuntimeException("No se puedo crear el directorio " + OUTPUT_PATH, e);
		}
	}

	@Override
	public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues) {
		tuplesToWrite.add(input);
		if (lastValues) {
			System.out.println("Crearndo archivo " + getBoltId() + XML_EXT);
			final File file = new File(OUTPUT_PATH + getBoltId() + XML_EXT);
			try {
				if (!file.exists()) {
					file.createNewFile();
				}

				final PrintWriter bufferWritter = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));

				for (final WorkberchTuple inputToWrite : tuplesToWrite) {
					System.out.println("Valor - " + inputToWrite.getValues().get(getBoltId()));
					bufferWritter.println("Valor - " + inputToWrite.getValues().get(getBoltId()));
				}

				System.out.println("Cerrando Archivo");
				
				bufferWritter.close();
				RedisHandeler.setStateFinished(getBoltId());
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}

		}
	}

}
