package main.java.bolts;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class DownloadSRA extends WorkberchGenericBolt {

	final static String FILE_PATH = "/home/proyecto/Downloads/";
	final static String FINAL_PATH = "/home/proyecto/Documents/";

	public DownloadSRA(final List<String> inputFields, final List<String> outputFields) {
		super(outputFields);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeLogic(final WorkberchTuple tuple, final BasicOutputCollector collector, final boolean lastValue) {
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

		System.out.println("Descargando" + ftpURLOutput);

		final String fileStr = FILE_PATH + runAccessionID + ".sra";

		final File file = new File(fileStr);
		System.out.println("Antes del try");
		if (!file.exists()) {
			try {
				System.out.println("En el try");
				final URL url = new URL(ftpURLOutput);
				final URLConnection urlc = url.openConnection();
				System.out.println("Open connection");
				final InputStream is = urlc.getInputStream();
				final BufferedInputStream bis = new BufferedInputStream(is);

				final OutputStream os = new FileOutputStream(fileStr);
				final BufferedOutputStream bos = new BufferedOutputStream(os);

				final byte[] buffer = new byte[1024];
				int readCount;
				System.out.println("antes del while");
				while ((readCount = bis.read(buffer)) > 0) {
					bos.write(buffer, 0, readCount);
				}
				System.out.println("Va a copiar");
				bos.close();
				is.close(); // close the FTP inputstream

				final String command = "cp " + fileStr + " " + FINAL_PATH;

				System.out.println(command);
				Runtime.getRuntime().exec(command);

			} catch (final Exception e) {
				System.out.println("Todo mal");
				e.printStackTrace();
			}

		} else {
			System.out.println("Ya existe el archivo " + fileStr);
		}
	}

}
