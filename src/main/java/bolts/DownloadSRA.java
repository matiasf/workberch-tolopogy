package main.java.bolts;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.BaseTuple;
import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class DownloadSRA extends WorkberchGenericBolt{
	
	final static String FILE_PATH = "/home/proyecto/Downloads/";
	final static String FINAL_PATH = "/home/proyecto/Documents/";

	public DownloadSRA(List<String> inputFields, List<String> outputFields) {
		super(inputFields, outputFields);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void executeLogic(WorkberchTuple tuple, BasicOutputCollector collector) {
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
		
		System.out.println("Descargando" + ftpURLOutput);	
		
		String fileStr = FILE_PATH+runAccessionID+".sra";
		
		File file = new File(fileStr);
		System.out.println("Antes del try");
		if (!file.exists()){
			try {
				System.out.println("En el try");
				URL url = new URL(ftpURLOutput);
				URLConnection urlc = url.openConnection();
				System.out.println("Open connection");
				InputStream is = urlc.getInputStream();
				BufferedInputStream bis = new BufferedInputStream(is);

				OutputStream os = new FileOutputStream(fileStr);
				BufferedOutputStream bos = new BufferedOutputStream(os);
				
				byte[] buffer = new byte[1024];
				int readCount;
				System.out.println("antes del while");
				while( (readCount = bis.read(buffer)) > 0) {
					bos.write(buffer, 0, readCount);
				}
				System.out.println("Va a copiar");
				bos.close();
				is.close (); // close the FTP inputstream
				
				String command = "cp " + fileStr + " " + FINAL_PATH;
				
				System.out.println(command);
				Runtime.getRuntime().exec(command);
				
			}
			catch (Exception e) {
				System.out.println("Todo mal");
				e.printStackTrace();
			}
			

		}
		else {
			System.out.println("Ya existe el archivo " + fileStr);
		}
	}

}
