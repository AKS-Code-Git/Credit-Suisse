package task.com.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;

import task.com.LogAlert;

public class Utility {
	static Logger log = Logger.getLogger(LogAlert.class.getName());

	public static void writeState(long lineNumber) {
		OutputStream output = null;
		try {
			output = new FileOutputStream(PropUtil.getUserDir() + "/track.properties");
			Properties prop = new Properties();
			prop.setProperty("lastread", "" + lineNumber);
			prop.store(output, null);
			log.info("State updated for number of files read during latest change :" + lineNumber);
		} catch (IOException e) {

			log.error(e);
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					log.error(e);
				}
			}
		}
	}

	public static long readState() {
		long retVal = 0;
		try {
			InputStream input = new FileInputStream(PropUtil.getUserDir() + "/track.properties");
			Properties prop = new Properties();
			prop.load(input);
			if (prop.getProperty("lastread") != null) {
				retVal = Long.valueOf(prop.getProperty("lastread")).longValue();
			}
		} catch (IOException e) {
			log.error(e);
		}
		return retVal;
	}

	public static String replicatefile() {
		Path src = Paths.get(PropUtil.getJsonFile());
		Path dest = Paths.get(PropUtil.getJsonFile() + ".temp");
		try {
			if (dest.toFile().exists()) {
				Files.delete(dest);
			}
			Files.copy(src, dest);
		} catch (IOException e) {
			log.error(e);
		}
		return dest.toFile().getAbsolutePath();
	}

	public static List<String> readfile(String path) {
		List<String> s = new ArrayList<String>();
		try {
			FileInputStream fis = new FileInputStream(path);
			Scanner sc = new Scanner(fis);
			while (sc.hasNextLine()) {
				s.add(sc.nextLine());
			}
			sc.close();
		} catch (IOException e) {
			log.error(e);
		}
		return s;
	}
}
