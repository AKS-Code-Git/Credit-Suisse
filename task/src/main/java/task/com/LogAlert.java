package task.com;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import task.com.bean.Log;
import task.com.dao.LogDao;
import task.com.util.PropUtil;
import task.com.util.Utility;

public class LogAlert {
	static {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-hhmmss");
		System.setProperty("logtime", dateFormat.format(new Date()));
	}
	static Logger log = Logger.getLogger(LogAlert.class.getName());

	public static void main(String[] args) {

		PropUtil.readProperties();
		LogDao ld = new LogDao();
		ld.createTable();
		GsonBuilder builder = new GsonBuilder();
		builder.setPrettyPrinting();

		boolean ok = true;

		List<Log> logs = new ArrayList<Log>();
		try {
			while (ok) {
				Thread.sleep(PropUtil.getPause());
				List<String> lines = Utility.readfile(Utility.replicatefile());
				long lineNumber = 0;
				long skipLine = 0;
				long lastRead = Utility.readState();
				if (lastRead < -1) {
					ok = false;
				} else {
					for (Iterator<String> iterator = lines.iterator(); iterator.hasNext();) {
						String jsonString = iterator.next();
						if (jsonString.trim().endsWith("}") && skipLine > lastRead) {
							Gson gson = builder.create();
							logs.add(gson.fromJson(jsonString, Log.class));
							lineNumber++;
						}
						if (jsonString.trim().endsWith("}")) {
							skipLine++;
						}
					}
					if (logs != null && logs.size() > 0) {
						ld.insertLog(logs);
						logs.removeAll(logs);
						Utility.writeState(lineNumber + lastRead);
					}
				}
				log.info("New JSON logs count :" + lineNumber);
			}

		} catch (JsonSyntaxException e) {
			log.error(e);
		} catch (InterruptedException e) {
			log.error(e);
		} finally {
			ld.anhilateDao();
		}
	}

}
