package task.com;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

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
		List<Log> logs = new ArrayList<Log>();
		try {
			boolean ok = true;
			GsonBuilder builder = new GsonBuilder();
			builder.setPrettyPrinting();
			while (ok) {
				Thread.sleep(PropUtil.getPause());
				long lastRead = Utility.readState();
				List<String> lines = Utility.readfile(Utility.replicatefile(), lastRead);
				if (lastRead < -1) {
					ok = false;
				} else {
					logs = lines.stream().map(jsonString -> {
						Log lg = null;
						Gson gson = builder.create();
						lg = gson.fromJson(jsonString, Log.class);
						return lg;
					}).collect(Collectors.toList());
					log.info("Number of JSON log processed :" + logs.size());
					if (logs != null && logs.size() > 0) {
						ld.insertLog(logs);
						Utility.writeState(logs.size() + lastRead);
						logs.removeAll(logs);
					}
				}
			}
		} catch (JsonSyntaxException e) {
			log.error(e.getMessage() ,e);
		} catch (InterruptedException e) {
			log.error(e);
		} finally {
			ld.anhilateDao();
		}
		log.info("Alert flags are set...Good Bye..");
	}
}
