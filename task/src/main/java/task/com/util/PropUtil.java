package task.com.util;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public final class PropUtil {
	private static String userDir = System.getProperty("user.dir");
	private static String jdbcDriver;
	private static String jdbcURL;
	private static String dbUser;
	private static String dbPassword;
	private static String queryCreate;
	private static String queryDrop;
	private static String queryInsert;
	private static String queryAlert;
	private static String queryAlertUpdate;
	private static String jsonFile;
	private static short alertThreshold = 0;
	private static long pause;

	static Logger log = Logger.getLogger(PropUtil.class.getName());

	public static void readProperties() {
		try {
			log.info("Reading property file.");
			FileReader reader = new FileReader(PropUtil.userDir + "/task.properties");

			Properties p = new Properties();
			p.load(reader);
			PropUtil.jdbcDriver = p.getProperty("JDBC_DRIVER");
			PropUtil.jdbcURL = p.getProperty("JDBC_URL");
			PropUtil.dbUser = p.getProperty("DB_USER");
			PropUtil.dbPassword = p.getProperty("DB_PASSWORD");
			PropUtil.queryCreate = p.getProperty("QUERY_CREATE");
			PropUtil.queryDrop = p.getProperty("QUERY_DROP");
			PropUtil.queryInsert = p.getProperty("QUERY_INSERT");
			PropUtil.queryAlert = p.getProperty("QUERY_FOR_ALERT");
			PropUtil.queryAlertUpdate = p.getProperty("QUERY_FOR_ALERT_UPDATE");
			PropUtil.jsonFile = p.getProperty("JSON_FILE");
			if (p.getProperty("ALERT_THRESHOLD") != null) {
				PropUtil.alertThreshold = Short.valueOf(p.getProperty("ALERT_THRESHOLD")).shortValue();
			}
			if (p.getProperty("PAUSE") != null) {
				PropUtil.pause = Long.valueOf(p.getProperty("PAUSE")).longValue();
			}
			log.info("Property file read.");

		} catch (IOException e) {
			log.error(e);
		}
	}

	/**
	 * @return the userDir
	 */
	public static final String getUserDir() {
		return userDir;
	}

	/**
	 * @return the jdbcDriver
	 */
	public static final String getJdbcDriver() {
		return jdbcDriver;
	}

	/**
	 * @return the jdbcURL
	 */
	public static final String getJdbcURL() {
		return jdbcURL;
	}

	/**
	 * @return the dbUser
	 */
	public static final String getDbUser() {
		return dbUser;
	}

	/**
	 * @return the dbPassword
	 */
	public static final String getDbPassword() {
		return dbPassword;
	}

	/**
	 * @return the queryCreate
	 */
	public static final String getQueryCreate() {
		return queryCreate;
	}

	/**
	 * @return the queryDrop
	 */
	public static final String getQueryDrop() {
		return queryDrop;
	}

	/**
	 * @return the queryInsert
	 */
	public static final String getQueryInsert() {
		return queryInsert;
	}

	/**
	 * @return the queryAlert
	 */
	public static final String getQueryAlert() {
		return queryAlert;
	}

	/**
	 * @return the queryAlertUpdate
	 */
	public static final String getQueryAlertUpdate() {
		return queryAlertUpdate;
	}

	/**
	 * @return the jsonFile
	 */
	public static final String getJsonFile() {
		return jsonFile;
	}

	/**
	 * @return the alertThreshold
	 */
	public static final short getAlertThreshold() {
		return alertThreshold;
	}

	/**
	 * @return the pause
	 */
	public static final long getPause() {
		return pause;
	}
}
