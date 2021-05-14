package task.com.dao;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import org.apache.log4j.Logger;

import task.com.bean.Log;
import task.com.functional.UpdateAlertFunctional;
import task.com.util.Constants;
import task.com.util.PropUtil;

public class LogDao {
	private Connection connection;
	static Logger loger = Logger.getLogger(LogDao.class.getName());

	public LogDao() {
		try {
			Class.forName(PropUtil.getJdbcDriver());
			this.connection = DriverManager.getConnection(PropUtil.getJdbcURL(), PropUtil.getDbUser(),
					PropUtil.getDbPassword());
			loger.info("Database connection created...");
		} catch (ClassNotFoundException e) {
			loger.error(e);
		} catch (SQLException e) {
			loger.error(e);
		}
	}

	public void anhilateDao() {
		if (this.connection != null) {
			try {
				this.connection.close();
				loger.info("Database connection closed...");
			} catch (SQLException e) {
				loger.error(e);
			}
			this.connection = null;
		}
	}

	public void createTable() {
		try {
			Statement stmt = this.connection.createStatement();
			if (PropUtil.getQueryDrop() != null && PropUtil.getQueryDrop().length() > 0) {
				stmt.executeUpdate(PropUtil.getQueryDrop());
			}
			stmt.executeUpdate(PropUtil.getQueryCreate());
			loger.info("Table created as per SQL :" + PropUtil.getQueryCreate());
			stmt.close();
		} catch (SQLException e) {
			loger.error(e);
		}
	}

	public void insertLog(List<Log> log) {
		ResultSet rs = null;

		try {
			loger.info("Number of JSON logs to insert :" + log.size());
			Statement stmt = this.connection.createStatement();
			final PreparedStatement prepStmt = this.connection.prepareStatement(PropUtil.getQueryInsert());

			log.forEach(log2 -> {
				try {
					prepStmt.setString(1, log2.getId());
					prepStmt.setString(2, log2.getState());
					prepStmt.setString(3, log2.getType());
					prepStmt.setString(4, log2.getHost());
					prepStmt.setLong(5, log2.getTimestamp());
					prepStmt.setShort(6, log2.getAlert());
					prepStmt.executeUpdate();
				} catch (SQLException e) {
					loger.error(e);
				}
			});
			prepStmt.close();

			loger.info("Number of JSON logs inserted :" + log.size());
			stmt = this.connection.createStatement();
			rs = stmt.executeQuery(PropUtil.getQueryAlert());
			LinkedList<Log> scsmbstgr = fetchDataForAlert(rs);

			final PreparedStatement prepStmtU = this.connection.prepareStatement(PropUtil.getQueryAlertUpdate());
			loger.info("Updating 'alert' flag in database.");
			scsmbstgr.forEach(log2 -> {
				try {
					prepStmtU.setShort(1, log2.getAlert());
					prepStmtU.setInt(2, log2.getDuration());
					prepStmtU.setInt(3, log2.getPk_id());
					prepStmtU.executeUpdate();
				} catch (SQLException e) {
					loger.error(e);
				}
			});
			prepStmtU.close();
			loger.info("Updated 'alert' flag in database.");

			rs = stmt.executeQuery("select * from LOG");
			writeToFile().apply(rs);

		} catch (SQLException e) {
			loger.error(e);
		} finally {

		}
	}

	private Function<ResultSet, Boolean> writeToFile() {
		Function<ResultSet, Boolean> fs = (r) -> {
			Boolean b = true;
			BufferedWriter writer = null;
			final String path = PropUtil.getUserDir() + "\\logs_" + System.currentTimeMillis() + ".csv";
			loger.info("Preparing csv file with 'alert' flag at location : " + path);
			try {
				writer = new BufferedWriter(new FileWriter(path));
				writer.write(Constants.PK_ID + " , " + Constants.ID + " , " + Constants.STATE + " , " + Constants.TYPE
						+ " , " + Constants.HOST + " , " + Constants.TIMESTAMP + " , " + Constants.ALERT + " , "
						+ Constants.DURATION + "\n");
				while (r.next()) {
					writer.write(r.getInt(Constants.PK_ID) + " , " + r.getString(Constants.ID) + " , "
							+ r.getString(Constants.STATE) + " , " + r.getString(Constants.TYPE) + " , "
							+ r.getString(Constants.HOST) + " , " + r.getString(Constants.TIMESTAMP) + " , "
							+ r.getShort(Constants.ALERT) + "," + r.getInt(Constants.DURATION) + "\n");

				}
			} catch (IOException e) {
				b = false;
				loger.error(e);
			} catch (SQLException e) {
				b = false;
				loger.error(e);
			} finally {
				try {
					if (writer != null)
						writer.close();
				} catch (IOException e) {
					loger.error(e);
				}
			}
			loger.info("Prepared csv file with 'alert' flag at location : " + path);

			return b;
		};
		return fs;
	}

	private LinkedList<Log> fetchDataForAlert(ResultSet rs) {
		LinkedList<Log> start = new LinkedList<Log>();
		LinkedList<Log> finish = new LinkedList<Log>();
		try {
			while (rs.next()) {
				String state = rs.getString(Constants.STATE);				
				Log log = getLog().apply(rs);
				if ("STARTED".equals(state)) {
					start.add(log);
				} else if ("FINISHED".equals(state)) {
					finish.add(log);
				}
			}
		} catch (SQLException e) {
			loger.error(e);
		}
		loger.info("Updating 'alert' flag.");

		LinkedList<Log> alert = updateAlert().updateAlert(start, finish);
		loger.info("Updated 'alert' flag.");
		return alert;

	}

	private UpdateAlertFunctional updateAlert() {
		UpdateAlertFunctional uaf = (s, f) -> {
			int size = (s.size() - f.size()) > 0 ? s.size() : f.size();
			for (int i = 0; i < size; i++) {
				long x = f.get(i).getTimestamp() - s.get(i).getTimestamp();
				boolean isValid = s.get(i).getId().equals(f.get(i).getId());
				if (isValid) {
					if (x > 4) {
						f.get(i).setAlert((short) 1);
						f.get(i).setDuration((int) x);
						s.get(i).setAlert((short) 1);
					} else {
						f.get(i).setDuration((int) x);
						f.get(i).setAlert((short) 0);
						s.get(i).setAlert((short) 0);
					}
				}
			}
			s.addAll(f);
			return s;
		};
		return uaf;
	}


	private Function<ResultSet, Log> getLog() {
		Function<ResultSet, Log> fn = (rs) -> {
			Log log = new Log();
			try {
				log.setPk_id(rs.getInt(Constants.PK_ID));
				log.setId(rs.getString(Constants.ID));
				log.setState(rs.getString(Constants.STATE));
				log.setType(rs.getString(Constants.STATE));
				log.setHost(rs.getString(Constants.HOST));
				log.setTimestamp(rs.getLong(Constants.TIMESTAMP));
				log.setAlert(rs.getShort(Constants.ALERT));
			} catch (SQLException e) {
				loger.error(e);
			}
			return log;
		};

		return fn;

	}
}
