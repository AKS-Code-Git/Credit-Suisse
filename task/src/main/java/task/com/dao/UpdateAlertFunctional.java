package task.com.dao;

import java.util.LinkedList;

import task.com.bean.Log;

public interface UpdateAlertFunctional {
	public LinkedList<Log> updateAlert(LinkedList<Log> start, LinkedList<Log> finish);
}
