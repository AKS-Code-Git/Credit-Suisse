package task.com.bean;

import task.com.util.Constants;

public class Log {
	int pk_id;
	String id = Constants.EMPTY_STRING;
	String state = Constants.EMPTY_STRING;
	String type = Constants.EMPTY_STRING;
	String host = Constants.EMPTY_STRING;
	long timestamp;
	short alert = -1;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id == null ? Constants.EMPTY_STRING : id;
		
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state == null ? Constants.EMPTY_STRING : state;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type == null ? Constants.EMPTY_STRING : type;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host == null ? Constants.EMPTY_STRING : host;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		StringBuilder sbr = new StringBuilder();
		sbr.append(this.getPk_id());
		sbr.append("^");
		sbr.append(this.getTimestamp());
		sbr.append("^");
		sbr.append(this.getId());
		sbr.append("^");
		sbr.append(this.getState());
		sbr.append("^");
		sbr.append(this.getType());
		sbr.append("^");
		sbr.append(this.getHost());
		sbr.append("^");
		sbr.append(this.getAlert());
		return sbr.toString();
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return (int) Math.sqrt(timestamp);
	}

	public short getAlert() {
		return alert;
	}

	public void setAlert(short alert) {
		this.alert = alert;
	}

	public int getPk_id() {
		return pk_id;
	}

	public void setPk_id(int pk_id) {
		this.pk_id = pk_id;
	}

	public void setData(Log log) {
		this.id=log.getId();
		this.host=log.getHost();
		this.alert=log.getAlert();
		this.pk_id=log.getPk_id();
		this.state=log.getState();
		this.timestamp=log.getTimestamp();
		this.type=log.getType();
	}
}
