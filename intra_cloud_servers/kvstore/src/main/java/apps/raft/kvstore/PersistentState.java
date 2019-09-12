package apps.raft.kvstore;

import kvstore.*;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;


public class PersistentState implements Serializable {

	int currentTerm;
	int votedFor;
	List<LogEntry> logs;
	ReentrantLock mu = null;

	public PersistentState(int currentTerm, int votedFor, List<Kvstore.Log> logList, ReentrantLock mu) {
		this.currentTerm = currentTerm;
		this.votedFor = votedFor;
		this.mu = mu;
		if (logList == null) {
			return;
		}
		logs = new ArrayList<>();

		mu.lock();
		for(int i=0; i<logList.size(); i++) {
			Kvstore.Log entry = logList.get(i);
			int term = entry.getTerm();
			String key = entry.getPutReq().getKey();
			String value = entry.getPutReq().getValue();
			logs.add(new LogEntry(term,key,value));
		}
		mu.unlock();
	}

	public int getCurrentTerm(){
		return currentTerm;
	}

	public int getVotedFor() {
		return votedFor;
	}

	public List<Kvstore.Log> getLogs() {

		if (logs == null) {
			return null;
		}
		List<Kvstore.Log> logList = new ArrayList<>();
		for(int i=0; i<logs.size(); i++) {
			LogEntry entry = logs.get(i);
			int term = entry.getTerm();
			String key = entry.getKey();
			String value = entry.getValue();
			Kvstore.PutRequest.Builder putRequestBuilder = Kvstore.PutRequest.newBuilder();
			putRequestBuilder.setKey(key);
			putRequestBuilder.setValue(value);
			Kvstore.Log.Builder logBuilder = Kvstore.Log.newBuilder();
			logBuilder.setPutReq(putRequestBuilder.build());
			logBuilder.setTerm(term);
			logList.add(logBuilder.build());
		}
		return logList;
	}

	class LogEntry implements Serializable {
		int term;
		String key;
		String value;
		public LogEntry(int term, String key, String value) {
			this.term = term;
			this.key = key;
			this.value = value;
		}
		public int getTerm() {
			return term;
		}
		public String getKey() {
			return key;
		}
		public String getValue() {
			return value;
		}
	}


}