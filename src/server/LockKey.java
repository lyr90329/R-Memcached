package server;

public class LockKey {
	public Integer memNumber = 0;
	public Integer ncount = 0;
	public Integer state = unLock;
	public long time;

	LockKey(Integer num, Integer count, long t, Integer s) {
		memNumber = num;
		ncount = count;
		time = t;
		state = s;
	}

	public final static Integer unLock = 0;
	public final static Integer badLock = 1;
	public final static Integer waitLock = 2;
}
