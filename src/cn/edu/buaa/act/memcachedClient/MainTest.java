package cn.edu.buaa.act.memcachedClient;

import java.util.Hashtable;

import org.apache.log4j.PropertyConfigurator;
/**
 * Test the communication between R-Memcached and Memcached server in one cache node.
 * @author Yanran Lu
 */
public class MainTest {
	// store results from threads
	private static Hashtable<Integer, StringBuilder> threadInfo = new Hashtable<Integer, StringBuilder>();

	public static void main(String[] args) {
		PropertyConfigurator.configure(MainTest.class.getClass()
				.getResource("/").getPath()
				+ "log4j.properties");
		// String[] serverlist = { "127.0.0.1:20000", "127.0.0.1:20001" ,
		// "127.0.0.1:20002" };

		int threads = Integer.parseInt(args[0]); // the number of threads
		int runs = Integer.parseInt(args[1]); // how many request send out by a thread
		int Nums = Integer.parseInt(args[2]); // the total size of data 
		int size = Integer.parseInt(args[3]); // the size of a data

		// get object to store
		byte[] obj = new byte[size];
		for (int i = 0; i < size; i++) {
			obj[i] = '1';
		}
		String value = new String(obj);

		String[] keys = new String[size];
		for (int i = 0; i < Nums; i++) {
			keys[i] = "key" + i;
		}

		for (int i = 0; i < threads; i++) {
			bench b = new bench(runs, Nums, i, value, keys);
			b.start();
		}

		int i = 0;
		while (i < threads) {
			if (threadInfo.containsKey(new Integer(i))) {
				System.out.println(threadInfo.get(new Integer(i)));
				i++;
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		System.exit(1);
	}

	/**
	 * Test code per thread.
	 */
	private static class bench extends Thread {
		private int runs;
		private int threadNum;
		private String object;
		private String[] keys;
		private int size;
		private int nums;

		public bench(int runs, int nums, int threadNum, String object,
				String[] keys) {
			this.runs = runs;
			this.threadNum = threadNum;
			this.object = object;
			this.keys = keys;
			this.size = object.length();
			this.nums = nums;
		}

		public void run() {
			StringBuilder result = new StringBuilder();

			// get client instance
			MemcachedClient mc = new MemcachedClient("127.0.0.1:20000");
			mc.setCompressEnable(false);
			mc.setCompressThreshold(0);

			try {
				Thread.sleep(0);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// time deletes
			long start = System.nanoTime();
			randReadWrite(mc, 0.8);
			long elapse = (System.nanoTime() - start) / 1000000;
			float avg = (float) elapse / runs;
			result.append("\nthread " + threadNum + ": runs: " + runs
					+ " read or write of obj " + (size / 1024)
					+ "KB -- avg time per req " + avg + " ms (total: " + elapse
					+ " ms)");

			threadInfo.put(new Integer(threadNum), result);
		}

		public void randReadWrite(MemcachedClient mc, double scale) {
			for (int i = 0; i < runs; i++) {
				if (Math.random() < scale) {
					mc.get(keys[i % nums]);
				} else {
					mc.set(keys[i % nums], object);
				}
			}
		}
	}
}
