package memcached;

import java.util.*;
import java.util.zip.*;
import java.io.*;
import java.net.URLEncoder;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

public class MemcachedClient {

	// logger
	private static Logger log = Logger.getLogger(MemcachedClient.class
			.getName());

	// return codes
	private static final String VALUE = "VALUE"; // start of value line from
													// server
	private static final String STATS = "STAT"; // start of stats line from
												// server
	private static final String ITEM = "ITEM"; // start of item line from server
	private static final String DELETED = "DELETED"; // successful deletion
	private static final String NOTFOUND = "NOT_FOUND"; // record not found for
														// delete or incr/decr
	private static final String STORED = "STORED"; // successful store of data
	private static final String NOTSTORED = "NOT_STORED"; // data not stored
	private static final String OK = "OK"; // success
	private static final String END = "END"; // end of data from server

	private static final String ERROR = "ERROR"; // invalid command name from
													// client
	private static final String CLIENT_ERROR = "CLIENT_ERROR"; // client error
																// in input line
																// - invalid
																// protocol
	private static final String SERVER_ERROR = "SERVER_ERROR"; // server error

	// default compression threshold
	private static final int COMPRESS_THRESH = 30720;

	// values for cache flags
	public static final int MARKER_BYTE = 1;
	public static final int MARKER_BOOLEAN = 8192;
	public static final int MARKER_INTEGER = 4;
	public static final int MARKER_LONG = 16384;
	public static final int MARKER_CHARACTER = 16;
	public static final int MARKER_STRING = 32;
	public static final int MARKER_STRINGBUFFER = 64;
	public static final int MARKER_FLOAT = 128;
	public static final int MARKER_SHORT = 256;
	public static final int MARKER_DOUBLE = 512;
	public static final int MARKER_DATE = 1024;
	public static final int MARKER_STRINGBUILDER = 2048;
	public static final int MARKER_BYTEARR = 4096;
	public static final int F_COMPRESSED = 2;
	public static final int F_SERIALIZED = 8;

	private int socketTO = 1000 * 3; // default timeout of socket reads
	private int socketConnectTO = 1000 * 3; // default timeout of socket
											// connections
	private boolean nagle = false; // enable/disable Nagle's algorithm

	// flags
	private boolean sanitizeKeys;
	private boolean primitiveAsString;
	private boolean compressEnable;
	private long compressThreshold;
	private String defaultEncoding;

	public static SockIO sock;

	// which pool to use
	private String HostName;

	// optional passed in classloader
	private ClassLoader classLoader;

	// optional error handler
	private ErrorHandler errorHandler;

	/**
	 * Creates a new instance of MemCachedClient accepting a passed in pool
	 * name.
	 * 
	 * @param poolName
	 *            name of SockIOPool
	 */
	public MemcachedClient(String HostName) {
		this.HostName = HostName;
		try {
			sock = new SockIO(HostName, socketTO, socketConnectTO, nagle);
			if (!sock.isConnected()) {
				log.error("++++ failed to get SockIO obj for: " + HostName
						+ " -- new socket is not connected");
				sock = null;
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		init();
	}

	/**
	 * Creates a new instance of MemCacheClient but acceptes a passed in
	 * ClassLoader.
	 * 
	 * @param classLoader
	 *            ClassLoader object.
	 */
	public MemcachedClient(ClassLoader classLoader) {
		this.classLoader = classLoader;
		init();
	}

	/**
	 * Creates a new instance of MemCacheClient but acceptes a passed in
	 * ClassLoader and a passed in ErrorHandler.
	 * 
	 * @param classLoader
	 *            ClassLoader object.
	 * @param errorHandler
	 *            ErrorHandler object.
	 */
	public MemcachedClient(ClassLoader classLoader, ErrorHandler errorHandler) {
		this.classLoader = classLoader;
		this.errorHandler = errorHandler;
		init();
	}

	/**
	 * Creates a new instance of MemCacheClient but acceptes a passed in
	 * ClassLoader, ErrorHandler, and SockIOPool name.
	 * 
	 * @param classLoader
	 *            ClassLoader object.
	 * @param errorHandler
	 *            ErrorHandler object.
	 * @param poolName
	 *            SockIOPool name
	 */
	public MemcachedClient(ClassLoader classLoader, ErrorHandler errorHandler,
			String HostName) {
		this.classLoader = classLoader;
		this.errorHandler = errorHandler;
		this.HostName = HostName;
		init();
	}

	/**
	 * Initializes client object to defaults.
	 * 
	 * This enables compression and sets compression threshhold to 15 KB.
	 */
	private void init() {
		this.sanitizeKeys = true;
		this.primitiveAsString = false;
		this.compressEnable = true;
		this.compressThreshold = COMPRESS_THRESH;
		this.defaultEncoding = "UTF-8";
	}

	/**
	 * Sets an optional ClassLoader to be used for serialization.
	 * 
	 * @param classLoader
	 */
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	/**
	 * Sets an optional ErrorHandler.
	 * 
	 * @param errorHandler
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Enables/disables sanitizing keys by URLEncoding.
	 * 
	 * @param sanitizeKeys
	 *            if true, then URLEncode all keys
	 */
	public void setSanitizeKeys(boolean sanitizeKeys) {
		this.sanitizeKeys = sanitizeKeys;
	}

	/**
	 * Enables storing primitive types as their String values.
	 * 
	 * @param primitiveAsString
	 *            if true, then store all primitives as their string value.
	 */
	public void setPrimitiveAsString(boolean primitiveAsString) {
		this.primitiveAsString = primitiveAsString;
	}

	/**
	 * Sets default String encoding when storing primitives as Strings. Default
	 * is UTF-8.
	 * 
	 * @param defaultEncoding
	 */
	public void setDefaultEncoding(String defaultEncoding) {
		this.defaultEncoding = defaultEncoding;
	}

	/**
	 * Enable storing compressed data, provided it meets the threshold
	 * requirements.
	 * 
	 * If enabled, data will be stored in compressed form if it is<br/>
	 * longer than the threshold length set with setCompressThreshold(int)<br/>
	 * <br/>
	 * The default is that compression is enabled.<br/>
	 * <br/>
	 * Even if compression is disabled, compressed data will be automatically<br/>
	 * decompressed.
	 * 
	 * @param compressEnable
	 *            <CODE>true</CODE> to enable compression, <CODE>false</CODE> to
	 *            disable compression
	 */
	public void setCompressEnable(boolean compressEnable) {
		this.compressEnable = compressEnable;
	}

	/**
	 * Sets the required length for data to be considered for compression.
	 * 
	 * If the length of the data to be stored is not equal or larger than this
	 * value, it will not be compressed.
	 * 
	 * This defaults to 15 KB.
	 * 
	 * @param compressThreshold
	 *            required length of data to consider compression
	 */
	public void setCompressThreshold(long compressThreshold) {
		this.compressThreshold = compressThreshold;
	}

	/**
	 * Checks to see if key exists in cache.
	 * 
	 * @param key
	 *            the key to look for
	 * @return true if key found in cache, false if not (or if cache is down)
	 */
	public boolean keyExists(String key) {
		return (this.get(key, null, true) != null);
	}

	/**
	 * Deletes an object from cache given cache key.
	 * 
	 * @param key
	 *            the key to be removed
	 * @return <code>true</code>, if the data was deleted successfully
	 */
	public boolean delete(String key) {
		return delete(key, null, null);
	}

	/**
	 * Deletes an object from cache given cache key and expiration date.
	 * 
	 * @param key
	 *            the key to be removed
	 * @param expiry
	 *            when to expire the record.
	 * @return <code>true</code>, if the data was deleted successfully
	 */
	public boolean delete(String key, Date expiry) {
		return delete(key, null, expiry);
	}

	/**
	 * Deletes an object from cache given cache key, a delete time, and an
	 * optional hashcode.
	 * 
	 * The item is immediately made non retrievable.<br/>
	 * Keep in mind {@link #add(String, Object) add} and
	 * {@link #replace(String, Object) replace}<br/>
	 * will fail when used with the same key will fail, until the server reaches
	 * the<br/>
	 * specified time. However, {@link #set(String, Object) set} will succeed,<br/>
	 * and the new value will not be deleted.
	 * 
	 * @param key
	 *            the key to be removed
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @param expiry
	 *            when to expire the record.
	 * @return <code>true</code>, if the data was deleted successfully
	 */
	public boolean delete(String key, Integer hashCode, Date expiry) {

		if (key == null) {
			log.error("null value for key passed to delete()");
			return false;
		}

		try {
			key = sanitizeKey(key);
		} catch (UnsupportedEncodingException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnDelete(this, e, key);

			log.error("failed to sanitize your key!", e);
			return false;
		}

		// return false if unable to get SockIO obj
		if (sock == null) {
			if (errorHandler != null)
				errorHandler.handleErrorOnDelete(this, new IOException(
						"no socket to server available"), key);
			return false;
		}

		// build command
		StringBuilder command = new StringBuilder("delete ").append(key);
		if (expiry != null)
			command.append(" " + expiry.getTime() / 1000);

		command.append("\r\n");

		try {
			sock.write(command.toString().getBytes());
			sock.flush();

			// if we get appropriate response back, then we return true
			String line = sock.readLine();
			if (DELETED.equals(line)) {
				if (log.isInfoEnabled())
					log.info("++++ deletion of key: " + key
							+ " from cache was a success");

				// return sock to pool and bail here
				sock.close();
				sock = null;
				return true;
			} else if (NOTFOUND.equals(line)) {
				if (log.isInfoEnabled())
					log.info("++++ deletion of key: " + key
							+ " from cache failed as the key was not found");
			} else {
				log.error("++++ error deleting key: " + key);
				log.error("++++ server response: " + line);
			}
		} catch (IOException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnDelete(this, e, key);

			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on delete");
			log.error(e.getMessage(), e);

			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error("++++ failed to close socket : " + sock.toString());
			}

			sock = null;
		}

		if (sock != null) {
			sock.close();
			sock = null;
		}

		return false;
	}

	/**
	 * Stores data on the server; only the key and the value are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean set(String key, Object value) {
		return set("set", key, value, null, null, primitiveAsString);
	}

	/**
	 * Stores data on the server; only the key and the value are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean set(String key, Object value, Integer hashCode) {
		return set("set", key, value, null, hashCode, primitiveAsString);
	}

	/**
	 * Stores data on the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @return true, if the data was successfully stored
	 */
	public boolean set(String key, Object value, Date expiry) {
		return set("set", key, value, expiry, null, primitiveAsString);
	}

	/**
	 * Stores data on the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean set(String key, Object value, Date expiry, Integer hashCode) {
		return set("set", key, value, expiry, hashCode, primitiveAsString);
	}

	/**
	 * Adds data to the server; only the key and the value are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean add(String key, Object value) {
		return set("add", key, value, null, null, primitiveAsString);
	}

	/**
	 * Adds data to the server; the key, value, and an optional hashcode are
	 * passed in.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean add(String key, Object value, Integer hashCode) {
		return set("add", key, value, null, hashCode, primitiveAsString);
	}

	/**
	 * Adds data to the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @return true, if the data was successfully stored
	 */
	public boolean add(String key, Object value, Date expiry) {
		return set("add", key, value, expiry, null, primitiveAsString);
	}

	/**
	 * Adds data to the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean add(String key, Object value, Date expiry, Integer hashCode) {
		return set("add", key, value, expiry, hashCode, primitiveAsString);
	}

	/**
	 * Updates data on the server; only the key and the value are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @return true, if the data was successfully stored
	 */
	public boolean replace(String key, Object value) {
		return set("replace", key, value, null, null, primitiveAsString);
	}

	/**
	 * Updates data on the server; only the key and the value and an optional
	 * hash are specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean replace(String key, Object value, Integer hashCode) {
		return set("replace", key, value, null, hashCode, primitiveAsString);
	}

	/**
	 * Updates data on the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @return true, if the data was successfully stored
	 */
	public boolean replace(String key, Object value, Date expiry) {
		return set("replace", key, value, expiry, null, primitiveAsString);
	}

	/**
	 * Updates data on the server; the key, value, and an expiration time are
	 * specified.
	 * 
	 * @param key
	 *            key to store data under
	 * @param value
	 *            value to store
	 * @param expiry
	 *            when to expire the record
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true, if the data was successfully stored
	 */
	public boolean replace(String key, Object value, Date expiry,
			Integer hashCode) {
		return set("replace", key, value, expiry, hashCode, primitiveAsString);
	}

	/**
	 * Stores data to cache.
	 * 
	 * If data does not already exist for this key on the server, or if the key
	 * is being<br/>
	 * deleted, the specified value will not be stored.<br/>
	 * The server will automatically delete the value when the expiration time
	 * has been reached.<br/>
	 * <br/>
	 * If compression is enabled, and the data is longer than the compression
	 * threshold<br/>
	 * the data will be stored in compressed form.<br/>
	 * <br/>
	 * As of the current release, all objects stored will use java
	 * serialization.
	 * 
	 * @param cmdname
	 *            action to take (set, add, replace)
	 * @param key
	 *            key to store cache under
	 * @param value
	 *            object to cache
	 * @param expiry
	 *            expiration
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @param asString
	 *            store this object as a string?
	 * @return true/false indicating success
	 */
	private boolean set(String cmdname, String key, Object value, Date expiry,
			Integer hashCode, boolean asString) {

		if (cmdname == null || cmdname.trim().equals("") || key == null) {
			log.error("key is null or cmd is null/empty for set()");
			return false;
		}

		try {
			key = sanitizeKey(key);
		} catch (UnsupportedEncodingException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnSet(this, e, key);

			log.error("failed to sanitize your key!", e);
			return false;
		}

		if (value == null) {
			log.error("trying to store a null value to cache");
			return false;
		}

		if (sock == null) {
			if (errorHandler != null)
				errorHandler.handleErrorOnSet(this, new IOException(
						"no socket to server available"), key);
			return false;
		}

		if (expiry == null)
			expiry = new Date(0);

		// store flags
		int flags = 0;

		// byte array to hold data
		byte[] val;

		if (NativeHandler.isHandled(value)) {

			if (asString) {
				// useful for sharing data between java and non-java
				// and also for storing ints for the increment method
				try {
					if (log.isInfoEnabled())
						log.info("++++ storing data as a string for key: "
								+ key + " for class: "
								+ value.getClass().getName());
					val = value.toString().getBytes(defaultEncoding);
				} catch (UnsupportedEncodingException ue) {

					// if we have an errorHandler, use its hook
					if (errorHandler != null)
						errorHandler.handleErrorOnSet(this, ue, key);

					log.error("invalid encoding type used: " + defaultEncoding,
							ue);
					sock.close();
					sock = null;
					return false;
				}
			} else {
				try {
					if (log.isInfoEnabled())
						log.info("Storing with native handler...");
					flags |= NativeHandler.getMarkerFlag(value);
					val = NativeHandler.encode(value);
				} catch (Exception e) {

					// if we have an errorHandler, use its hook
					if (errorHandler != null)
						errorHandler.handleErrorOnSet(this, e, key);

					log.error("Failed to native handle obj", e);

					sock.close();
					sock = null;
					return false;
				}
			}
		} else {
			// always serialize for non-primitive types
			try {
				if (log.isInfoEnabled())
					log.info("++++ serializing for key: " + key
							+ " for class: " + value.getClass().getName());
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				(new ObjectOutputStream(bos)).writeObject(value);
				val = bos.toByteArray();
				flags |= F_SERIALIZED;
			} catch (IOException e) {

				// if we have an errorHandler, use its hook
				if (errorHandler != null)
					errorHandler.handleErrorOnSet(this, e, key);

				// if we fail to serialize, then
				// we bail
				log.error("failed to serialize obj", e);
				log.error(value.toString());

				// return socket to pool and bail
				sock.close();
				sock = null;
				return false;
			}
		}

		// now try to compress if we want to
		// and if the length is over the threshold
		if (compressEnable && val.length > compressThreshold) {

			try {
				if (log.isInfoEnabled()) {
					log.info("++++ trying to compress data");
					log.info("++++ size prior to compression: " + val.length);
				}
				ByteArrayOutputStream bos = new ByteArrayOutputStream(
						val.length);
				GZIPOutputStream gos = new GZIPOutputStream(bos);
				gos.write(val, 0, val.length);
				gos.finish();
				gos.close();

				// store it and set compression flag
				val = bos.toByteArray();
				flags |= F_COMPRESSED;

				if (log.isInfoEnabled())
					log.info("++++ compression succeeded, size after: "
							+ val.length);
			} catch (IOException e) {

				// if we have an errorHandler, use its hook
				if (errorHandler != null)
					errorHandler.handleErrorOnSet(this, e, key);

				log.error("IOException while compressing stream: "
						+ e.getMessage());
				log.error("storing data uncompressed");
			}
		}

		// now write the data to the cache server
		try {
			String cmd = String.format("%s %s %d %d %d\r\n", cmdname, key,
					flags, (expiry.getTime() / 1000), val.length);
			sock.write(cmd.getBytes());
			sock.write(val);
			sock.write("\r\n".getBytes());
			sock.flush();

			// get result code
			String line = sock.readLine();
			if (log.isInfoEnabled())
				log.info("++++ memcache cmd (result code): " + cmd + " ("
						+ line + ")");

			if (STORED.equals(line)) {
				if (log.isInfoEnabled())
					log.info("++++ data successfully stored for key: " + key);
				// sock.close();
				// sock = null;
				return true;
			} else if (NOTSTORED.equals(line)) {
				if (log.isInfoEnabled())
					log.info("++++ data not stored in cache for key: " + key);
			} else {
				log.error("++++ error storing data in cache for key: " + key
						+ " -- length: " + val.length);
				log.error("++++ server response: " + line);
			}
		} catch (IOException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnSet(this, e, key);

			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on set");
			log.error(e.getMessage(), e);

			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error("++++ failed to close socket : " + sock.toString());
			}

			sock = null;
		}

		if (sock != null) {
			sock.close();
			sock = null;
		}

		return false;
	}

	/**
	 * Store a counter to memcached given a key
	 * 
	 * @param key
	 *            cache key
	 * @param counter
	 *            number to store
	 * @return true/false indicating success
	 */
	public boolean storeCounter(String key, long counter) {
		return set("set", key, new Long(counter), null, null, true);
	}

	/**
	 * Store a counter to memcached given a key
	 * 
	 * @param key
	 *            cache key
	 * @param counter
	 *            number to store
	 * @return true/false indicating success
	 */
	public boolean storeCounter(String key, Long counter) {
		return set("set", key, counter, null, null, true);
	}

	/**
	 * Store a counter to memcached given a key
	 * 
	 * @param key
	 *            cache key
	 * @param counter
	 *            number to store
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return true/false indicating success
	 */
	public boolean storeCounter(String key, Long counter, Integer hashCode) {
		return set("set", key, counter, null, hashCode, true);
	}

	/**
	 * Returns value in counter at given key as long.
	 * 
	 * @param key
	 *            cache ket
	 * @return counter value or -1 if not found
	 */
	public long getCounter(String key) {
		return getCounter(key, null);
	}

	/**
	 * Returns value in counter at given key as long.
	 * 
	 * @param key
	 *            cache ket
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return counter value or -1 if not found
	 */
	public long getCounter(String key, Integer hashCode) {

		if (key == null) {
			log.error("null key for getCounter()");
			return -1;
		}

		long counter = -1;
		try {
			counter = Long.parseLong((String) get(key, hashCode, true));
		} catch (Exception ex) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, ex, key);

			// not found or error getting out
			if (log.isInfoEnabled())
				log.info(String.format(
						"Failed to parse Long value for key: %s", key));
		}

		return counter;
	}

	/**
	 * Thread safe way to initialize and increment a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @return value of incrementer
	 */
	public long addOrIncr(String key) {
		return addOrIncr(key, 0, null);
	}

	/**
	 * Thread safe way to initialize and increment a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            value to set or increment by
	 * @return value of incrementer
	 */
	public long addOrIncr(String key, long inc) {
		return addOrIncr(key, inc, null);
	}

	/**
	 * Thread safe way to initialize and increment a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            value to set or increment by
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return value of incrementer
	 */
	public long addOrIncr(String key, long inc, Integer hashCode) {
		boolean ret = set("add", key, new Long(inc), null, hashCode, true);

		if (ret) {
			return inc;
		} else {
			return incrdecr("incr", key, inc, hashCode);
		}
	}

	/**
	 * Thread safe way to initialize and decrement a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @return value of incrementer
	 */
	public long addOrDecr(String key) {
		return addOrDecr(key, 0, null);
	}

	/**
	 * Thread safe way to initialize and decrement a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            value to set or increment by
	 * @return value of incrementer
	 */
	public long addOrDecr(String key, long inc) {
		return addOrDecr(key, inc, null);
	}

	/**
	 * Thread safe way to initialize and decrement a counter.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            value to set or increment by
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return value of incrementer
	 */
	public long addOrDecr(String key, long inc, Integer hashCode) {
		boolean ret = set("add", key, new Long(inc), null, hashCode, true);

		if (ret) {
			return inc;
		} else {
			return incrdecr("decr", key, inc, hashCode);
		}
	}

	/**
	 * Increment the value at the specified key by 1, and then return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long incr(String key) {
		return incrdecr("incr", key, 1, null);
	}

	/**
	 * Increment the value at the specified key by passed in val.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            how much to increment by
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long incr(String key, long inc) {
		return incrdecr("incr", key, inc, null);
	}

	/**
	 * Increment the value at the specified key by the specified increment, and
	 * then return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            how much to increment by
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long incr(String key, long inc, Integer hashCode) {
		return incrdecr("incr", key, inc, hashCode);
	}

	/**
	 * Decrement the value at the specified key by 1, and then return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long decr(String key) {
		return incrdecr("decr", key, 1, null);
	}

	/**
	 * Decrement the value at the specified key by passed in value, and then
	 * return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            how much to increment by
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long decr(String key, long inc) {
		return incrdecr("decr", key, inc, null);
	}

	/**
	 * Decrement the value at the specified key by the specified increment, and
	 * then return it.
	 * 
	 * @param key
	 *            key where the data is stored
	 * @param inc
	 *            how much to increment by
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return -1, if the key is not found, the value after incrementing
	 *         otherwise
	 */
	public long decr(String key, long inc, Integer hashCode) {
		return incrdecr("decr", key, inc, hashCode);
	}

	/**
	 * Increments/decrements the value at the specified key by inc.
	 * 
	 * Note that the server uses a 32-bit unsigned integer, and checks for<br/>
	 * underflow. In the event of underflow, the result will be zero. Because<br/>
	 * Java lacks unsigned types, the value is returned as a 64-bit integer.<br/>
	 * The server will only decrement a value if it already exists;<br/>
	 * if a value is not found, -1 will be returned.
	 * 
	 * @param cmdname
	 *            increment/decrement
	 * @param key
	 *            cache key
	 * @param inc
	 *            amount to incr or decr
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return new value or -1 if not exist
	 */
	private long incrdecr(String cmdname, String key, long inc, Integer hashCode) {

		if (key == null) {
			log.error("null key for incrdecr()");
			return -1;
		}

		try {
			key = sanitizeKey(key);
		} catch (UnsupportedEncodingException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, e, key);

			log.error("failed to sanitize your key!", e);
			return -1;
		}

		if (sock == null) {
			if (errorHandler != null)
				errorHandler.handleErrorOnSet(this, new IOException(
						"no socket to server available"), key);
			return -1;
		}

		try {
			String cmd = String.format("%s %s %d\r\n", cmdname, key, inc);
			if (log.isDebugEnabled())
				log.debug("++++ memcache incr/decr command: " + cmd);

			sock.write(cmd.getBytes());
			sock.flush();

			// get result back
			String line = sock.readLine();

			if (line.matches("\\d+")) {

				// return sock to pool and return result
				sock.close();
				try {
					return Long.parseLong(line);
				} catch (Exception ex) {

					// if we have an errorHandler, use its hook
					if (errorHandler != null)
						errorHandler.handleErrorOnGet(this, ex, key);

					log.error(String.format(
							"Failed to parse Long value for key: %s", key));
				}
			} else if (NOTFOUND.equals(line)) {
				if (log.isInfoEnabled())
					log.info("++++ key not found to incr/decr for key: " + key);
			} else {
				log.error("++++ error incr/decr key: " + key);
				log.error("++++ server response: " + line);
			}
		} catch (IOException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, e, key);

			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on incr/decr");
			log.error(e.getMessage(), e);

			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error("++++ failed to close socket : " + sock.toString());
			}

			sock = null;
		}

		if (sock != null) {
			sock.close();
			sock = null;
		}

		return -1;
	}

	/**
	 * Retrieve a key from the server, using a specific hash.
	 * 
	 * If the data was compressed or serialized when compressed, it will
	 * automatically<br/>
	 * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 * <br/>
	 * Non-serialized data will be returned as a string, so explicit conversion
	 * to<br/>
	 * numeric types will be necessary, if desired<br/>
	 * 
	 * @param key
	 *            key where data is stored
	 * @return the object that was previously stored, or null if it was not
	 *         previously stored
	 */
	public Object get(String key) {
		return get(key, null, false);
	}

	/**
	 * Retrieve a key from the server, using a specific hash.
	 * 
	 * If the data was compressed or serialized when compressed, it will
	 * automatically<br/>
	 * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 * <br/>
	 * Non-serialized data will be returned as a string, so explicit conversion
	 * to<br/>
	 * numeric types will be necessary, if desired<br/>
	 * 
	 * @param key
	 *            key where data is stored
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return the object that was previously stored, or null if it was not
	 *         previously stored
	 */
	public Object get(String key, Integer hashCode) {
		return get(key, hashCode, false);
	}

	/**
	 * Retrieve a key from the server, using a specific hash.
	 * 
	 * If the data was compressed or serialized when compressed, it will
	 * automatically<br/>
	 * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
	 * <br/>
	 * Non-serialized data will be returned as a string, so explicit conversion
	 * to<br/>
	 * numeric types will be necessary, if desired<br/>
	 * 
	 * @param key
	 *            key where data is stored
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @param asString
	 *            if true, then return string val
	 * @return the object that was previously stored, or null if it was not
	 *         previously stored
	 */
	public Object get(String key, Integer hashCode, boolean asString) {

		if (key == null) {
			log.error("key is null for get()");
			return null;
		}

		try {
			key = sanitizeKey(key);
		} catch (UnsupportedEncodingException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, e, key);

			log.error("failed to sanitize your key!", e);
			return null;
		}

		if (sock == null) {
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, new IOException(
						"no socket to server available"), key);
			return null;
		}

		try {
			String cmd = "get " + key + "\r\n";

			if (log.isDebugEnabled())
				log.debug("++++ memcache get command: " + cmd);

			sock.write(cmd.getBytes());
			sock.flush();

			// ready object
			Object o = null;

			while (true) {
				String line = sock.readLine();

				if (log.isDebugEnabled())
					log.debug("++++ line: " + line);

				if (line.startsWith(VALUE)) {
					String[] info = line.split(" ");
					int flag = Integer.parseInt(info[2]);
					int length = Integer.parseInt(info[3]);

					if (log.isDebugEnabled()) {
						log.debug("++++ key: " + key);
						log.debug("++++ flags: " + flag);
						log.debug("++++ length: " + length);
					}

					// read obj into buffer
					byte[] buf = new byte[length];
					sock.read(buf);
					sock.clearEOL();

					if ((flag & F_COMPRESSED) == F_COMPRESSED) {
						try {
							// read the input stream, and write to a byte array
							// output stream since
							// we have to read into a byte array, but we don't
							// know how large it
							// will need to be, and we don't want to resize it a
							// bunch
							GZIPInputStream gzi = new GZIPInputStream(
									new ByteArrayInputStream(buf));
							ByteArrayOutputStream bos = new ByteArrayOutputStream(
									buf.length);

							int count;
							byte[] tmp = new byte[2048];
							while ((count = gzi.read(tmp)) != -1) {
								bos.write(tmp, 0, count);
							}

							// store uncompressed back to buffer
							buf = bos.toByteArray();
							gzi.close();
						} catch (IOException e) {

							// if we have an errorHandler, use its hook
							if (errorHandler != null)
								errorHandler.handleErrorOnGet(this, e, key);

							log.error("++++ IOException thrown while trying to uncompress input stream for key: "
									+ key + " -- " + e.getMessage());
							throw new NestedIOException(
									"++++ IOException thrown while trying to uncompress input stream for key: "
											+ key, e);
						}
					}

					// we can only take out serialized objects
					if ((flag & F_SERIALIZED) != F_SERIALIZED) {
						if (primitiveAsString || asString) {
							// pulling out string value
							if (log.isInfoEnabled())
								log.info("++++ retrieving object and stuffing into a string.");
							o = new String(buf, defaultEncoding);
						} else {
							// decoding object
							try {
								o = NativeHandler.decode(buf, flag);
							} catch (Exception e) {

								// if we have an errorHandler, use its hook
								if (errorHandler != null)
									errorHandler.handleErrorOnGet(this, e, key);

								log.error(
										"++++ Exception thrown while trying to deserialize for key: "
												+ key, e);
								throw new NestedIOException(e);
							}
						}
					} else {
						// deserialize if the data is serialized
						ContextObjectInputStream ois = new ContextObjectInputStream(
								new ByteArrayInputStream(buf), classLoader);
						try {
							o = ois.readObject();
							if (log.isInfoEnabled())
								log.info("++++ deserializing " + o.getClass());
						} catch (Exception e) {
							if (errorHandler != null)
								errorHandler.handleErrorOnGet(this, e, key);

							o = null;
							log.error("++++ Exception thrown while trying to deserialize for key: "
									+ key + " -- " + e.getMessage());
						}
					}
				} else if (END.equals(line)) {
					if (log.isDebugEnabled())
						log.debug("++++ finished reading from cache server");
					break;
				}
			}

			// sock.close();
			// sock = null;
			return o;
		} catch (IOException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnGet(this, e, key);

			// exception thrown
			log.error("++++ exception thrown while trying to get object from cache for key: "
					+ key + " -- " + e.getMessage());

			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error("++++ failed to close socket : " + sock.toString());
			}
			sock = null;
		}

		if (sock != null)
			sock.close();

		return null;
	}

	/**
	 * Retrieve multiple objects from the memcache.
	 * 
	 * This is recommended over repeated calls to {@link #get(String) get()},
	 * since it<br/>
	 * is more efficient.<br/>
	 * 
	 * @param keys
	 *            String array of keys to retrieve
	 * @param hashCodes
	 *            if not null, then the Integer array of hashCodes
	 * @param asString
	 *            if true, retrieve string vals
	 * @return Object array ordered in same order as key array containing
	 *         results
	 */

	/**
	 * Retrieve multiple objects from the memcache.
	 * 
	 * This is recommended over repeated calls to {@link #get(String) get()},
	 * since it<br/>
	 * is more efficient.<br/>
	 * 
	 * @param keys
	 *            String array of keys to retrieve
	 * @return a hashmap with entries for each key is found by the server, keys
	 *         that are not found are not entered into the hashmap, but
	 *         attempting to retrieve them from the hashmap gives you null.
	 */

	/**
	 * This method loads the data from cache into a Map.
	 * 
	 * Pass a SockIO object which is ready to receive data and a HashMap<br/>
	 * to store the results.
	 * 
	 * @param sock
	 *            socket waiting to pass back data
	 * @param hm
	 *            hashmap to store data into
	 * @param asString
	 *            if true, and if we are using NativehHandler, return string val
	 * @throws IOException
	 *             if io exception happens while reading from socket
	 */
	private void loadMulti(LineInputStream input, Map<String, Object> hm,
			boolean asString) throws IOException {

		while (true) {
			String line = input.readLine();
			if (log.isDebugEnabled())
				log.debug("++++ line: " + line);

			if (line.startsWith(VALUE)) {
				String[] info = line.split(" ");
				String key = info[1];
				int flag = Integer.parseInt(info[2]);
				int length = Integer.parseInt(info[3]);

				if (log.isDebugEnabled()) {
					log.debug("++++ key: " + key);
					log.debug("++++ flags: " + flag);
					log.debug("++++ length: " + length);
				}

				// read obj into buffer
				byte[] buf = new byte[length];
				input.read(buf);
				input.clearEOL();

				// ready object
				Object o;

				// check for compression
				if ((flag & F_COMPRESSED) == F_COMPRESSED) {
					try {
						// read the input stream, and write to a byte array
						// output stream since
						// we have to read into a byte array, but we don't know
						// how large it
						// will need to be, and we don't want to resize it a
						// bunch
						GZIPInputStream gzi = new GZIPInputStream(
								new ByteArrayInputStream(buf));
						ByteArrayOutputStream bos = new ByteArrayOutputStream(
								buf.length);

						int count;
						byte[] tmp = new byte[2048];
						while ((count = gzi.read(tmp)) != -1) {
							bos.write(tmp, 0, count);
						}

						// store uncompressed back to buffer
						buf = bos.toByteArray();
						gzi.close();
					} catch (IOException e) {

						// if we have an errorHandler, use its hook
						if (errorHandler != null)
							errorHandler.handleErrorOnGet(this, e, key);

						log.error("++++ IOException thrown while trying to uncompress input stream for key: "
								+ key + " -- " + e.getMessage());
						throw new NestedIOException(
								"++++ IOException thrown while trying to uncompress input stream for key: "
										+ key, e);
					}
				}

				// we can only take out serialized objects
				if ((flag & F_SERIALIZED) != F_SERIALIZED) {
					if (primitiveAsString || asString) {
						// pulling out string value
						if (log.isInfoEnabled())
							log.info("++++ retrieving object and stuffing into a string.");
						o = new String(buf, defaultEncoding);
					} else {
						// decoding object
						try {
							o = NativeHandler.decode(buf, flag);
						} catch (Exception e) {

							// if we have an errorHandler, use its hook
							if (errorHandler != null)
								errorHandler.handleErrorOnGet(this, e, key);

							log.error("++++ Exception thrown while trying to deserialize for key: "
									+ key + " -- " + e.getMessage());
							throw new NestedIOException(e);
						}
					}
				} else {
					// deserialize if the data is serialized
					ContextObjectInputStream ois = new ContextObjectInputStream(
							new ByteArrayInputStream(buf), classLoader);
					try {
						o = ois.readObject();
						if (log.isInfoEnabled())
							log.info("++++ deserializing " + o.getClass());
					} catch (InvalidClassException e) {
						/*
						 * Errors de-serializing are to be expected in the case
						 * of a long running server that spans client restarts
						 * with updated classes.
						 */
						// if we have an errorHandler, use its hook
						if (errorHandler != null)
							errorHandler.handleErrorOnGet(this, e, key);

						o = null;
						log.error("++++ InvalidClassException thrown while trying to deserialize for key: "
								+ key + " -- " + e.getMessage());
					} catch (ClassNotFoundException e) {

						// if we have an errorHandler, use its hook
						if (errorHandler != null)
							errorHandler.handleErrorOnGet(this, e, key);

						o = null;
						log.error("++++ ClassNotFoundException thrown while trying to deserialize for key: "
								+ key + " -- " + e.getMessage());
					}
				}

				// store the object into the cache
				if (o != null)
					hm.put(key, o);
			} else if (END.equals(line)) {
				if (log.isDebugEnabled())
					log.debug("++++ finished reading from cache server");
				break;
			}
		}
	}

	private String sanitizeKey(String key) throws UnsupportedEncodingException {
		return (sanitizeKeys) ? URLEncoder.encode(key, "UTF-8") : key;
	}

	/**
	 * Invalidates the entire cache.
	 * 
	 * Will return true only if succeeds in clearing all servers.
	 * 
	 * @return success true/false
	 */
	public boolean flushAll() {
		return flushAll(null);
	}

	/**
	 * Invalidates the entire cache.
	 * 
	 * Will return true only if succeeds in clearing all servers. If pass in
	 * null, then will try to flush all servers.
	 * 
	 * @param servers
	 *            optional array of host(s) to flush (host:port)
	 * @return success true/false
	 */
	public boolean flushAll(String[] servers) {
		boolean success = true;
		if (sock == null) {
			log.error("++++ unable to get connection to : " + HostName);
			success = false;
			if (errorHandler != null)
				errorHandler.handleErrorOnFlush(this, new IOException(
						"no socket to server available"));

			// build command
			String command = "flush_all\r\n";

			try {
				sock.write(command.getBytes());
				sock.flush();

				// if we get appropriate response back, then we return true
				String line = sock.readLine();
				success = (OK.equals(line)) ? success && true : false;
			} catch (IOException e) {

				// if we have an errorHandler, use its hook
				if (errorHandler != null)
					errorHandler.handleErrorOnFlush(this, e);

				// exception thrown
				log.error("++++ exception thrown while writing bytes to server on flushAll");
				log.error(e.getMessage(), e);

				try {
					sock.trueClose();
				} catch (IOException ioe) {
					log.error("++++ failed to close socket : "
							+ sock.toString());
				}

				success = false;
				sock = null;
			}

			if (sock != null) {
				sock.close();
				sock = null;
			}
		}

		return success;
	}

	public Map stats() {
		return stats("stats\r\n", STATS);
	}

	public Map statsItems() {
		return stats("stats items\r\n", STATS);
	}

	/**
	 * Retrieves stats for passed in servers (or all servers).
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains slabs stats with slabnumber:field as key and value as value.
	 * 
	 * @param servers
	 *            string array of servers to retrieve stats from, or all if this
	 *            is null
	 * @return Stats map
	 */
	public Map statsSlabs() {
		return stats("stats slabs\r\n", STATS);
	}

	/**
	 * Retrieves stats for passed in servers (or all servers).
	 * 
	 * Returns a map keyed on the servername. The value is another map which
	 * contains cachedump stats with the cachekey as key and byte size and unix
	 * timestamp as value.
	 * 
	 * @param servers
	 *            string array of servers to retrieve stats from, or all if this
	 *            is null
	 * @param slabNumber
	 *            the item number of the cache dump
	 * @return Stats map
	 */
	public Map statsCacheDump(int slabNumber, int limit) {
		return stats(
				String.format("stats cachedump %d %d\r\n", slabNumber, limit),
				ITEM);
	}

	private Map stats(String command, String lineStart) {

		if (command == null || command.trim().equals("")) {
			log.error("++++ invalid / missing command for stats()");
			return null;
		}

		if (sock == null) {
			log.error("++++ unable to get connection to : " + HostName);
			if (errorHandler != null)
				errorHandler.handleErrorOnStats(this, new IOException(
						"no socket to server available"));
			return null;
		}
		// map to hold key value pairs
		Map<String, String> stats = new HashMap<String, String>();
		// build command
		try {
			sock.write(command.getBytes());
			sock.flush();

			// loop over results
			while (true) {
				String line = sock.readLine();
				if (log.isDebugEnabled())
					log.debug("++++ line: " + line);

				if (line.startsWith(lineStart)) {
					String[] info = line.split(" ", 3);
					String key = info[1];
					String value = info[2];

					if (log.isDebugEnabled()) {
						log.debug("++++ key  : " + key);
						log.debug("++++ value: " + value);
					}

					stats.put(key, value);
				} else if (END.equals(line)) {
					// finish when we get end from server
					if (log.isDebugEnabled())
						log.debug("++++ finished reading from cache server");
					break;
				} else if (line.startsWith(ERROR)
						|| line.startsWith(CLIENT_ERROR)
						|| line.startsWith(SERVER_ERROR)) {
					log.error("++++ failed to query stats");
					log.error("++++ server response: " + line);
					break;
				}
			}
		} catch (IOException e) {

			// if we have an errorHandler, use its hook
			if (errorHandler != null)
				errorHandler.handleErrorOnStats(this, e);

			// exception thrown
			log.error("++++ exception thrown while writing bytes to server on stats");
			log.error(e.getMessage(), e);

			try {
				sock.trueClose();
			} catch (IOException ioe) {
				log.error("++++ failed to close socket : " + sock.toString());
			}

			sock = null;
		}

		if (sock != null) {
			sock.close();
			sock = null;
		}

		return stats;
	}
}
