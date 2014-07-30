package memcached;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

/**
 * MemCached client for Java, utility class for Socket IO.
 * 
 * This class is a wrapper around a Socket and its streams.
 * 
 */
public class SockIO implements LineInputStream {
	// logger
	private static Logger log = Logger.getLogger(SockIO.class.getName());

	// data
	private String host;
	private Socket sock;

	private DataInputStream in;
	private BufferedOutputStream out;

	/**
	 * creates a new SockIO object wrapping a socket connection to host:port,
	 * and its input and output streams
	 * 
	 * @param pool
	 *            Pool this object is tied to
	 * @param host
	 *            host to connect to
	 * @param port
	 *            port to connect to
	 * @param timeout
	 *            int ms to block on data for read
	 * @param connectTimeout
	 *            timeout (in ms) for initial connection
	 * @param noDelay
	 *            TCP NODELAY option?
	 * @throws IOException
	 *             if an io error occurrs when creating socket
	 * @throws UnknownHostException
	 *             if hostname is invalid
	 */
	public SockIO(String host, int port, int timeout, int connectTimeout,
			boolean noDelay) throws IOException, UnknownHostException {
		// get a socket channel
		sock = getSocket(host, port, connectTimeout);

		if (timeout >= 0)
			sock.setSoTimeout(timeout);

		// testing only
		sock.setTcpNoDelay(noDelay);

		// wrap streams
		in = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
		out = new BufferedOutputStream(sock.getOutputStream());

		this.host = host + ":" + port;
	}

	/**
	 * creates a new SockIO object wrapping a socket connection to host:port,
	 * and its input and output streams
	 * 
	 * @param host
	 *            hostname:port
	 * @param timeout
	 *            read timeout value for connected socket
	 * @param connectTimeout
	 *            timeout for initial connections
	 * @param noDelay
	 *            TCP NODELAY option?
	 * @throws IOException
	 *             if an io error occurrs when creating socket
	 * @throws UnknownHostException
	 *             if hostname is invalid
	 */
	public SockIO(String host, int timeout, int connectTimeout, boolean noDelay)
			throws IOException, UnknownHostException {
		String[] ip = host.split(":");

		// get socket: default is to use non-blocking connect
		sock = getSocket(ip[0], Integer.parseInt(ip[1]), connectTimeout);

		if (timeout >= 0)
			this.sock.setSoTimeout(timeout);

		// testing only
		sock.setTcpNoDelay(noDelay);

		// wrap streams
		in = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
		out = new BufferedOutputStream(sock.getOutputStream());

		this.host = host;
	}

	/**
	 * Method which gets a connection from SocketChannel.
	 * 
	 * @param host
	 *            host to establish connection to
	 * @param port
	 *            port on that host
	 * @param timeout
	 *            connection timeout in ms
	 * 
	 * @return connected socket
	 * @throws IOException
	 *             if errors connecting or if connection times out
	 */
	protected static Socket getSocket(String host, int port, int timeout)
			throws IOException {
		SocketChannel sock = SocketChannel.open();
		sock.socket().connect(new InetSocketAddress(host, port), timeout);
		return sock.socket();
	}

	/**
	 * Lets caller get access to underlying channel.
	 * 
	 * @return the backing SocketChannel
	 */
	public SocketChannel getChannel() {
		return sock.getChannel();
	}

	/**
	 * returns the host this socket is connected to
	 * 
	 * @return String representation of host (hostname:port)
	 */
	public String getHost() {
		return this.host;
	}

	/**
	 * closes socket and all streams connected to it
	 * 
	 * @throws IOException
	 *             if fails to close streams or socket
	 */
	public void trueClose() throws IOException {
		trueClose(true);
	}

	/**
	 * closes socket and all streams connected to it
	 * 
	 * @throws IOException
	 *             if fails to close streams or socket
	 */
	public void trueClose(boolean addToDeadPool) throws IOException {
		if (log.isDebugEnabled())
			log.debug("++++ Closing socket for real: " + toString());

		boolean err = false;
		StringBuilder errMsg = new StringBuilder();

		if (in != null) {
			try {
				in.close();
			} catch (IOException ioe) {
				log.error("++++ error closing input stream for socket: "
						+ toString() + " for host: " + getHost());
				log.error(ioe.getMessage(), ioe);
				errMsg.append("++++ error closing input stream for socket: "
						+ toString() + " for host: " + getHost() + "\n");
				errMsg.append(ioe.getMessage());
				err = true;
			}
		}

		if (out != null) {
			try {
				out.close();
			} catch (IOException ioe) {
				log.error("++++ error closing output stream for socket: "
						+ toString() + " for host: " + getHost());
				log.error(ioe.getMessage(), ioe);
				errMsg.append("++++ error closing output stream for socket: "
						+ toString() + " for host: " + getHost() + "\n");
				errMsg.append(ioe.getMessage());
				err = true;
			}
		}

		if (sock != null) {
			try {
				sock.close();
			} catch (IOException ioe) {
				log.error("++++ error closing socket: " + toString()
						+ " for host: " + getHost());
				log.error(ioe.getMessage(), ioe);
				errMsg.append("++++ error closing socket: " + toString()
						+ " for host: " + getHost() + "\n");
				errMsg.append(ioe.getMessage());
				err = true;
			}
		}

		// check in to pool
		if (addToDeadPool && sock != null) {
			// check in to pool pool.checkIn( this, false );
			System.out.println("check in to pool ");
		}

		in = null;
		out = null;
		sock = null;

		if (err)
			throw new IOException(errMsg.toString());
	}

	/**
	 * sets closed flag and checks in to connection pool but does not close
	 * connections
	 */
	void close() {
		// check in to pool
		if (log.isDebugEnabled())
			log.debug("++++ marking socket (" + this.toString()
					+ ") as closed and available to return to avail pool");
		// pool.checkIn( this );
	}

	/**
	 * checks if the connection is open
	 * 
	 * @return true if connected
	 */
	boolean isConnected() {
		return (sock != null && sock.isConnected());
	}

	/*
	 * checks to see that the connection is still working
	 * 
	 * @return true if still alive
	 */
	boolean isAlive() {

		if (!isConnected())
			return false;

		// try to talk to the server w/ a dumb query to ask its version
		try {
			this.write("version\r\n".getBytes());
			this.flush();
			this.readLine();
		} catch (IOException ex) {
			return false;
		}

		return true;
	}

	/**
	 * reads a line intentionally not using the deprecated readLine method from
	 * DataInputStream
	 * 
	 * @return String that was read in
	 * @throws IOException
	 *             if io problems during read
	 */
	public String readLine() throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to read from closed socket");
			throw new IOException("++++ attempting to read from closed socket");
		}

		byte[] b = new byte[1];
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		boolean eol = false;

		while (in.read(b, 0, 1) != -1) {

			if (b[0] == 13) {
				eol = true;
			} else {
				if (eol) {
					if (b[0] == 10)
						break;

					eol = false;
				}
			}

			// cast byte into char array
			bos.write(b, 0, 1);
		}

		if (bos == null || bos.size() <= 0) {
			throw new IOException(
					"++++ Stream appears to be dead, so closing it down");
		}

		// else return the string
		return bos.toString().trim();
	}

	/**
	 * reads up to end of line and returns nothing
	 * 
	 * @throws IOException
	 *             if io problems during read
	 */
	public void clearEOL() throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to read from closed socket");
			throw new IOException("++++ attempting to read from closed socket");
		}

		byte[] b = new byte[1];
		boolean eol = false;
		while (in.read(b, 0, 1) != -1) {

			// only stop when we see
			// \r (13) followed by \n (10)
			if (b[0] == 13) {
				eol = true;
				continue;
			}

			if (eol) {
				if (b[0] == 10)
					break;

				eol = false;
			}
		}
	}

	/**
	 * reads length bytes into the passed in byte array from dtream
	 * 
	 * @param b
	 *            byte array
	 * @throws IOException
	 *             if io problems during read
	 */
	public int read(byte[] b) throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to read from closed socket");
			throw new IOException("++++ attempting to read from closed socket");
		}

		int count = 0;
		while (count < b.length) {
			int cnt = in.read(b, count, (b.length - count));
			count += cnt;
		}

		return count;
	}

	/**
	 * flushes output stream
	 * 
	 * @throws IOException
	 *             if io problems during read
	 */
	void flush() throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to write to closed socket");
			throw new IOException("++++ attempting to write to closed socket");
		}
		out.flush();
	}

	/**
	 * writes a byte array to the output stream
	 * 
	 * @param b
	 *            byte array to write
	 * @throws IOException
	 *             if an io error happens
	 */
	void write(byte[] b) throws IOException {
		if (sock == null || !sock.isConnected()) {
			log.error("++++ attempting to write to closed socket");
			throw new IOException("++++ attempting to write to closed socket");
		}
		out.write(b);
	}

	/**
	 * use the sockets hashcode for this object so we can key off of SockIOs
	 * 
	 * @return int hashcode
	 */
	public int hashCode() {
		return (sock == null) ? 0 : sock.hashCode();
	}

	/**
	 * returns the string representation of this socket
	 * 
	 * @return string
	 */
	public String toString() {
		return (sock == null) ? "" : sock.toString();
	}

	/**
	 * Hack to reap any leaking children.
	 */
	protected void finalize() throws Throwable {
		try {
			if (sock != null) {
				log.error("++++ closing potentially leaked socket in finalize");
				sock.close();
				sock = null;
			}
		} catch (Throwable t) {
			log.error(t.getMessage(), t);
		} finally {
			super.finalize();
		}
	}
}