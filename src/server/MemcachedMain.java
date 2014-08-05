package server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import client.Client;
import client.ClientMgr;

import common.RegisterHandler;

public class MemcachedMain {
	HashMap<Integer, ClientConfig> m_mapMemcachedClient;
	String webServerHost;
	String protocolName;

	public boolean initConfig() {
		m_mapMemcachedClient = new HashMap<Integer, ClientConfig>();

		File f = new File(System.getProperty("user.dir"));
		String path = f.getPath() + File.separator + "bin" + File.separator;
		readClientsXML(path + "client.xml");
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(path+"config.properties"));
			webServerHost = properties.getProperty("webServerHost").toString();
			ClientMgr.nCopyNode = Integer.parseInt(properties.getProperty("replicasNum"));
			protocolName = properties.getProperty("consistencyProtocol").toString();
			if(protocolName.equals("twoPhaseCommit")){
				ClientMgr.protocol = ClientMgr.twoPhaseCommit;
			}else if(protocolName.equals("paxos")){
				ClientMgr.protocol = ClientMgr.paxos;
			}else if(protocolName.equals("weak")){
				ClientMgr.protocol = ClientMgr.weak;
			}else{
				System.err.print("consistency protocol input error");
				return false;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	public int getMemcachedNumber() {
		System.out.print("Please in put R-Memcached ID:");
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		return Integer.decode(scanner.next());
	}

	public void start() {
		initConfig();
		int num = getMemcachedNumber();

		RegisterHandler.initHandler();
		memSession.getInstance().start(m_mapMemcachedClient.get(num).memcached);

		ClientMgr clientMgr = ClientMgr.getInstance();
		Server server = Server.getInstance();
		server.init(m_mapMemcachedClient.get(num).client_port);
		clientMgr.init(num, m_mapMemcachedClient);

		Client webClient = new Client();
		webClient.init(webServerHost, 8888);
	}

	public static void main(String[] args) {
		MemcachedMain entrance = new MemcachedMain();
		entrance.start();
	}

	// ∂¡»°memcached client≈‰÷√
	public boolean readClientsXML(String str) {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		try {
			factory.setIgnoringElementContentWhitespace(true);

			DocumentBuilder db = factory.newDocumentBuilder();
			Document xmldoc = db.parse(new File(str));
			Element elmtInfo = xmldoc.getDocumentElement();
			NodeList nodes = elmtInfo.getChildNodes();
			for (int i = 0; i < nodes.getLength(); i++) {
				Node result = nodes.item(i);
				if (result.getNodeType() == Node.ELEMENT_NODE
						&& result.getNodeName().equals("client")) {
					NodeList ns = result.getChildNodes();
					ClientConfig localClient = new ClientConfig();
					int m = 0;
					for (int j = 0; j < ns.getLength(); j++) {
						Node record = ns.item(j);
						if (record.getNodeType() == Node.ELEMENT_NODE) {
							if (record.getNodeName().equals("id")) {
								m++;
								localClient.id = Integer.decode(record
										.getTextContent());
							} else if (record.getNodeName().equals("host")) {
								m++;
								localClient.host = record.getTextContent();
							} else if (record.getNodeName().equals(
									"client_port")) {
								m++;
								localClient.client_port = Integer.decode(record
										.getTextContent());
							} else if (record.getNodeName().equals("memcached")) {
								m++;
								localClient.memcached = record.getTextContent();
							}
						}
					}
					if (m == 4) {
						m_mapMemcachedClient.put(localClient.id, localClient);
					}
				}
			}
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}
