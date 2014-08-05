package client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import server.ClientConfig;

public class ClientMgr
{	
	public HashMap<Integer, Client> m_mapLocalClients;
	static ClientMgr clientMgr;
	public int mClientNumber;
	public static int nCopyNode;
	public static int protocol;
	public static final Integer twoPhaseCommit = 0;
	public static final Integer paxos = (nCopyNode-1)-((nCopyNode-1)/2+1);
	public static final Integer weak = nCopyNode-2;
	
	public static ClientMgr getInstance()
	{
		if (clientMgr == null) 
		{
			clientMgr = new ClientMgr();	
		}
		return clientMgr;
	}
	
	public Integer getSize() {
		return m_mapLocalClients.size()+1;
	}
	
	// num 为hash第一个节点
	public boolean isCopyNode(Integer hash)
	{
		if ((mClientNumber-hash + m_mapLocalClients.size()+1)
				%(m_mapLocalClients.size()+1)<nCopyNode) 
		{
			return true;
		}
		return false;
	}
	
	@SuppressWarnings("rawtypes")
	public void init(int num, HashMap<Integer, ClientConfig> hm)
	{
		m_mapLocalClients = new HashMap<Integer, Client>();
		mClientNumber = num;
		
		Iterator iter = hm.entrySet().iterator();
		while (iter.hasNext()) 
		{
			Entry entry = (Entry) iter.next();
			ClientConfig cc = (ClientConfig)entry.getValue();
			if (cc.id != mClientNumber) 
			{
				Client lc = new Client();
				lc.host = cc.host;
				lc.port = cc.client_port;
				lc.id = cc.id;
				m_mapLocalClients.put(lc.id, lc);
				if(lc.init(lc.host, lc.port))
				{
					System.out.println("client connected successful");
				}
			}
		}
	}
	
	
	public String getCopyHost(Integer number, Integer index)
	{
		Client client =m_mapLocalClients.get((number+index)%getSize());
		if (client==null) {
			return null;
		}
		return client.host+client.port;
	}
	
	public String getCurCoyhost(Integer number)
	{
		Client client =m_mapLocalClients.get((number+mClientNumber)%m_mapLocalClients.size());
		if (client==null) {
			return null;
		}
		return client.host+client.port;
	}
	
	@SuppressWarnings("rawtypes")
	public Integer getClientNum(String host)
	{
		Iterator iter = m_mapLocalClients.entrySet().iterator();
		while (iter.hasNext()) 
		{
			Entry entry = (Entry) iter.next();
			Client client = (Client)entry.getValue();
			if (host.compareTo("/"+client.host+":"+client.port) == 0) 
			{
				return client.id;	
			}
		}
		return -1;
	}
	
	@SuppressWarnings("rawtypes")
	public void connect(String host)
	{		
		Iterator iter = m_mapLocalClients.entrySet().iterator();
		while (iter.hasNext()) 
		{
			Entry entry = (Entry) iter.next();
			Client client = (Client)entry.getValue();
			if (client.host == host) 
			{
				client.init(client.host, client.port);	
				break;
			}
		}
	}
}
