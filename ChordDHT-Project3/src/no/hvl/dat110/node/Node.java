package no.hvl.dat110.node;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
//import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

public class Node extends UnicastRemoteObject implements ChordNodeInterface {

	private static final long serialVersionUID = 1L;
	private BigInteger nodeID;
	private String nodeIP;
	private ChordNodeInterface successor;
	private ChordNodeInterface predecessor;
	private List<ChordNodeInterface> fingerTable;
	private Set<BigInteger> fileKey;
	private Map<BigInteger, Message> filesMetadata;

	private List<Message> queue;
	private List<Message> queueACK;
	private Set<Message> activenodesforfile;

	private int counter;
	private boolean CS_BUSY = false;
	private boolean WANTS_TO_ENTER_CS = false;
	private int quorum;

	public Node(String nodename) throws RemoteException, UnknownHostException {
		super();

		fingerTable = new ArrayList<ChordNodeInterface>();
		fileKey = new HashSet<BigInteger>();
	
		setNodeIP(nodename);
		BigInteger hashvalue = Hash.hashOf(getNodeIP());
		setNodeID(hashvalue);

		setSuccessor(null);
		setPredecessor(null);

		filesMetadata = new HashMap<BigInteger, Message>();

		counter = 0;
		queue = new ArrayList<Message>();
		queueACK = new ArrayList<Message>();
		queueACK = Collections.synchronizedList(queueACK);
	}

	public BigInteger getNodeID() {
		return nodeID;
	}

	public void setNodeID(BigInteger nodeID) {
		this.nodeID = nodeID;
	}

	public String getNodeIP() {
		return nodeIP;
	}

	public void setNodeIP(String nodeIP) {
		this.nodeIP = nodeIP;
	}

	public ChordNodeInterface getSuccessor() {
		return successor;
	}

	public void setSuccessor(ChordNodeInterface successor) {
		this.successor = successor;
	}

	public ChordNodeInterface getPredecessor() {
		return predecessor;
	}

	public void setPredecessor(ChordNodeInterface predecessor) {
		this.predecessor = predecessor;
	}

	public List<ChordNodeInterface> getFingerTable() {
		return fingerTable;
	}

	public void addToFingerTable(ChordNodeInterface finger) {
		this.fingerTable.add(finger);
	}

	public void removeFromFingerTable(ChordNodeInterface finger) {
		this.fingerTable.remove(finger);
	}

	public void setFingerTable(List<ChordNodeInterface> fingerTable) {
		this.fingerTable = fingerTable;
	}

	public Set<BigInteger> getFileKey() {
		return fileKey;
	}

	public void addToFileKey(BigInteger fileKey) {
		this.fileKey.add(fileKey);
	}

	public void removeFromFileKey(BigInteger fileKey) {
		this.fileKey.remove(fileKey);
	}

	@Override
	public ChordNodeInterface findSuccessor(BigInteger keyid) throws RemoteException {

		ChordNodeInterface succ = this.getSuccessor();

		ChordNodeInterface succstub = Util.registryHandle(succ); 

		if (succstub != null) {
			BigInteger succID = succstub.getNodeID();
			BigInteger nodeID = this.getNodeID();

			Boolean cond = Util.computeLogic(keyid, nodeID.add(new BigInteger("1")), succID);

			if (cond) {
				return succstub;
			} else {
				ChordNodeInterface highest_pred = findHighestPredecessor(keyid);
				return highest_pred.findSuccessor(keyid);
			}
		}

		return null;
	}

	private ChordNodeInterface findHighestPredecessor(BigInteger ID) throws RemoteException {

		BigInteger nodeID = getNodeID();
		List<ChordNodeInterface> fingers = getFingerTable();

		int size = fingers.size() - 1;
		// System.out.println("FingerTable size: "+fingers.size());
		for (int i = 0; i < fingers.size() - 1; i++) {
			int m = size - i;
			ChordNodeInterface ftsucc = fingers.get(m);
			try {
				BigInteger ftsuccID = ftsucc.getNodeID();

				Registry registry = Util.locateRegistry(ftsucc.getNodeIP());

				if (registry == null)
					return this;

				ChordNodeInterface ftsuccnode = (ChordNodeInterface) registry.lookup(ftsuccID.toString());

				boolean cond = Util.computeLogic(ftsuccID, nodeID.add(new BigInteger("1")),
						ID.subtract(new BigInteger("1")));
				if (cond) {
					return ftsuccnode;
				}
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}

		return (ChordNodeInterface) this;
	}

	@Override
	public void notifySuccessor(ChordNodeInterface pred_new) throws RemoteException {

		ChordNodeInterface pred_old = this.getPredecessor();

		if (pred_old == null) {
			this.setPredecessor(pred_new);
			return;
		}

		BigInteger succID = this.getNodeID();
		BigInteger pred_oldID = pred_old.getNodeID();
		BigInteger pred_newID = pred_new.getNodeID();

		boolean cond = Util.computeLogic(pred_newID, pred_oldID.add(new BigInteger("1")),
				succID.add(new BigInteger("1")));
		if (cond) {
			this.setPredecessor(pred_new);
		}

	}

	@Override
	public void createFileInNodeLocalDirectory(String initialcontent, BigInteger destID) throws RemoteException {
		String path = new File(".").getAbsolutePath().replace(".", "");
		// System.out.println(path);
		String destpath = path + "/" + this.getNodeIP() + "/" + destID;

		File file = new File(destpath);

		try {
			if (!file.exists())
				file.createNewFile();
		} catch (IOException e) {
			// e.printStackTrace();
		}

		Util.writetofile(initialcontent, file);

		buildMessage(destID, destpath);
	}

	public Map<BigInteger, Message> getFilesMetadata() throws RemoteException {
		return filesMetadata;
	}

	private void buildMessage(BigInteger destID, String destpath) throws RemoteException {

		Message m = new Message();
		m.setNodeID(getNodeID());
		m.setNodeIP(getNodeIP());
		m.setFilename(destID);
		m.setFilepath(destpath);
		m.setVersion(0);
		filesMetadata.put(destID, m);
	}

	@Override
	public void incrementclock() throws RemoteException {
		counter++;
	}

	@Override
	public void acquireLock() throws RemoteException {
		CS_BUSY = true;
		incrementclock();
	}

	@Override
	public void releaseLocks() throws RemoteException {
		CS_BUSY = false;
		WANTS_TO_ENTER_CS = false;
	}

	@Override
	public boolean requestWriteOperation(Message message) throws RemoteException {
		incrementclock();
		message.setOptype(OperationType.WRITE);
		message.setClock(counter);

		WANTS_TO_ENTER_CS = true;
		boolean resultat = multicastMessage(message);

		return resultat;
	}

	@Override
	public boolean requestReadOperation(Message message) throws RemoteException {
		incrementclock();
		message.setOptype(OperationType.READ);
		message.setClock(counter);

		WANTS_TO_ENTER_CS = true;
		boolean resultat = multicastMessage(message);

		return resultat;
	}

	private boolean multicastMessage(Message message) throws AccessException, RemoteException {
		List<Message> list = new ArrayList<>(activenodesforfile);
		list.remove(message);
		Collections.shuffle(list);

		for (Message msg : list) {
			String nodeID = msg.getNodeID().toString();
			nodeIP = msg.getNodeIP();
			try {
				Registry reg = Util.locateRegistry(nodeIP);
				ChordNodeInterface cni = (ChordNodeInterface) reg.lookup(nodeID);
				Message m = cni.onMessageReceived(message);
				queueACK.add(m);
			} catch (Exception e) {
				System.out.println("Multicast message failed");
				e.printStackTrace();
			}
		}

		boolean resultat = majorityAcknowledged();

		return resultat;
	}

	@Override
	public Message onMessageReceived(Message message) throws RemoteException {

		incrementclock();

		if (CS_BUSY) {
			Message m = new Message();
			m.setClock(this.counter);
			m.setNodeIP(nodeIP);
			m.setNodeID(nodeID);
			m.setAcknowledged(false);
			return m;
		}

		if (WANTS_TO_ENTER_CS) {
			Message m = new Message();
			m.setClock(this.counter);
			m.setNodeIP(nodeIP);
			m.setNodeID(nodeID);
			if (m.getClock() < message.getClock()) {
				m.setAcknowledged(false);
				return m;
			} else {
				m.setAcknowledged(true);
				acquireLock();
				return m;
			}
		}
		
		if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
			Message m = new Message();
			m.setClock(this.counter);
			m.setNodeIP(nodeIP);
			m.setNodeID(nodeID);
			m.setAcknowledged(true);
			acquireLock();
			return m;
		}
		
		return null;
	}

	@Override
	public boolean majorityAcknowledged() throws RemoteException {
		int yes = 0;
		quorum = (this.activenodesforfile.size() / 2) + 1;
		for (Message msg : queueACK) {
			if (msg.isAcknowledged()) {
				yes++;
			}
		}
		return quorum <= yes;
	}

	@Override
	public void setActiveNodesForFile(Set<Message> messages) throws RemoteException {
		activenodesforfile = messages;
	}

	@Override
	public void onReceivedVotersDecision(Message message) throws RemoteException {
		if (!message.isAcknowledged()) {
			releaseLocks();
		}
	}

	@Override
	public void onReceivedUpdateOperation(Message message) throws RemoteException {
		if (message.getOptype() == OperationType.WRITE) {
			Operations op = new Operations(this, message, activenodesforfile);
			op.performOperation();
			releaseLocks();
		} else if (message.getOptype() == OperationType.READ) {
			releaseLocks();
		}
	}

	@Override
	public void multicastUpdateOrReadReleaseLockOperation(Message message) throws RemoteException {
		Operations op = new Operations(this, message, activenodesforfile);
		if (message.getOptype() == OperationType.WRITE) {
			op.multicastOperationToReplicas(message);
		} else {
			op.multicastReadReleaseLocks();
		}
	}

	@Override
	public void multicastVotersDecision(Message message) throws RemoteException {
		for (Message msg : activenodesforfile) {
			String nodeID = msg.getNodeID().toString();
			nodeIP = msg.getNodeIP();
			try {
				Registry reg = Util.locateRegistry(nodeIP);
				ChordNodeInterface cni = (ChordNodeInterface) reg.lookup(nodeID);
				cni.onReceivedVotersDecision(message);
			} catch (Exception e) {
				System.out.println("MulticastVoters message failed");
				e.printStackTrace();
			}
		}
	}
}