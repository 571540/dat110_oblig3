package no.hvl.dat110.mutexprocess;

import java.io.File;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import no.hvl.dat110.interfaces.ProcessInterface;
import no.hvl.dat110.util.Util;

public class MutexProcess extends UnicastRemoteObject implements ProcessInterface {

	private static final long serialVersionUID = 1L;

	private int processId;
	private String procStubname;
	private int counter;
	private List<Message> queueACK;
	private File localfile;
	private String filename = "file1.txt";
	private int version = 0;
	private List<String> replicas;
	private boolean CS_BUSY = false;
	private boolean WANTS_TO_ENTER_CS = false;
	private int N;
	private int quorum;

	protected MutexProcess(int procId, String stubName) throws RemoteException {
		super();
		this.processId = procId;
		this.procStubname = stubName;
		counter = 0;

		queueACK = new ArrayList<Message>();
		queueACK = Collections.synchronizedList(queueACK);

		replicas = Util.getProcessReplicas();
		N = replicas.size();
		quorum = N / 2 + 1;

		createFile();
	}

	private void createFile() {
		String path = new File(".").getAbsolutePath().replace(".", "");
		System.out.println(path);
		path = path + "/" + procStubname + "/";
		File fpath = new File(path);
		if (!fpath.exists()) {
			boolean suc = fpath.mkdir();
			try {
				if (suc) {
					File file = new File(fpath + "/" + filename);
					file.createNewFile();

				}
			} catch (IOException e) {

				e.printStackTrace();
			}
		}
		this.setFilename(fpath.getAbsolutePath() + "/" + filename);
	}

	public void incrementclock() throws RemoteException {
		counter++;
	}

	public void acquireLock() throws RemoteException {
		CS_BUSY = true;
		incrementclock();
	}

	public void releaseLocks() throws RemoteException {
		CS_BUSY = false;
		WANTS_TO_ENTER_CS = false;
		incrementclock();
	}

	public boolean requestWriteOperation(Message message) throws RemoteException {
		incrementclock();
		message.setClock(counter);
		message.setProcessID(processId);
		message.setOptype(OperationType.WRITE);

		WANTS_TO_ENTER_CS = true;

		boolean resultat;
		resultat = multicastMessage(message, N - 1);

		return resultat;
	}

	public boolean requestReadOperation(Message message) throws RemoteException {
		incrementclock();
		message.setClock(counter);
		message.setProcessID(processId);
		message.setOptype(OperationType.READ);

		WANTS_TO_ENTER_CS = true;

		boolean resultat;
		resultat = multicastMessage(message, N - 1);

		return resultat;
	}

	private boolean multicastMessage(Message message, int n) throws AccessException, RemoteException {

		replicas.remove(this.procStubname);
		Collections.shuffle(replicas);
	
		for (int i = 0; i < n; i++) {
			String temp = replicas.get(i);
			try {
				Message m = Util.registryHandle(temp).onMessageReceived(message);
				queueACK.add(m);
			} catch (NotBoundException e) {
				System.out.println("multicast message failed");
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
			m.setProcessStubName(this.procStubname);
			m.setClock(this.counter);
			m.setAcknowledged(false);
			return m;
		}

		if (WANTS_TO_ENTER_CS) {
			Message m = new Message();
			m.setProcessStubName(this.procStubname);
			m.setClock(this.counter);
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
			m.setProcessStubName(this.procStubname);
			m.setClock(this.counter);
			m.setAcknowledged(true);
			acquireLock();
			return m;
		}
		
		return null;
	}

	public boolean majorityAcknowledged() throws RemoteException {
		int yes = 0;
		for (Message m : queueACK) {
			if (m.isAcknowledged()) {
				yes++;
			}
		}
		return quorum <= yes;
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
			Operations op = new Operations(this, message);
			op.performOperation();
			releaseLocks();
		} else if (message.getOptype() == OperationType.WRITE) {
			releaseLocks();
		}
	}

	@Override
	public void multicastUpdateOrReadReleaseLockOperation(Message message) throws RemoteException {
		replicas.remove(this.procStubname);
		if (message.getOptype() == OperationType.WRITE) {
			for (String replica : replicas) {
				try {
					Util.registryHandle(replica).onReceivedUpdateOperation(message);
				} catch (Exception e) {
					System.out.println("multicast WRITE failed");
				}
			}
		} else if (message.getOptype() == OperationType.READ) {
			for (String replica : replicas) {
				try {
					Util.registryHandle(replica).onReceivedUpdateOperation(message);
				} catch (Exception e) {
					System.out.println("multicast READ failed");
				}
			}
		}

	}

	@Override
	public void multicastVotersDecision(Message message) throws RemoteException {
		replicas.remove(this.procStubname);
		if (message.getOptype() == OperationType.WRITE) {
			for (String replica : replicas) {
				try {
					Util.registryHandle(replica).onReceivedVotersDecision(message);
				} catch (Exception e) {
					System.out.println("multicast voters failed");
				}
			}
		}
	}

	@Override
	public int getProcessID() throws RemoteException {
		return processId;
	}

	@Override
	public int getVersion() throws RemoteException {
		return version;
	}

	public File getLocalfile() {
		return localfile;
	}

	public void setLocalfile(File localfile) {
		this.localfile = localfile;
	}

	@Override
	public void setVersion(int version) throws RemoteException {
		this.version = version;
	}

	public String getFilename() throws RemoteException {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

}
