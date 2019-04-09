package no.hvl.dat110.file;


/**
 * @author tdoy
 * 
 * dat110 - demo/exercise
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import no.hvl.dat110.node.Message;
import no.hvl.dat110.node.OperationType;
import no.hvl.dat110.node.Operations;
import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

public class FileManager extends Thread {
	
	private BigInteger[] replicafiles;					// array stores replicated files for distribution to matching nodes
	private int nfiles = 4;								// let's assume each node manages nfiles (5 for now) - can be changed from the constructor
	private ChordNodeInterface chordnode;
	
	public FileManager(ChordNodeInterface chordnode, int N) throws RemoteException {
		this.nfiles = N;
		replicafiles = new BigInteger[N];
		this.chordnode = chordnode;
	}
	
	public void run() {
		
		while(true) {
			try {
				distributeReplicaFiles();
				Thread.sleep(3000);
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void createReplicaFiles(String filename) {
		
		for(int i=0; i<nfiles; i++) {
			String replicafile = filename + i;
			replicafiles[i] = Hash.hashOf(replicafile);	

		}
		//System.out.println("Generated replica file keyids for "+chordnode.getNodeIP()+" => "+Arrays.asList(replicafiles));
	}
	
	public void distributeReplicaFiles() throws IOException {
		
		// lookup(keyid) operation for each replica
		// findSuccessor() function should be invoked to find the node with identifier id >= keyid and store the file (create & write the file)
		
		for(int i=0; i<replicafiles.length; i++) {
			BigInteger fileID = (BigInteger) replicafiles[i];
			ChordNodeInterface succOfFileID = chordnode.findSuccessor(fileID);
			
			// if we find the successor node of fileID, we can assign the file to the successor. This should always work even with one node
			if(succOfFileID != null) {
				succOfFileID.addToFileKey(fileID);
				String initialcontent = chordnode.getNodeIP()+"\n"+chordnode.getNodeID();
				succOfFileID.createFileInNodeLocalDirectory(initialcontent, fileID);			// copy the file to the successor local dir
			}			
		}
	}
	
	/**
	 * 
	 * @param filename
	 * @return list of active nodes in a list of messages having the replicas of this file
	 * @throws RemoteException 
	 */
	public Set<Message> requestActiveNodesForFile(String filename) throws RemoteException {
		
		createReplicaFiles(filename);
		Set<Message> meldinger = new HashSet<>();
		for (BigInteger nøkkelID : replicafiles) {
			
			ChordNodeInterface node = chordnode.findSuccessor(nøkkelID);
			if (node != null) {
				Message melding = node.getFilesMetadata().get(nøkkelID);
				if (melding != null && !checkDuplicateActiveNode(meldinger, melding)) {
					meldinger.add(melding);
				}
			}
		}
		return meldinger;
	}
	
	private boolean checkDuplicateActiveNode(Set<Message> activenodesdata, Message nodetocheck) {
		
		for(Message nodedata : activenodesdata) {
			if(nodetocheck.getNodeID().compareTo(nodedata.getNodeID()) == 0)
				return true;
		}
		
		return false;
	}
	
	public boolean requestToReadFileFromAnyActiveNode(String filename) throws RemoteException, NotBoundException {
		
		Set<Message> aktivenoder = requestActiveNodesForFile(filename);
		Message meldingfranode = new ArrayList<>(aktivenoder).get(0);
		Message melding = meldingfranode;
		
		ChordNodeInterface node = (ChordNodeInterface) Util.locateRegistry(meldingfranode.getNodeIP()).lookup(meldingfranode.getNodeID().toString());
		

		melding.setOptype(OperationType.READ);
		
		node.setActiveNodesForFile(aktivenoder);
		
		melding.setNodeIP(node.getNodeIP());
		boolean resultat = node.requestReadOperation(melding);
		melding.setAcknowledged(resultat);
		
		if (melding.isAcknowledged()) {
			node.multicastVotersDecision(melding);
			node.acquireLock();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Operations operasjon = new Operations(node, melding, aktivenoder);
			operasjon.performOperation();

			operasjon.multicastReadReleaseLocks();
			node.releaseLocks();
		}

		return melding.isAcknowledged();
	}
	
	public boolean requestWriteToFileFromAnyActiveNode(String filename, String newcontent) throws RemoteException, NotBoundException {
				
		Set<Message> aktivenoder = requestActiveNodesForFile(filename);
		Message medlingfranode = new ArrayList<>(aktivenoder).get(0);
		Message melding = medlingfranode;
		
		ChordNodeInterface node = (ChordNodeInterface) Util.locateRegistry(medlingfranode.getNodeIP()).lookup(medlingfranode.getNodeID().toString());

		melding.setOptype(OperationType.WRITE);
		melding.setNewcontent(newcontent);

		node.setActiveNodesForFile(aktivenoder);
		melding.setNodeIP(node.getNodeIP());
		boolean resultat = node.requestWriteOperation(melding);
		melding.setAcknowledged(resultat);
		
		if (melding.isAcknowledged()) {
			node.multicastVotersDecision(melding);
			node.acquireLock();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Operations operasjon = new Operations(node, melding, aktivenoder);
			operasjon.performOperation();
			operasjon.multicastReadReleaseLocks();	
			node.releaseLocks();
		}
		return melding.isAcknowledged(); 

	}

	/**
	 * create the localfile with the node's name and id as content of the file
	 * @param nodename
	 * @throws RemoteException 
	 */
	public void createLocalFile() throws RemoteException {
		String nodename = chordnode.getNodeIP();
		String path = new File(".").getAbsolutePath().replace(".", "");
		File fpath = new File(path+"/"+nodename);				// we'll have ../../nodename/
		if(!fpath.exists()) {
			boolean suc = fpath.mkdir();
			try {
				if(suc) {
					File file = new File(fpath+"/"+nodename); 	// end up with:  ../../nodename/nodename  (actual file no ext)
					file.createNewFile();	
					// write the node's data into this file
					writetofile(file);
				}
			} catch (IOException e) {
				
				//e.printStackTrace();
			}
		}
		
	}
	
	private void writetofile(File file) throws RemoteException {
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			bw.write(chordnode.getNodeIP());
			bw.newLine();
			bw.write(chordnode.getNodeID().toString());
			bw.close();
									
		} catch (IOException e) {
			
			//e.printStackTrace();
		}
	}
}