package no.hvl.dat110.node.client.test;

import java.math.BigInteger;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.List;

import no.hvl.dat110.file.FileManager;
import no.hvl.dat110.node.Message;
import no.hvl.dat110.rpc.StaticTracker;
import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

public class NodeClientReader extends Thread {

	private boolean succeed = false;
	
	private String filename;
	
	public NodeClientReader(String filename) {
		this.filename = filename;
	}
	
	public void run() {
		sendRequest();
	}
	
	private void sendRequest() {
		
		String aktivNode = StaticTracker.ACTIVENODES[0];
		BigInteger ID = Hash.hashOf(aktivNode);
		
		try {
			ChordNodeInterface CNIN = (ChordNodeInterface) Util.locateRegistry(aktivNode).lookup(ID.toString());
			FileManager fm = new FileManager(CNIN, StaticTracker.N);
			succeed = fm.requestToReadFileFromAnyActiveNode(filename);
		}catch(RemoteException e) {
			succeed = false;
		}catch(NotBoundException e) {
			succeed = false;
		}
	
	}
	
	public boolean isSucceed() {
		return succeed;
	}

}
