package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.SKAdmin;
import com.ms.silverking.cloud.gridconfig.GridConfiguration;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.SystemTimeSource;

/**
 * Responsible for diagnosing and fixing nodes that are down.
 */
public class Doctor {
	private final SKGridConfiguration	gc;
	private final ConcurrentMap<String, Patient>			patients;
	private final boolean	forceInclusionOfUnsafeExcludedServers;
	private final int	nodeStartupTimeoutSeconds;
	
	private static final int	reachabilityTimeoutMillis = 20 * 1000;
	
	static {
		SKAdmin.exitOnCompletion = false;
	}
	
	public Doctor(GridConfiguration gc, boolean forceInclusionOfUnsafeExcludedServers, int nodeStartupTimeoutSeconds) {
		this.gc = new SKGridConfiguration(gc);
		this.forceInclusionOfUnsafeExcludedServers = forceInclusionOfUnsafeExcludedServers;
		this.nodeStartupTimeoutSeconds = nodeStartupTimeoutSeconds;
		patients = new ConcurrentHashMap<>();
	}
	
	public void admitPatients(Set<IPAndPort> patients) {
		for (IPAndPort patient : patients) {
			admitPatient(patient.getIPAsString());
		}
	}
	
	public void admitPatient(String server) {
		patients.putIfAbsent(server, new Patient(server));
	}
	
	public void releasePatients(Set<IPAndPort> patients) {
		for (IPAndPort patient : patients) {
			releasePatient(patient.getIPAsString());
		}
	}
	
	public void releasePatient(String server) {
		patients.remove(server);
	}
	
	public boolean canConnectToDaemon(String node) {
		return canConnectToNodeAndPort(node, gc.getClientDHTConfiguration().getPort());
	}
	
	private boolean canConnectToNodeAndPort(String node, int port) {
		boolean	ok;
		Socket	s;
		
		ok = false;
		s = null;
		try {
			s = new Socket(node, port);
			ok = true;
		} catch (ConnectException ioe) {
			ok = false;
		} catch (IOException ioe) {
			Log.logErrorWarning(ioe);
			ok = false;
		} finally {
			if (s != null) {
				try {
					s.close();
				} catch (IOException ioe) {
					Log.logErrorWarning(ioe);
					ok = false;
				}
			}
		}
		return ok;
	}
	
	public boolean isReachable(String node) {
		try {
			return InetAddress.getByName(node).isReachable(reachabilityTimeoutMillis);
		} catch (IOException e) {
			Log.logErrorWarning(e);
			return false;
		}
	}
	
	public void startNodes(Set<String> nodes) {
		String	args;
		
		args = "-g "+ gc.getName() +" -c StartNodes -to "+ nodeStartupTimeoutSeconds +" -r "
		+ (forceInclusionOfUnsafeExcludedServers ? "-forceUnsafe " :"") +"-e -t "+ CollectionUtil.toString(nodes, ','); 
		Log.warningf("args: {%s} ", args);
		try {
			SKAdmin.main(args.split("\\s+"));
		} catch (IneligibleServerException ise) {
			Log.warningf("Ignoring IneligibleServerException %s", ise.getMessage());
		}
	}
	
	public void makeRounds() {
		Set<String>	patientsToRestart;
		long		absTimeMillis;
		
		absTimeMillis = SystemTimeSource.instance.absTimeMillis();
		patientsToRestart = new HashSet<>();
		for (Patient patient : patients.values()) {
			Diagnosis	diagnosis;
			
			diagnosis = checkOnPatient(patient);
			Log.warningf("%s\t%s", patient, diagnosis);
			if (diagnosis.isRestartable()) {
				if (patient.restartPending()) {
					if (patient.restartTimedOut(absTimeMillis)) {
						patient.handleTimeout();
					}
				}
				if (!patient.restartPending()) {
					patient.markRestarted(absTimeMillis);
					patientsToRestart.add(patient.getName());
				}
			}
		}
		if (patientsToRestart.size() > 0) {
			Log.warning("Attempting to start restartable servers: "+ CollectionUtil.toString(patientsToRestart, ','));
			startNodes(patientsToRestart);
		}
	}
	
	private Diagnosis checkOnPatient(Patient patient) {
		Set<Disease>	diseases;
		
		diseases = new HashSet<>();
		if (!isReachable(patient.getName())) {
			diseases.add(Disease.Unreachable);
		} else {
			if (!canConnectToDaemon(patient.getName())) {
				diseases.add(Disease.NoNodeDaemon);
			}
		}
		return new Diagnosis(diseases);
	}

	// For unit testing only
	public static void main(String[] args) {
		try {
			Doctor	doctor;
			
			doctor = new Doctor(GridConfiguration.parseFile(args[0]), false, Integer.parseInt(args[1]));
			for (int i = 1; i < args.length; i++) {
				String	arg;
				
				arg = args[i];
				System.out.printf("%s\t%s\t%s\n", arg, doctor.isReachable(arg), doctor.canConnectToDaemon(arg));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
