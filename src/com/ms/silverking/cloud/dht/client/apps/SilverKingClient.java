package com.ms.silverking.cloud.dht.client.apps;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import jline.console.ConsoleReader;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.crypto.AESEncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.crypto.XOREncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.impl.MetaDataTextUtil;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.os.OSUtil;
import com.ms.silverking.text.ObjectDefParser2;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.thread.lwt.LWTThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class SilverKingClient {
    private final BufferedReader    in;
    private final PrintStream       out;
    private final PrintStream       err;
    private final DHTClient         dhtClient;
    private final DHTSession        session;
    private SynchronousNamespacePerspective<String,byte[]>  syncNSP;
    private PutOptions              putOptions;
    private Stopwatch   sw;
    private int reps;
    private boolean verbose;
    private ValueMapDisplay valueMapDisplay;
    private Map<String,ValueMapDisplay> valueMapDisplays;
    private ValueFormat valueFormat;
    private Map<String,ValueFormat>     valueFormats;
    private EncrypterDecrypter	encrypterDecrypter;
	
	private static final int   clientWorkUnit = 10;
	
    private static final String    multiLinePrompt = "> ";
    private static final String    prompt = "skc> ";
    private static final String    terminator = ";";
    private static final String    noSuchValue = "<No such value>";
    private static final String    exists = "<Exists>";
    private static final String    defaultValueMapDisplay = "basic";
    private static final String    defaultValueFormat = "raw";
    
	public SilverKingClient(ClientDHTConfigurationProvider dhtConfigProvider, String preferredServer,
	        InputStream in, PrintStream out, PrintStream err) throws IOException, ClientException {
        this.in = new BufferedReader(new InputStreamReader(in));
        this.out = out;
        this.err = err;
		dhtClient = new DHTClient();        
        session = dhtClient.openSession(new SessionOptions(dhtConfigProvider, preferredServer));
        if (session == null) {
            throw new RuntimeException("null session");
        }
		reps = 1;
		sw = new SimpleStopwatch();
		
		valueMapDisplays = new HashMap<>();
		addValueMapDisplay(new BasicValueMapDisplay());
        addValueMapDisplay(new TabDelimitedValueMapDisplay());
        addValueMapDisplay(new CSVValueMapDisplay());
        valueMapDisplay = valueMapDisplays.get(defaultValueMapDisplay);
        
        valueFormats = new HashMap<>();
        addValueFormat(new RawValueFormat());
        addValueFormat(new MetaDataValueFormat());
        addValueFormat(new HexValueFormat());
        addValueFormat(new LengthValueFormat());
        addValueFormat(new ChecksumValueFormat());
        addValueFormat(new ChecksumAndLengthValueFormat());
        valueFormat = valueFormats.get(defaultValueFormat);
	}
	
	private void addValueMapDisplay(ValueMapDisplay vmd) {
	    valueMapDisplays.put(vmd.getName(), vmd);
	}
	
    private void addValueFormat(ValueFormat valueFormat) {
        valueFormats.put(valueFormat.getName(), valueFormat);
    }
    
    private void doRetrieve(String[] args, RetrievalType retrievalType, WaitMode waitMode) throws OperationException, IOException {
        Map<String, ? extends StoredValue<byte[]>>  storedValues;
        Set<String> keys;
        
        keys = retrievalKeySet(args);
        storedValues = retrieve(args, retrievalType, waitMode);
        if (retrievalType.hasValue()) {
            displayValueMap(keys, storedValues);
        }
        if (retrievalType == RetrievalType.META_DATA) {
            displayMetaDataMap(keys, storedValues);
        }
    }
    
    private void doRetrieve(String[] args, RetrievalType retrievalType) throws OperationException, IOException {
        Map<String, ? extends StoredValue<byte[]>>  storedValues;
        Set<String> keys;
        RetrievalOptions    retrievalOptions;
        
        if (args[0].startsWith("{")) {
            if (!args[0].endsWith("}")) {
                err.printf("retrievalOptions missing closing }\n");
                return;
            } else {
                String[]    newArgs;
                
                newArgs = new String[args.length - 1];
                System.arraycopy(args, 1, newArgs, 0, newArgs.length);
                retrievalOptions = ((RetrievalOptions)ObjectDefParser2.parse(RetrievalOptions.class, 
                                args[0].substring(1, args[0].length() - 1)));//.retrievalType(retrievalType);
                retrievalType = retrievalOptions.getRetrievalType();
                if (verbose) {
                    out.printf("retrievalOptions: %s\n", retrievalOptions);
                }
                args = newArgs;
            }
        } else {
            retrievalOptions = syncNSP.getNamespace().getOptions().getDefaultGetOptions();
        }
        
        keys = retrievalKeySet(args);
        storedValues = retrieve(args, retrievalOptions);
        if (retrievalType.hasValue()) {
            displayValueMap(keys, storedValues);
        }
        if (retrievalType == RetrievalType.META_DATA) {
            displayMetaDataMap(keys, storedValues);
        } else if (retrievalType == RetrievalType.EXISTENCE) {
            displayExistence(keys, storedValues);
        }
    }
    
    private List<String> sortedList(Set<String> s) {
    	List<String>	sorted;
    	
    	sorted = new ArrayList<>(s);
    	Collections.sort(sorted);
    	return sorted;
    }
    
    private void displayMetaDataMap(Set<String> keys, Map<String, ? extends StoredValue<byte[]>> storedValues) {
        for (String key : sortedList(keys)) {
            StoredValue<byte[]>    storedValue;

            storedValue = storedValues.get(key);
            out.printf("\n%s\n%s\n", key, storedValue != null ? MetaDataTextUtil.toMetaDataString(storedValue.getMetaData(), true) : noSuchValue);
        }
    }
    
    private void displayExistence(Set<String> keys, Map<String, ? extends StoredValue<byte[]>> storedValues) {
        for (String key : sortedList(keys)) {
            StoredValue<byte[]>    storedValue;

            storedValue = storedValues.get(key); 
            out.printf("\n%s\t%s\n", key, storedValue != null ? exists : noSuchValue);
        }
    }
    
	private void displayValueMap(Set<String> keys, Map<String, ? extends StoredValue<byte[]>> storedValues) {
	    valueMapDisplay.displayValueMap(keys, storedValues);
    }
	
	private Set<String> retrievalKeySet(String[] args) {
        ImmutableSet.Builder<String>    builder;
        
        builder = ImmutableSet.builder();
        for (int i = 0; i < args.length; i ++) {
            builder.add(translateKey(args[i]));
        }
        return builder.build();
	}

    private Map<String, ? extends StoredValue<byte[]>> retrieve(String[] args, RetrievalType retrievalType, 
                                        WaitMode waitMode) throws OperationException, IOException {
        RetrievalOptions retrievalOptions;
        
        if (waitMode == WaitMode.WAIT_FOR) {
            retrievalOptions = syncNSP.getNamespace().getOptions().getDefaultWaitOptions().retrievalType(retrievalType);
        } else {
            retrievalOptions = syncNSP.getNamespace().getOptions().getDefaultGetOptions().retrievalType(retrievalType);
        }

        return retrieve(args, retrievalOptions);
    }

    private Map<String, ? extends StoredValue<byte[]>> retrieve(String[] args, RetrievalOptions retrievalOptions) 
                                                                throws OperationException, IOException {
        try {
            Map<String, ? extends StoredValue<byte[]>>    storedValues;
            Set<String> keys;
            
            if (syncNSP == null) {
                out.printf("No namespace set\n");
                return ImmutableMap.of();
            }

            keys = retrievalKeySet(args);
            
            opMessage("Retrieving");
            storedValues = null;
            sw.reset();
            for (int i = 0; i < reps; i++) {
                // System.out.printf("Calling retrieve %d\n", i);
                storedValues = syncNSP.retrieve(keys, retrievalOptions);
                // System.out.printf("Done retrieve %d\n", i);
            }
            sw.stop();
            return storedValues;
        } catch (RetrievalException re) {
            displayRetrievalExceptionDetails(re);
            throw re;
        }
    }
    
    private void doRetrieveAllValuesForKey(String[] args) throws OperationException, IOException {
        sw.reset();
    	for (String key : args) {
	        Map<String, ? extends StoredValue<byte[]>>  storedValues;
	        
	        storedValues = retrieveAllValuesForKey(key);
            displayMetaDataMap(storedValues.keySet(), storedValues);
    	}
        sw.stop();    	
    }
    
    private Map<String, ? extends StoredValue<byte[]>> retrieveAllValuesForKey(String key) throws RetrievalException {
    	RetrievalOptions retrievalOptions;
    	Map<String, StoredValue<byte[]>>	keyValues;
    	
    	keyValues = new HashMap<>();
    	retrievalOptions = syncNSP.getOptions().getDefaultGetOptions().retrievalType(RetrievalType.META_DATA)
    			.nonExistenceResponse(NonExistenceResponse.NULL_VALUE).versionConstraint(VersionConstraint.greatest)
    			.returnInvalidations(true);
        try {
        	do {
            	StoredValue<byte[]>	storedValue;
            	
            	//System.out.printf("%s\t%s\n", key, retrievalOptions);
            	
				storedValue = syncNSP.retrieve(key, retrievalOptions);
				if (storedValue != null) {
		        	String	keyAndVersion;
		        	
		        	keyAndVersion = String.format("%s %d %d %s", key, storedValue.getVersion(), storedValue.getCreationTime().inNanos(), storedValue.getCreationTime().toDateString());
		        	keyValues.put(keyAndVersion, storedValue);
		        	//System.out.printf("%d\t%d\n", storedValue.getMetaData().getVersion(), storedValue.getMetaData().getCreationTime().inNanos());
		        	//retrievalOptions = retrievalOptions.versionConstraint(retrievalOptions.getVersionConstraint().maxCreationTime(storedValue.getCreationTime().inNanos() - 1));
		        	retrievalOptions = retrievalOptions.versionConstraint(retrievalOptions.getVersionConstraint().maxBelowOrEqual(storedValue.getVersion() - 1));
		        	//ThreadUtil.sleep(1000);
				} else {
					break;
				}
        	} while (true);
		} catch (RetrievalException re) {
            displayRetrievalExceptionDetails(re);
            throw re;
		}
        return keyValues;
    }
    
    private void doPutRandom(String[] args) throws OperationException, IOException {
        Map<String,byte[]>  map;
        ImmutableMap.Builder<String,byte[]>    builder;
        
        builder = ImmutableMap.builder();
        for (int i = 0; i < args.length; i += 2) {
            builder.put(translateKey(args[i]), translateRandomValue(args[i + 1]));
        }
        map = builder.build();
        opMessage("Putting");
        sw.reset();
        try {
            for (int i = 0; i < reps; i++) {
                syncNSP.put(map);
            }
        } catch (PutException pe) {
            out.println(pe.getDetailedFailureMessage());
            throw pe;
        }
        sw.stop();
    }
    
    private void doPut(String[] args) throws OperationException, IOException {
        Map<String,byte[]>  map;
        ImmutableMap.Builder<String,byte[]>    builder;
        PutOptions	putOptions;
        
        if (args[0].startsWith("{")) {
            if (!args[0].endsWith("}")) {
                err.printf("putOptions missing closing }\n");
                return;
            } else {
                String[]    newArgs;
                
                newArgs = new String[args.length - 1];
                System.arraycopy(args, 1, newArgs, 0, newArgs.length);
                putOptions = ((PutOptions)ObjectDefParser2.parse(PutOptions.class, 
                                args[0].substring(1, args[0].length() - 1)));
                if (verbose) {
                    out.printf("putOptions: %s\n", putOptions);
                }
                args = newArgs;
            }
        } else {
        	putOptions = syncNSP.getNamespace().getOptions().getDefaultPutOptions();
        }
        
        builder = ImmutableMap.builder();
        for (int i = 0; i < args.length; i += 2) {
            builder.put(translateKey(args[i]), translateValue(args[i + 1]));
        }
        map = builder.build();
        opMessage("Putting");
        sw.reset();
        try {
            for (int i = 0; i < reps; i++) {
                syncNSP.put(map, putOptions);
            }
        } catch (PutException pe) {
            out.println(pe.getDetailedFailureMessage());
            throw pe;
        }
        sw.stop();
    }
    
    private void doInvalidation(String[] args) throws OperationException, IOException {
        Set<String>  set;
        ImmutableSet.Builder<String>    builder;
        
        builder = ImmutableSet.builder();
        for (int i = 0; i < args.length; i ++) {
            builder.add(translateKey(args[i]));
        }
        set = builder.build();
        opMessage("Invalidating");
        sw.reset();
        try {
            for (int i = 0; i < reps; i++) {
                syncNSP.invalidate(set);
            }
        } catch (PutException pe) {
            out.println(pe.getDetailedFailureMessage());
            throw pe;
        }
        sw.stop();
    }
    
    private String translateKey(String key) {
        return key;
    }

    private byte[] translateValue(String value) {
        return value.getBytes();
    }
    
    private byte[] translateRandomValue(String value) {
        int     multiplier;
        byte[]  b;
        
        value = value.trim();
        if (value.endsWith("B")) {
            value = value.substring(0, value.length() - 1);
        }
        if (value.endsWith("M")) {
            multiplier = 1024 * 1024;
            value = value.substring(0, value.length() - 1);
        } else if (value.endsWith("K")) {
            multiplier = 1024;
            value = value.substring(0, value.length() - 1);
        } else {
            multiplier = 1;
        }
        b = new byte[multiplier * Integer.parseInt(value)];
        ThreadLocalRandom.current().nextBytes(b);
        return b;
    }
    
    private void doSnapshot(String[] args) throws OperationException, IOException {
        long            version;
        
        if (args.length > 0) {
            version = Long.parseLong(args[0]);
        } else {
            version = DHTUtil.currentTimeMillis();
        }
        opMessage("Creating snapshot");
        sw.reset();
    	// snapshots deprecated for now
        /*
        try {
            for (int i = 0; i < reps; i++) {
                //syncNSP.snapshot(version);
            }
        } catch (SnapshotException pe) {
            out.println(pe.getDetailedFailureMessage());
            throw pe;
        }
        */
        sw.stop();
    }
    
    private void doLinkToNamespace(String[] args) throws OperationException, IOException {
        String              parent;
        NamespaceOptions    nsOptions;
        
        nsOptions = syncNSP.getNamespace().getOptions();
        switch (nsOptions.getVersionMode()) {
        case SINGLE_VERSION:
            break;
        default:
            System.out.println("linkToNamespace is only supported for NamespaceVersionMode.SINGLE_VERSION");
            return;
        }
        
        parent = args[0];
        opMessage("Linking namespace");
        sw.reset();
        syncNSP.getNamespace().linkTo(parent);
        sw.stop();
    }
	
    private void doCreateNamespace(String[] args) throws OperationException, IOException {
        String              name;
        NamespaceOptions    nsOptions;
        
        name = args[0];
        if (args.length > 1) {
            nsOptions = NamespaceOptions.parse(args[1]);
        } else {
            nsOptions = null;
        }
        opMessage("Creating namespace");
        if (verbose) {
        	System.out.printf("NamespaceOptions: %s\n", nsOptions);
        }
        sw.reset();
        session.createNamespace(name, nsOptions);
        sw.stop();
    }
    
    private void doCloneNamespace(String[] args) throws OperationException, IOException {
        String              name;
        NamespaceOptions    nsOptions;
        long                version;
        
        nsOptions = syncNSP.getNamespace().getOptions();
        switch (nsOptions.getVersionMode()) {
        case SEQUENTIAL:
            System.out.println("clone currently not supported for NamespaceVersionMode.SEQUENTIAL");
            return;
        case CLIENT_SPECIFIED:
            if (args.length > 1) {
                version = Long.parseLong(args[1]);
            } else {
                version = System.currentTimeMillis();
                out.printf("No version specified. Using System.currentTimeMillis() %d %s\n", version, new Date(version).toString());
            }
            break;
        default:
            version = Long.MIN_VALUE;
            break;
        }
        
        name = args[0];
        out.println(version == Long.MIN_VALUE ? "No version passed" : "Passing version: "+ version);
        opMessage("Cloning namespace");
        sw.reset();
        if (version == Long.MIN_VALUE) {
            syncNSP.getNamespace().clone(name);
        } else {
            syncNSP.getNamespace().clone(name, version);
        }
        sw.stop();
    }
    
    private void doGetNamespaceOptions(String[] args) throws OperationException, IOException {
        String              name;
        NamespaceOptions    nsOptions;

        opMessage("Getting namespace options");
        name = args[0];
        nsOptions = null;
        sw.reset();
        for (int i = 0; i < reps; i++) {
            try {
                nsOptions = session.getNamespace(name).getOptions();
            } catch (NamespaceNotCreatedException nnce) {
                err.printf("No such namespace: %s\n", name);
            }
        }
        sw.stop();
        if (nsOptions != null) {
            out.println(nsOptions);
        }
    }

    private void setValueMapDisplay(String[] args) {
        if (args.length == 0) {
            out.printf("ValueMapDisplay is %s\n", valueMapDisplay.getName());
        } else {
            if (args.length != 1) {
                err.println("ValueMapDisplay <vmdName>");
            } else {
                String  vmdName;
                ValueMapDisplay vmd;
                
                vmdName = args[0];
                vmd = valueMapDisplays.get(vmdName);
                if (vmd == null) {
                    err.printf("Unknown ValueMapDisplay %s\n", vmdName);
                } else {
                    valueMapDisplay = vmd;
                    if (verbose) {
                        out.printf("ValueMapDisplay now %s\n", valueMapDisplay.getName());
                    }
                }
            }
        }
    }
    
    private void setValueFormat(String[] args) {
        if (args.length == 0) {
            out.printf("ValueFormat is %s\n", valueFormat.getName());
        } else {
            if (args.length != 1) {
                err.println("ValueFormat <valueFormatName>");
            } else {
                String  valueFormatName;
                ValueFormat _valueFormat;
                
                valueFormatName = args[0];
                _valueFormat = valueFormats.get(valueFormatName);
                if (_valueFormat == null) {
                    err.printf("Unknown ValueFormat %s\n", valueFormatName);
                } else {
                    valueFormat = _valueFormat;
                    if (verbose) {
                        out.printf("ValueFormat now %s\n", valueFormat.getName());
                    }
                }
            }
        }
    }
    
    private void doSetEncryption(String[] args) throws IOException {
        if (args.length == 0) {
            out.printf("Encryption is %s\n", encrypterDecrypter.getName());
        } else {
            if (args.length != 2) {
                err.println("SetEncryption <encrypterDecrypterName> <keyFileName>");
            } else {
                String  encrypterDecrypterName;
                EncrypterDecrypter	_encrypterDecrypter;
                File				keyFile;
                
                encrypterDecrypterName = args[0];
                keyFile = new File(args[1]);
                if (!keyFile.exists()) {
                	err.printf("Key file doesn't exist: %s\n", keyFile.getName());
                }
                if (encrypterDecrypterName.equalsIgnoreCase(AESEncrypterDecrypter.name)) {
                	_encrypterDecrypter = new AESEncrypterDecrypter(keyFile);
                } else if (encrypterDecrypterName.equalsIgnoreCase(XOREncrypterDecrypter.name)) {
                	_encrypterDecrypter = new XOREncrypterDecrypter(keyFile);
                } else {
                	_encrypterDecrypter = null;
                }
                if (_encrypterDecrypter == null) {
                    err.printf("Unknown EncrypterDecrypter %s\n", encrypterDecrypterName);
                } else {
                	encrypterDecrypter = _encrypterDecrypter;;
                    if (verbose) {
                        out.printf("EncrypterDecrypter now %s\n", encrypterDecrypter.getName());
                    }
                    syncNSP.setOptions(syncNSP.getOptions().encrypterDecrypter(encrypterDecrypter));
                }
            }
        }
	}
    
    private void toggleVerbose() {
        verbose = !verbose;
        out.printf("Verbose is now %s\n", verbose);
    }
    
    private void displayHelpMessage() {
        out.println(Action.helpMessage());
    }
    
    ////////////////////////////////////////////////////////
    
    private void displayRetrievalExceptionDetails(RetrievalException re) {
        err.println("Failed keys: ");
        for (Object key : re.getFailedKeys()) {
            out.printf("%s\t%s\n", key, re.getFailureCause(key));
        }
    }
    
    private void opMessage(String m) {
        if (verbose) {
            Log.warning(m);
        }
    }
    
    private void shellLoop() throws Exception {
        ConsoleReader   reader;
        boolean running;
        String  cmd;
     
    	reader = new ConsoleReader();
        running = true;
        cmd = "";
        while (running) {
            String  s;
            
            if (OSUtil.isWindows()) {
                out.print(prompt);
                s = in.readLine();
            } else {
                s = reader.readLine(cmd.length() == 0 ? prompt : multiLinePrompt);
            }
            cmd += " "+ s;
            if (s.endsWith(terminator)) {
                cmd = cmd.trim();
                cmd = cmd.substring(0, cmd.length() - 1);
                //System.out.println("command: "+ cmd);
                try {
                    doCommand(cmd);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                cmd = "";
            }
        }
    }
    
    private void runCommands(String commandsDef) throws Exception {
        List<String>    commands;
        
        commands = ImmutableList.copyOf(StringUtil.stripQuotes(commandsDef).split(terminator));
        runCommands(commands);
    }
    
    private void runCommands(File commandFile) throws Exception {
        runCommands(FileUtil.readFileAsString(commandFile));
    }
    
    private void runCommands(Iterable<String> commands) throws Exception {
        for (String command : commands) {
            command = command.trim();
            if (command.length() > 0) {
                out.println(prompt+ command);
                doCommand(command);
            }
        }
    }
    
	private void doCommand(String cmd) throws OperationException, IOException {
	    String[]   tokens;
	    
	    tokens = cmd.split("\\s+");
	    doCommand(tokens);
    }

    private void doCommand(String[] tokens) throws OperationException, IOException {
        Action      action;
        String[]    args;
        
        try {
            action = Action.parse(tokens[0]);
        } catch (IllegalArgumentException iae) {
            out.println("Unknown action");
            return;
        }
        args = cdr(tokens);
        doCommand(action, args);
    }
    
    private String[] cdr(String[] a) {
        String[]    b;
        
        b = new String[a.length - 1];
        for (int i = 1; i < a.length; i++) {
            b[i - 1] = a[i];
        }
        return b;
    }
    
    private void doCommand(Action action, String[] args) throws IOException, OperationException {
        boolean actionPerformed;
        
        sw.reset();
        actionPerformed = true;
        switch (action) {
        case Put: doPut(args); break;
        case Invalidation: doInvalidation(args); break;
        case PutRandom: doPutRandom(args); break;
        case WaitFor: doRetrieve(args, valueFormat.getRetrievalType(), WaitMode.WAIT_FOR); break;
        case Get: doRetrieve(args, valueFormat.getRetrievalType(), WaitMode.GET); break;
        case Retrieve: doRetrieve(args, valueFormat.getRetrievalType()); break;
        case GetMeta: doRetrieve(args, RetrievalType.META_DATA, WaitMode.GET); break;
        case GetAllValuesForKey: doRetrieveAllValuesForKey(args); break;
        case CreateNamespace: 
            doCreateNamespace(args);
            doSetNamespace(args); break;
        case CloneNamespace: 
            doCloneNamespace(args);
            doSetNamespace(args); break;
        case LinkToNamespace: 
            doLinkToNamespace(args); break;
        case GetNamespaceOptions: doGetNamespaceOptions(args); break;
        case SetNamespace: doSetNamespace(args); break;
        case Snapshot: doSnapshot(args); break;
        case ToggleVerbose: toggleVerbose(); actionPerformed = false; break;
        case ValueMapDisplay: setValueMapDisplay(args); actionPerformed = false; break;
        case ValueFormat: setValueFormat(args); actionPerformed = false; break;
        case Reps: doSetReps(args); break;
        case Help: displayHelpMessage(); break;
        case Quit: System.exit(0); break;
        case SetEncryption: doSetEncryption(args); actionPerformed = false; break;
        default: 
            out.println("Action not supported: "+ action); 
            actionPerformed = false;
        }
        // Commands that care about precise timing will also reset the sw internally
        // We reset/stop it in this routine to handle commands for which it isn't critical 
        if (sw.isRunning()) {
            sw.stop();
        }
        if (verbose && actionPerformed) {
            out.printf("Elapsed:  %f\n", sw.getElapsedSeconds());
            if (action.usesReps()) {
                double  timePerRep;
                
                timePerRep = sw.getElapsedSeconds() / (double)reps;
                out.printf("Time/rep: %f\tReps: %d\n", timePerRep, reps);
            }
        }
    }
    
	private static String[] wrapOneArg(String arg) {
        String[]    s;
        
        s = new String[1];
        s[0] = arg;
        return s;
    }

    private void doSetNamespace(String[] args) {
        SynchronousNamespacePerspective<String,byte[]>  _syncNSP;
        String  namespace;
        
        namespace = args[0];
        out.printf("Setting namespace to \"%s\"\n", namespace);
        _syncNSP = syncNSP;
        try {
        	Namespace	ns;
            NamespacePerspectiveOptions<String,byte[]> nspOptions;
            
            ns = session.getNamespace(namespace);
            if (args.length > 1) {
                nspOptions = ns.getDefaultNSPOptions(String.class, byte[].class);
                nspOptions = nspOptions.parse(args[1]);
            } else {
                nspOptions = ns.getDefaultNSPOptions(String.class, byte[].class);
            }
            syncNSP = null;
            syncNSP = ns.openSyncPerspective(nspOptions);
        } catch (NamespaceNotCreatedException nnce) {
            err.printf("No such namespace: %s\n", namespace);
        }
        if (syncNSP == null && _syncNSP != null) {
            syncNSP = _syncNSP;
            err.printf("Setting namespace back to: %s\n", syncNSP.getName());
        }
    }
    
    private void doSetReps(String[] args) {
        if (args.length == 0) {
            out.printf("Reps = %d\n", reps);
        } else {
            if (args.length > 1) {
                err.println("Reps <reps>");
            } else {
                reps = Integer.parseInt(args[0]);
                out.printf("Reps = %d\n", reps);
            }
        }
    }
    
    /**
	 * @param args
	 */
	public static void main(String[] args) {
    	try {
    		SilverKingClient		skc;
    		SilverKingClientOptions	options;
    		CmdLineParser	        parser;
    		ClientDHTConfigurationProvider	configProvider;
    		
    		args = repairArgs(args);
    		
            LWTPoolProvider.createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters().workUnit(clientWorkUnit));
            LWTThreadUtil.setLWTThread();
    		options = new SilverKingClientOptions();
    		parser = new CmdLineParser(options);
    		try {
    			parser.parseArgument(args);
    		} catch (CmdLineException cle) {
    			System.err.println(cle.getMessage());
    			parser.printUsage(System.err);
    			System.exit(-1);
    		}
    		if (options.gridConfig == null && options.clientDHTConfiguration == null && !SessionOptions.isReservedServerName(options.server)) {
    			System.err.println("Neither gridConfig nor clientDHTConfiguration provided, but server name is not reserved");
    			parser.printUsage(System.err);
    			System.exit(-1);
    		}
    		Log.setLevel(options.logLevel);
    		Log.fine(options);
    		//if (options.verbose) {
    		//    Log.setLevelAll();
    		//}
    		
    		if (options.gridConfig != null) {
        		if (options.gridConfigBase != null) {
        			configProvider = SKGridConfiguration.parseFile(new File(options.gridConfigBase), options.gridConfig);
        		} else {
        			configProvider = SKGridConfiguration.parseFile(options.gridConfig);
        		}
    		} else if (options.clientDHTConfiguration != null) {
    			configProvider = ClientDHTConfiguration.parse(options.clientDHTConfiguration);
    		} else {
    			configProvider = null;
    		}
    		skc = new SilverKingClient(configProvider, options.server, System.in, System.out, System.err);
            //System.out.println(options.namespace +":"+ options.key);
    		if (options.namespace != null) {
    		    skc.doSetNamespace(wrapOneArg(options.namespace));
    		}
    		if (options.commands == null && options.commandFile == null) {
    		    skc.shellLoop();
    		} else {
                if (options.commandFile != null) {
                    skc.runCommands(options.commandFile);
                }
                if (options.commands != null) {
                    skc.runCommands(options.commands);
                }
    		}
    		System.exit(0);
    	} catch (Exception e) {
    		e.printStackTrace();
			System.exit(-1);
    	}
    }
	
	private static String[] repairArgs(String[] args) {
		List<String>	newArgs;
		String			commandArg;
		boolean			inCommand;
		
		newArgs = new ArrayList<>();
		inCommand = false;
		commandArg = null;
		for (String arg : args) {
			arg = arg.trim();
			if (!inCommand) {
				newArgs.add(arg);
				if (arg.equals("-c")) {
					inCommand = true;
					commandArg = "";
				}
			} else {
				if (arg.startsWith("-") && arg.length() > 1 && !Character.isDigit(arg.charAt(1))) {
					newArgs.add(commandArg);
					newArgs.add(arg);
					inCommand = false;
				} else {
					commandArg += " "+ arg;
				}
			}
		}
		if (inCommand) {
			newArgs.add(commandArg.trim());
		}
		return newArgs.toArray(new String[0]);
	}

	private interface ValueMapDisplay {
	    public String getName();
	    public void displayValueMap(Set<String> keys, Map<String, ? extends StoredValue<byte[]>> storedValues);
	}
	
	private class BasicValueMapDisplay implements ValueMapDisplay {
	    @Override
	    public String getName() {
	        return "basic";
	    }
	    
	    @Override
	    public void displayValueMap(Set<String> keys, Map<String, ? extends StoredValue<byte[]>> storedValues) {
            for (String key : sortedList(keys)) {
	            StoredValue<byte[]>    storedValue;
	            
                storedValue = storedValues.get(key);
                out.printf("%10s => %s\n", key, storedValue != null ? valueFormat.valueToString(storedValue) : noSuchValue);
	        }
	    }
	}
	
    private class TabDelimitedValueMapDisplay implements ValueMapDisplay {
        @Override
        public String getName() {
            return "tabDelimited";
        }
        
        @Override
        public void displayValueMap(Set<String> keys, Map<String, ? extends StoredValue<byte[]>> storedValues) {
            for (String key : sortedList(keys)) {
                StoredValue<byte[]>    storedValue;
                
                storedValue = storedValues.get(key);
                out.printf("%10s\t%s\n", key, storedValue != null ? valueFormat.valueToString(storedValue) : noSuchValue);
            }
        }
    }

    private class CSVValueMapDisplay implements ValueMapDisplay {
        @Override
        public String getName() {
            return "csv";
        }
        
        @Override
        public void displayValueMap(Set<String> keys, Map<String, ? extends StoredValue<byte[]>> storedValues) {
            for (String key : sortedList(keys)) {
                StoredValue<byte[]>    storedValue;
                
                storedValue = storedValues.get(key);
                out.printf("\"%s\", \"%s\"\n", key, storedValue != null ? valueFormat.valueToString(storedValue) : noSuchValue);
            }
        }
    }    
    
    private interface ValueFormat {
        public String getName();
        public String valueToString(StoredValue<byte[]> value); 
        public RetrievalType getRetrievalType();
    }
    
    private class RawValueFormat implements ValueFormat {
        @Override
        public String getName() {
            return "raw";
        }
        
        @Override
        public RetrievalType getRetrievalType() {
            return RetrievalType.VALUE;
        }
        
        @Override
        public String valueToString(StoredValue<byte[]> value) {
            return new String(value.getValue());
        }
    }
    
    private class MetaDataValueFormat implements ValueFormat {
        @Override
        public String getName() {
            return "metaData";
        }
        
        @Override
        public RetrievalType getRetrievalType() {
            return RetrievalType.META_DATA;
        }
        
        @Override
        public String valueToString(StoredValue<byte[]> value) {
            return new String(value.getMetaData().toString(true));
        }
    }
    
    private class HexValueFormat implements ValueFormat {
        @Override
        public String getName() {
            return "hex";
        }
        
        @Override
        public RetrievalType getRetrievalType() {
            return RetrievalType.VALUE;
        }
        
        @Override
        public String valueToString(StoredValue<byte[]> value) {
            return StringUtil.byteArrayToHexString(value.getValue());
        }
    }
    
    private class LengthValueFormat implements ValueFormat {
        @Override
        public String getName() {
            return "length";
        }
        
        @Override
        public RetrievalType getRetrievalType() {
            return RetrievalType.VALUE;
        }
        
        @Override
        public String valueToString(StoredValue<byte[]> value) {
            return "length="+ value.getValue().length;
        }
    }
    
    private class ChecksumValueFormat implements ValueFormat {
        @Override
        public String getName() {
            return "checksum";
        }
        
        @Override
        public RetrievalType getRetrievalType() {
            return RetrievalType.VALUE_AND_META_DATA;
        }
        
        @Override
        public String valueToString(StoredValue<byte[]> value) {
            return value.getChecksumType() +":"+ StringUtil.byteArrayToHexString(value.getChecksum());
        }
    }
    
    private class ChecksumAndLengthValueFormat implements ValueFormat {
        @Override
        public String getName() {
            return "checksumAndLength";
        }
        
        @Override
        public RetrievalType getRetrievalType() {
            return RetrievalType.VALUE_AND_META_DATA;
        }
        
        @Override
        public String valueToString(StoredValue<byte[]> value) {
            return value.getChecksumType() +"="+ StringUtil.byteArrayToHexString(value.getChecksum()) +":length="+ value.getValue().length;
        }
    }
}
