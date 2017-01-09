\l XXXDHTLIBPATHXXX/qdht.q

/ use following for local test
/ \l qdht.q

\e 1

jvmopts: "-Xmx1G,-Xms32M,-Xcheck:jni,-XX:ParallelGCThreads=2";
//jvmopts: "";
hclient: ms.sk.dht.client.getclient[LVLOFF;jvmopts];

gcname: "GC_SK_";
host: "localhost";
hsession: ms.sk.dht.client.getsession[hclient;gcname;host];
show "====== got hsession =====";

dhtnamespace: 7?"abcdefg";
show "dhtnamespace: ", dhtnamespace;
nsoptions: "storageType=RAM,consistencyProtocol=TWO_PHASE_COMMIT,versionMode=CLIENT_SPECIFIED";
//nsoptions: "";

hnsopt: ms.sk.dht.nsoptions.parse[nsoptions];
//hnamespace: ms.sk.dht.session.createnamespace[hsession;dhtnamespace;hnsopt];  
hnamespace: ms.sk.dht.session.createnamespace3[hsession;dhtnamespace;nsoptions];
show "====== got namespace handle ======";
show hnamespace;

intervalSecs: ms.sk.dht.nsoptions.getsecondarysyncintervalseconds[hnsopt];
show `intervalSecs, intervalSecs;

show "====== create putoptions ======";
// obsolete: hputOpts: ms.sk.dht.putopts.standardoptions[0];
// ms.sk.dht.putopts.compression[hputOpts;DHTLZ4];
opTimeoutControllerq: ms.sk.dht.simpletimeoutcontroller.new[6;2147483646];
secondaryTargetsq: ();
hputOpts: ms.sk.dht.putopts.new[opTimeoutControllerq; DHTLZ4; DHTMURMUR32; 0b; 0; secondaryTargetsq; ""];
show "hputOpts ok"

relativeAttTimeoutMil: ms.sk.dht.simpletimeoutcontroller.getrelativetimeoutmillisforattempt[opTimeoutControllerq];
show `relativeAttTimeoutMil , relativeAttTimeoutMil;
ms.sk.dht.simpletimeoutcontroller.delete[opTimeoutControllerq];

scdaryTgts: ms.sk.dht.putopts.getsecondarytargets[hputOpts];
show `scdaryTgts, scdaryTgts;
usrData: ms.sk.dht.putopts.getuserdata[hputOpts];
show `usrData usrData ;

scdaryTgt1: ms.sk.dht.secondarytarget.new[ DHTNODEID; "dd975c1n11"];
tgtName: ms.sk.dht.secondarytarget.gettarget[scdaryTgt1];
show tgtName;
ms.sk.dht.putopts.secondarytarget[hputOpts, scdaryTgt1];
ms.sk.dht.secondarytarget.delete[scdaryTgt1];

scdaryTgts: ms.sk.dht.putopts.getsecondarytargets[hputOpts];
show `scdaryTgts, scdaryTgts;



show "====== create SyncNameSpacePerspective ======";
///hnspopt: ms.sk.dht.nsperspectiveopt.new[0];
hnspopt: ms.sk.dht.namespace.getdefaultnspoptions[hnamespace];
ms.sk.dht.nsperspectiveopt.defaultputoptions[hnspopt;hputOpts];

show "======  set wait options for waitfor / mwaitfor operations ==="; 
hgetopt: ms.sk.dht.getopts.new[VALUEANDMETADATA];
hwaitopt: ms.sk.dht.waitopts.new[0];
ms.sk.dht.waitopts.timeoutseconds[hwaitopt;10];
ms.sk.dht.waitopts.threshold[hwaitopt;100];
ms.sk.dht.waitopts.timeoutresponse[hwaitopt;DHTTIMEOUTIGNORE];
ms.sk.dht.nsperspectiveopt.defaultwaitoptions[hnspopt;hwaitopt];
fwdMode: ms.sk.dht.waitopts.getforwardingmode[hwaitopt];
show `fwdMode, fwdMode;

show "====== open SyncNameSpacePerspective with these options ======";
hsnsp: ms.sk.dht.session.getsyncnsp[hsession;dhtnamespace;hnspopt];
show "====== got sync nsp ======";
show hsnsp;
show .z.z;


//// single val put - syncnsp.put
show "====== test put key 1 into dht ======";
ms.sk.dht.syncnsp.put [hsnsp;"qhello0";"qhello0123"];
show "====== test put key 2 into dht ======";
ms.sk.dht.syncnsp.put [hsnsp;"qhello1";"qhello0123"];
show "====== test put key 3 into dht ======";
ms.sk.dht.syncnsp.put [hsnsp;"qhello000";"qhello000123"];

//// multi put - dhtmput
show "====== test multi put keys into dht ======";
dhtkeys: ("multik0";"multik1";"multik2";"multik3");
dhtvals: ("multival0";"multival1";"multival2";"multival3");
ms.sk.dht.syncnsp.mput [hsnsp;dhtkeys;dhtvals];
show "test multi put completed - success ";

show "====== test 2 multi put keys into dht ======";
dhtkeys: ("multik00";"multik01";"multik02";"multik03");
dhtvals: ("multival00";"multival01";"multival02";"multival03");
ms.sk.dht.syncnsp.mput [hsnsp;dhtkeys;dhtvals];
show "test multi put 2 completed - success ";
//show .z.z;

//// get - syncnsp.get
show "====== test get ======";
dhtvall: ms.sk.dht.syncnsp.get [hsnsp;"qhello0"];
show dhtvall;
show "====== test get ======";
dhtvall: ms.sk.dht.syncnsp.get [hsnsp;"qhello1"];
show dhtvall;
show "====== test waitfor ======";
dhtvall: ms.sk.dht.syncnsp.waitfor [hsnsp;"qhello000"];
show dhtvall;
show "test get succeeded";


//// multiget - syncnsp.mget
//show " ";
//show .z.z;
show "====== test multi get ======";
dhtkeyzz: ("multik0";"multik1";"multik2";"multik3");
dhtvalzz: ms.sk.dht.syncnsp.mget [hsnsp;dhtkeyzz];
show dhtvalzz;
dhtkeyzz: ("multik00";"multik01";"multik02";"multik03");
dhtvalzz: ms.sk.dht.syncnsp.mget [hsnsp;dhtkeyzz];
show dhtvalzz;
show "test multi get succeeded";

show "====== test multi_put_dict into dht ======";
dhtkeysmpd: ("mdict0";"mdict1";"mdict2";"mdict3");
dhtvalsmpd: ("mdictval0";"mdictval1";"mdictval2";"mdictval3");
dhtdict : dhtkeysmpd!dhtvalsmpd;
show dhtdict;
ms.sk.dht.syncnsp.mputdict [hsnsp;dhtdict];
show "test multi put dict completed - success ";
dhtvalsd: ms.sk.dht.syncnsp.mget [hsnsp;dhtkeysmpd];
show dhtvalsd;
show "test multi get completed - success ";

//// wait - dhtwait
show "====== test wait on success ======";
dhtvalw: ms.sk.dht.syncnsp.waitfor [hsnsp;"multik0"];
show dhtvalw;
show "test dhtwait succeeded";

show "====== test mwait on success ======";
dhtkeys: ("multik0";"multik1";"multik2";"multik3");
dhtvalsmw: ms.sk.dht.syncnsp.mwait [hsnsp;dhtkeys];
show dhtvalsmw;
show "test dhtmwaitfor succeeded";


show "====== test put key into dht ======";
ms.sk.dht.syncnsp.put   [hsnsp;"testZZZZ1";"0100000030000000630b00020000006368756e6b4e756d006e756d4368756e6b730006000200000001000000e2050000"];
ms.sk.dht.syncnsp.put   [hsnsp;"testZZZZ0";"0100000030000000630b00020000006368756e6b4e756d006e756d4368756e6b730006000200000000000000e2050000"];
ms.sk.dht.syncnsp.put   [hsnsp;"testZZZZ2";"0100000030000000630b00020000006368756e6b4e756d006e756d4368756e6b730006000200000002000000e2050000"];
ms.sk.dht.syncnsp.put   [hsnsp;"testZZZZ3";"0100000030000000630b00020000006368756e6b4e756d006e756d4368756e6b730006000200000003000000e2050000"];
ms.sk.dht.syncnsp.put   [hsnsp;"testZZZZ4";"0100000030000000630b00020000006368756e6b4e756d006e756d4368756e6b730006000200000004000000e2050000"];
ms.sk.dht.syncnsp.put   [hsnsp;"testZZZZ5";"0100000030000000630b00020000006368756e6b4e756d006e756d4368756e6b730006000200000005000000e2050000"];
show "====== test get key from dht ======";
dhter1: ms.sk.dht.syncnsp.get[hsnsp;"testZZZZ1"];
show "====== test get 6 keyS from dht ======";
dhterrkeys: ("testZZZZ0";"testZZZZ1";"testZZZZ2";"testZZZZ3";"testZZZZ4";"testZZZZ5");
dhterrv: ms.sk.dht.syncnsp.mget[hsnsp;dhterrkeys];
show dhterrv;

hwaitopts: ms.sk.dht.nsperspectiveopt.getdefaultwaitoptions[hnspopt];
tr: ms.sk.dht.waitopts.gettimeoutresponse[hwaitopts];
show "defaultwaitoptions timeoutresponse ";
show tr;

show "====== test dhtmwaitfor 10 secs non-existing values ====";
dhtkeys: ("multik0";"multik1";"multik2";"multik3";"multik4";"multik5");
dhtvalsne: ms.sk.dht.syncnsp.mwait [hsnsp;dhtkeys];
show "test dhtmwaitfor timed out";
show dhtvalsne;

show "===== test dhtmwaitfor 3 secs non-existing values ====";
rt: ms.sk.dht.waitopts.getretrievaltype[hwaitopts];
show "defaultwaitoptions retrievaltype ";
show rt;
ms.sk.dht.waitopts.timeoutseconds[hwaitopts;3];
dhtkeys2: ("multik0";"multik1";"multik2";"multik3";"multik4";"multik5");
dhtvalsne: ms.sk.dht.syncnsp.mwaitwo [hsnsp;dhtkeys2;hwaitopts];
show "test dhtmwaitfor timed out";
show dhtvalsne;

show "====== test mgetmeta 3 values ====";
dhtkeys3: ("multik0";"multik1";"multik2");
dhtvals3: ms.sk.dht.syncnsp.mgetmeta [hsnsp;dhtkeys3;hgetopt];
show dhtvals3;
show dhtvals3["multik0"];


show "====== open AsyncNameSpacePerspective ======";
hansp: ms.sk.dht.session.getasyncnsp[hsession;dhtnamespace;hnspopt];
show "====== got async nsp ======";
show hansp;

show "====== async mget without options ======";
dhtkeys2: ("multik0";"multik1";"multik2";"multik3";"multik4";"multik5");
hAsyncValueRetrieval: ms.sk.dht.asyncnsp.mget [hansp;dhtkeys2];
ms.sk.dht.avalret.waitforcompletion[hAsyncValueRetrieval];
avrNumKeys: ms.sk.dht.avalret.getnumkeys[hAsyncValueRetrieval];
show `asyncValueRetNumKeys, avrNumKeys;
valsavr1: ms.sk.dht.avalret.getvalues[hAsyncValueRetrieval];
show valsavr1;
ms.sk.dht.avalret.close[hAsyncValueRetrieval];
ms.sk.dht.avalret.delete[hAsyncValueRetrieval];

show "====== async mwait without options ======";
dhtkeys2: ("multik0";"multik1";"multik2";"multik3");
hAsyncValueRetrieval2: ms.sk.dht.asyncnsp.mwait [hansp;dhtkeys2];
ms.sk.dht.avalret.waitforcompletion[hAsyncValueRetrieval2];
valsavr2: ms.sk.dht.avalret.getvalues[hAsyncValueRetrieval2];
show valsavr2;
ms.sk.dht.avalret.close[hAsyncValueRetrieval2];
ms.sk.dht.avalret.delete[hAsyncValueRetrieval2];

show "====== async mwait with options ======";
dhtkeys3: ("multik0";"multik1";"multik2";"multik3";"multik4";"multik5");
hAsyncRetrieval: ms.sk.dht.asyncnsp.mwaitwo [hansp;dhtkeys3;hwaitopts];
ms.sk.dht.aretrieval.waitforcompletion[hAsyncRetrieval];
valsar2: ms.sk.dht.aretrieval.getstoredvalues[hAsyncRetrieval];
show valsar2;
ms.sk.dht.aretrieval.close[hAsyncRetrieval];
ms.sk.dht.aretrieval.delete[hAsyncRetrieval];

///////////////////////////// dhtshutdown ////////////////////////////////////////////////////
//show .z.z;
ms.sk.dht.nsperspectiveopt.delete[hnspopt];
ms.sk.dht.getopts.delete[hgetopt];
ms.sk.dht.waitopts.delete[hwaitopts];
ms.sk.dht.putopts.delete[hputOpts];
ms.sk.dht.asyncnsp.delete [hansp];
ms.sk.dht.syncnsp.delete [hsnsp];
ms.sk.dht.namespace.delete[hnamespace];
//ms.sk.dht.nsoptions.delete[hnsopt];
ms.sk.dht.session.delete[hsession];

show "test dhtshutdown";
ms.sk.dht.shutdown [0];
\\
