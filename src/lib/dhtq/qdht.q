libhandle: `:XXX_DHT_LIB_PATH_XXX/qsilverking;

/ use as follow for test
/ qdhtlibdir: "<path-to-silverking-release-area>/<architecture-folder-name>/lib/kdb";
/ libhandle: hsym `$ qdhtlibdir,"/qdht";

show "useing client lib"
\c 20 150
show libhandle

// --- DHT Retrieval Type ---
VALUE: 0i;
METADATA: 1i;
VALUEANDMETADATA: 2i;
EXISTENCE: 3i;
// --- DHT Wait Mode --- 
DHTWMGET: 0i;
DHTWMWAITFOR: 0i;
// --- DHT Compression Type --- 
DHTNOCOMPRESSION: 0i;
DHTZIP: 1i;
DHTBZIP2: 2i;
DHTSNAPPY: 3i;
DHTLZ4: 4i;
// --- DHT Checksum Type --- 
DHTNONE: 0i;
DHTMD5: 1i;
DHTSHA1: 2i;
DHTMURMUR32: 3i;
DHTMURMUR128: 4i;
// --- DHT KeyDigestType --- 
DIGESTNONE: 0i;
DIGESTMD5: 1i;
DIGESTSHA1: 2i;
// --- DHT Operation State --- 
OPSTINCOMPLETE: 0i;
OPSTSUCCEEDED: 1i;
OPSTFAILED: 2i;
// --- DHT Time Units --- 
NANOSECONDS:0i;
MICROSECONDS: 1i;
MILLISECONDS: 2i;
SECONDS: 3i;
MINUTES: 4i;
HOURS: 5i;
DAYS: 6i;
// --- DHT Failure Cause  --- 
FCERROR:0i;
FCTIMEOUT:1i;
FCMUTATION:2i;
FCMULTIPLE: 3i;
FCINVALIDVERSION: 4i;
FCSIMULTANEOUSPUT: 5i;
FCNOSUCHVALUE: 6i;
FCNOSUCHNAMESPACE: 7i;
FCCORRUPT: 8i;
// --- DHT Storage Type --- 
DHTRAMSTORE: 0i;
DHTFILESTORE: 1i;
// --- DHT Logging level --- 
LVLALL: 0i;
LVLLOG: 1i;
LVLFINE: 2i;
LVLINFO: 3i;
LVLWARNING: 4i;
LVLERROR: 5i;
LVLOFF: 6i;
// --- DHT Consistency Mode --- 
DHTLOOSE: 0i;
DHTTWOPHASECOMMIT: 1i;
// --- DHT Timeout Response --- 
DHTTIMEOUTEXCEPTION: 0i;
DHTTIMEOUTIGNORE: 1i;
// --- DHT NonExistence Response --- 
DHTNONEXISTNULLVALUE: 0i;
DHTNONEXISTEXCEPTION: 1i;
// --- DHT Version Mode --- 
DHTVMSINGLEVERSION: 0i;
DHTVMCLIENTSPECIFIED: 1i;
DHTVMSEQUENTIAL: 2i;
DHTVMSTIMEMILLIS: 3i;
DHTVMSTIMENANOS: 4i;
// --- DHT Version Constraint Mode: LEAST, GREATEST --- 
DHTVCLEAST: 0i;
DHTVCGREATEST: 1i;
// --- NsCreationMode ---
REQUIREEXPLICITCREATION: 0i;
REQUIREAUTOCREATION: 1i;
OPTIONALAUTOCREATIONALLOWMATCHES: 2i;
OPTIONALAUTOCREATIONDISALLOWMATCHES: 3i;
// --- SecondaryTargetType ---
DHTNODEID: 0i;
DHTANCESTORCLASS: 1i;
// --- ForwardingMode ---
DHTDONOTFORWARD: 0i;
DHTFORWARD: 1i;
// --- SKRevisionMode ---
/ No revisions allowed 
DHTNOREVISIONS: 0i;            
/ Unrestricted revisions allowed 
DHTUNRESTRICTEDREVISIONS: 1i;


// The part below can be generated using the following script and pasting results into this file below:
// please run from the same directory where qdht.c is:
//grep "^K dht\_" qdht.c | cut -b 3- | sed 's/(/ ( /1' |gawk '{z=(NF-3)/2; y=$1; x=gsub(/_/,".",y);  print "ms.sk." y ":    libhandle 2: (`" $1 ";" z ");\n/ " $0 "\n" }' >> qdht.q.tmp

ms.sk.dht.client.getclient:    libhandle 2: (`dht_client_getclient;2);
/ dht_client_getclient ( K loglevelq, K jvmoptionsq )

ms.sk.dht.shutdown:    libhandle 2: (`dht_shutdown;1);
/ dht_shutdown (  K dummyq )

ms.sk.dht.client.getsession:    libhandle 2: (`dht_client_getsession;3);
/ dht_client_getsession ( K clienthq, K gcname, K hostq )

ms.sk.dht.client.setlogfile:    libhandle 2: (`dht_client_setlogfile;1);
/ dht_client_setlogfile ( K logfilenameq)

ms.sk.dht.client.setloglevel:    libhandle 2: (`dht_client_setloglevel;1);
/ dht_client_setloglevel ( K loglevelq )

////ms.sk.dht.session.createnamespace:    libhandle 2: (`dht_session_createnamespace;3);
/ dht_session_createnamespace (  KJ sessionhq, KC namespaceq, KJ nsoptionsq )

ms.sk.dht.session.createnamespace3:    libhandle 2: (`dht_session_createnamespace3;3);
/ dht_session_createnamespace3 (  KJ sessionhq, KC namespaceq, KC nsoptionsStrq )

ms.sk.dht.session.createns:    libhandle 2: (`dht_session_createns;2);
/ dht_session_createns (  K sessionhq, K namespaceq )

ms.sk.dht.session.getnamespace:    libhandle 2: (`dht_session_getnamespace;2);
/ dht_session_getnamespace (  K sessionhq, K namespaceq )

ms.sk.dht.session.getsyncnsp:    libhandle 2: (`dht_session_getsyncnsp;3);
/ dht_session_getsyncnsp (  K sessionhq, K namespaceq, K nsperspectiveoptionsq )

ms.sk.dht.session.getasyncnsp:    libhandle 2: (`dht_session_getasyncnsp;3);
/ dht_session_getasyncnsp (  K sessionhq, K namespaceq, K nsperspectiveoptionsq  )

ms.sk.dht.session.getnscreationoptions:        libhandle 2: (`dht_session_getnscreationoptions;1);
/ dht_session_getnscreationoptions( K sessionhq )

ms.sk.dht.session.getdefaultnamespaceoptions:  libhandle 2: (`dht_session_getdefaultnamespaceoptions;1);
/ dht_session_getdefaultnamespaceoptions( K sessionhq )

ms.sk.dht.session.getdefaultputoptions:  libhandle 2: (`dht_session_getdefaultputoptions;1);
/ dht_session_getdefaultputoptions( K sessionhq )

ms.sk.dht.session.getdefaultgetoptions:  libhandle 2: (`dht_session_getdefaultgetoptions;1);
/ dht_session_getdefaultgetoptions( K sessionhq )

ms.sk.dht.session.getdefaultwaitoptions:  libhandle 2: (`dht_session_getdefaultwaitoptions;1);
/ dht_session_getdefaultwaitoptions( K sessionhq )

ms.sk.dht.session.deletens:    libhandle 2: (`dht_session_deletens;2);
/ dht_session_deletens (  KJ sessionhq, KS namespaceq )

ms.sk.dht.session.recoverns:    libhandle 2: (`dht_session_recoverns;2);
/ dht_session_recoverns (  KJ sessionhq, KS namespaceq )

ms.sk.dht.session.close:    libhandle 2: (`dht_session_close;1);
/ dht_session_close (  K sessionhq )

ms.sk.dht.session.delete:    libhandle 2: (`dht_session_delete;1);
/ dht_session_delete (  K sessionhq )

ms.sk.dht.namespace.getdefaultnspoptions:    libhandle 2: (`dht_namespace_getdefaultnspoptions;1);
/ dht_namespace_getdefaultnspoptions (  K namespacehq )

ms.sk.dht.namespace.getoptions:    libhandle 2: (`dht_namespace_getoptions;1);
/ dht_namespace_getoptions (  K namespacehq )

ms.sk.dht.namespace.openasyncnsp:    libhandle 2: (`dht_namespace_openasyncnsp;2);
/ dht_namespace_openasyncnsp (  K namespacehq, K nsperspectiveoptionhq )

ms.sk.dht.namespace.opensyncnsp:    libhandle 2: (`dht_namespace_opensyncnsp;2);
/ dht_namespace_opensyncnsp (  K namespacehq, K nsperspectiveoptionhq )

ms.sk.dht.namespace.getname:      libhandle 2: (`dht_namespace_getname;1);
/ dht_namespace_getname (  K namespacehq )

ms.sk.dht.namespace.clone:        libhandle 2: (`dht_namespace_clone;2);
/ dht_namespace_clone (  K namespacehq, K childnameq )

ms.sk.dht.namespace.clonev:       libhandle 2: (`dht_namespace_clonev;3);
/ dht_namespace_clonev (  K namespacehq, K childnameq, K versionq )

ms.sk.dht.namespace.linkto:       libhandle 2: (`dht_namespace_linkto;2);
/ dht_namespace_linkto (  K namespacehq, K targetq )

ms.sk.dht.namespace.delete:    libhandle 2: (`dht_namespace_delete;1);
/ dht_namespace_delete (  K namespacehq )

ms.sk.dht.nsoptions.parse:             libhandle 2: (`dht_nsoptions_parse;1);
/ dht_nsoptions_parse (  K nsoptq )

ms.sk.dht.nsoptions.new:               libhandle 2: (`dht_nsoptions_new;6);
/ dht_nsoptions_new( K storageTypeq, K consistencyProtocolq, K versionModeq, K putOptionsq, K getOptionsq, K waitOptionsq) 

ms.sk.dht.nsoptions.new7:              libhandle 2: (`dht_nsoptions_new7;7);
/ dht_nsoptions_new7( K storageTypeq, K consistencyProtocolq, K versionModeq, K revisionModeq, K opsOptionsDictq, 
/                     K secondarySyncIntervalSeconds, K segmentSizeq)
/ where opsOptionsDictq: ("putOptions","getOptions","waitOptions")!(putOptsHandleq,getOptsHandleq,waitOptsHandleq)

ms.sk.dht.nsoptions.new8:              libhandle 2: (`dht_nsoptions_new8;8);
/ dht_nsoptions_new8( K storageTypeq, K consistencyProtocolq, K versionModeq, K revisionModeq, K opsOptionsDictq, 
/                     K secondarySyncIntervalSeconds, K segmentSizeq, K allowLinksq) 
/ where opsOptionsDictq: ("putOptions","getOptions","waitOptions")!(putOptsHandleq,getOptsHandleq,waitOptsHandleq)

ms.sk.dht.nsoptions.getconsistencyprotocol:    libhandle 2: (`dht_nsoptions_getconsistencyprotocol;1);
/ dht_nsoptions_getconsistencyprotocol (  K nsoptionshq )

ms.sk.dht.nsoptions.getstoragetype:    libhandle 2: (`dht_nsoptions_getstoragetype;1);
/ dht_nsoptions_getstoragetype (  K nsoptionshq )

ms.sk.dht.nsoptions.getversionmode:    libhandle 2: (`dht_nsoptions_getversionmode;1);
/ dht_nsoptions_getversionmode (  K nsoptionshq )

ms.sk.dht.nsoptions.getrevisionmode:   libhandle 2: (`dht_nsoptions_getrevisionmode;1);
/ dht_nsoptions_getrevisionmode (  K nsoptionshq )

ms.sk.dht.nsoptions.getsegmentsize:    libhandle 2: (`dht_nsoptions_getsegmentsize;1);
/ dht_nsoptions_getsegmentsize (  K nsoptionshq )  //->int

ms.sk.dht.nsoptions.getallowlinks:     libhandle 2: (`dht_nsoptions_getallowlinks;1);
/ dht_nsoptions_getallowlinks (  K nsoptionshq )  //->bool

ms.sk.dht.nsoptions.getputopts:        libhandle 2: (`dht_nsoptions_getputopts;1);
/ dht_nsoptions_getputopts (  K nsoptionshq )

ms.sk.dht.nsoptions.getgetopts:        libhandle 2: (`dht_nsoptions_getgetopts;1);
/ dht_nsoptions_getgetopts (  K nsoptionshq )

ms.sk.dht.nsoptions.getwaitopts:       libhandle 2: (`dht_nsoptions_getwaitopts;1);
/ dht_nsoptions_getwaitopts (  K nsoptionshq )

ms.sk.dht.nsoptions.tostring:          libhandle 2: (`dht_nsoptions_tostring;1);
/ dht_nsoptions_tostring (  K nsoptionshq )

ms.sk.dht.nsoptions.storagetype:       libhandle 2: (`dht_nsoptions_storagetype;2);
/ dht_nsoptions_storagetype (  K nsoptq, K storageTypeq )

ms.sk.dht.nsoptions.consistencyprotocol:    libhandle 2: (`dht_nsoptions_consistencyprotocol;2);
/ dht_nsoptions_consistencyprotocol( K nsoptq, K consistencyProtocolq )

ms.sk.dht.nsoptions.versionmode:       libhandle 2: (`dht_nsoptions_versionmode;2);
/ dht_nsoptions_versionmode( K nsoptq, K versionModeq )

ms.sk.dht.nsoptions.revisionmode:      libhandle 2: (`dht_nsoptions_revisionmode;2);
/ dht_nsoptions_revisionmode( K nsoptq, K revisionModeq )

ms.sk.dht.nsoptions.segmentsize:       libhandle 2: (`dht_nsoptions_segmentsize;2);
/ dht_nsoptions_segmentsize( K nsoptq, K segmentSizeq )

ms.sk.dht.nsoptions.allowlinks:        libhandle 2: (`dht_nsoptions_allowlinks;2);
/ dht_nsoptions_allowlinks( K nsoptq, K allowLinksq )

ms.sk.dht.nsoptions.defaultputoptions: libhandle 2: (`dht_nsoptions_defaultputoptions;2);
/ dht_nsoptions_defaultputoptions( K nsOptq , K defaultPutOpsq)

ms.sk.dht.nsoptions.getsecondarysyncintervalseconds: libhandle 2: (`dht_nsoptions_getsecondarysyncintervalseconds;1);
/ dht_nsoptions_getsecondarysyncintervalseconds( KJ nsOptq )

ms.sk.dht.nsoptions.secondarysyncintervalseconds: libhandle 2: (`dht_nsoptions_secondarysyncintervalseconds;2);
/ dht_nsoptions_secondarysyncintervalseconds( KJ nsOptq , KI secondsq)

ms.sk.dht.nsoptions.delete:    libhandle 2: (`dht_nsoptions_delete;1);
/ dht_nsoptions_delete (  K nsoptionshq )

ms.sk.dht.storedvalue.getstoredlength:    libhandle 2: (`dht_storedvalue_getstoredlength;1);
/ dht_storedvalue_getstoredlength (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getuncompressedlength:    libhandle 2: (`dht_storedvalue_getuncompressedlength;1);
/ dht_storedvalue_getuncompressedlength (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getcompression:    libhandle 2: (`dht_storedvalue_getcompression;1);
/ dht_storedvalue_getcompression (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getchecksumtype:    libhandle 2: (`dht_storedvalue_getchecksumtype;1);
/ dht_storedvalue_getchecksumtype (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getversion:    libhandle 2: (`dht_storedvalue_getversion;1);
/ dht_storedvalue_getversion (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getcreationtime:    libhandle 2: (`dht_storedvalue_getcreationtime;1);
/ dht_storedvalue_getcreationtime (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getvalue:    libhandle 2: (`dht_storedvalue_getvalue;1);
/ dht_storedvalue_getvalue (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getuserdata:    libhandle 2: (`dht_storedvalue_getuserdata;1);
/ dht_storedvalue_getuserdata (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getchecksum:    libhandle 2: (`dht_storedvalue_getchecksum;1);
/ dht_storedvalue_getchecksum (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getcreatorid:    libhandle 2: (`dht_storedvalue_getcreatorid;1);
/ dht_storedvalue_getcreatorid (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.getcreatorip:    libhandle 2: (`dht_storedvalue_getcreatorip;1);
/ dht_storedvalue_getcreatorip (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.todict:    libhandle 2: (`dht_storedvalue_todict;1);
/ dht_storedvalue_todict (  K storedvalHandleq ) 

ms.sk.dht.storedvalue.delete:    libhandle 2: (`dht_storedvalue_delete;1);
/ dht_storedvalue_delete (  K storedvalHandleq )

ms.sk.dht.syncnsp.put:    libhandle 2: (`dht_syncnsp_put;3);
/ dht_syncnsp_put (  K snspHandleq, K keyq, K valueq ) 

ms.sk.dht.syncnsp.putwo:    libhandle 2: (`dht_syncnsp_putwo;4);
/ dht_syncnsp_putwo (  K snspHandleq, K keyq, K valueq, K putopthq ) 

ms.sk.dht.syncnsp.mputwo:    libhandle 2: (`dht_syncnsp_mputwo;4);
/ dht_syncnsp_mputwo (  K snspHandleq, K keylistq, K valuelistq, K putopthq ) 

ms.sk.dht.syncnsp.mput:    libhandle 2: (`dht_syncnsp_mput;3);
/ dht_syncnsp_mput (  K snspHandleq, K keylistq, K valuelistq ) 

ms.sk.dht.syncnsp.mputdictwo:    libhandle 2: (`dht_syncnsp_mputdictwo;3);
/ dht_syncnsp_mputdictwo (  K snspHandleq, K dictq, K putopthq ) 

ms.sk.dht.syncnsp.mputdict:    libhandle 2: (`dht_syncnsp_mputdict;2);
/ dht_syncnsp_mputdict (  K snspHandleq, K dictq ) 

ms.sk.dht.syncnsp.get:    libhandle 2: (`dht_syncnsp_get;2);
/ dht_syncnsp_get (  K snspHandleq, K keyq )

ms.sk.dht.syncnsp.waitfor:    libhandle 2: (`dht_syncnsp_waitfor;2);
/ dht_syncnsp_waitfor (  K snspHandleq, K keyq )

ms.sk.dht.syncnsp.getwo:    libhandle 2: (`dht_syncnsp_getwo;3);
/ dht_syncnsp_getwo (  K snspHandleq, K keyq, K getopsHandleq )

ms.sk.dht.syncnsp.waitforwo:    libhandle 2: (`dht_syncnsp_waitforwo;3);
/ dht_syncnsp_waitforwo (  K snspHandleq, K keyq, K waitopsHandleq )

ms.sk.dht.syncnsp.getasdictwo:    libhandle 2: (`dht_syncnsp_getasdictwo;3);
/ dht_syncnsp_getasdictwo (  K snspHandleq, K keyq, K getopsHandleq )

ms.sk.dht.syncnsp.getmeta:    libhandle 2: (`dht_syncnsp_getmeta;3);
/ dht_syncnsp_getmeta (  K snspHandleq, K keyq, K getopsHandleq ) 

ms.sk.dht.syncnsp.waitforasdictwo:    libhandle 2: (`dht_syncnsp_waitforasdictwo;3);
/ dht_syncnsp_waitforasdictwo (  K snspHandleq, K keyq, K waitopsHandleq )

ms.sk.dht.syncnsp.mget:    libhandle 2: (`dht_syncnsp_mget;2);
/ dht_syncnsp_mget (  K snspHandleq, K keylistq )

ms.sk.dht.syncnsp.mwait:    libhandle 2: (`dht_syncnsp_mwait;2);
/ dht_syncnsp_mwait (  K snspHandleq, K keylistq )

ms.sk.dht.syncnsp.mgetwo:    libhandle 2: (`dht_syncnsp_mgetwo;3);
/ dht_syncnsp_mgetwo (  K snspHandleq, K keylistq, K getoptsHandleq )

ms.sk.dht.syncnsp.mwaitwo:    libhandle 2: (`dht_syncnsp_mwaitwo;3);
/ dht_syncnsp_mwaitwo (  K snspHandleq, K keylistq, K waitoptsHandleq )

ms.sk.dht.syncnsp.mgetmeta:    libhandle 2: (`dht_syncnsp_mgetmeta;3);
/ dht_syncnsp_mgetmeta (  K snspHandleq, K keylistq, K getopsHandleq ) 

ms.sk.dht.syncnsp.snapshot:    libhandle 2: (`dht_syncnsp_snapshot;1);
/ dht_syncnsp_snapshot (  K snspHandleq )

ms.sk.dht.syncnsp.snapshotver:    libhandle 2: (`dht_syncnsp_snapshotver;2);
/ dht_syncnsp_snapshotver (  K snspHandleq, K versionq )

ms.sk.dht.syncnsp.syncrequest:    libhandle 2: (`dht_syncnsp_syncrequest;1);
/ dht_syncnsp_syncrequest (  K snspHandleq )

ms.sk.dht.syncnsp.syncrequestver:    libhandle 2: (`dht_syncnsp_syncrequestver;2);
/ dht_syncnsp_syncrequestver (  K snspHandleq, K versionq )

ms.sk.dht.syncnsp.getoptions:    libhandle 2: (`dht_syncnsp_getoptions;1);
/ dht_syncnsp_getoptions (  K snspHandleq )

ms.sk.dht.syncnsp.getnamespace:    libhandle 2: (`dht_syncnsp_getnamespace;1);
/ dht_syncnsp_getnamespace (  K snspHandleq )

ms.sk.dht.syncnsp.getname:    libhandle 2: (`dht_syncnsp_getname;1);
/ dht_syncnsp_getname (  K snspHandleq )

ms.sk.dht.syncnsp.setoptions:                libhandle 2: (`dht_syncnsp_setoptions;2);
/ dht_syncnsp_setoptions( K syncnspq , K nsPerspOptq)

ms.sk.dht.syncnsp.setdefaultversion:         libhandle 2: (`dht_syncnsp_setdefaultversion;2);
/ dht_syncnsp_setdefaultversion( K syncnspq , K versionq)

ms.sk.dht.syncnsp.setretrievalverconstraint: libhandle 2: (`dht_syncnsp_setretrievalverconstraint;2);
/ dht_syncnsp_setretrievalverconstraint( K syncnspq , K versionConstraintq)

ms.sk.dht.syncnsp.setversionprovider:        libhandle 2: (`dht_syncnsp_setversionprovider;2);
/ dht_syncnsp_setversionprovider( K syncnspq , K versionProviderq)

ms.sk.dht.syncnsp.close:                     libhandle 2: (`dht_syncnsp_close;1);
/ dht_syncnsp_close( K snspHandleq )

ms.sk.dht.syncnsp.delete:    libhandle 2: (`dht_syncnsp_delete;1);
/ dht_syncnsp_delete (  K snspHandleq )

ms.sk.dht.asyncnsp.put:    libhandle 2: (`dht_asyncnsp_put;3);
/ dht_asyncnsp_put (  K anspHandleq, K keyq, K valueq ) 

ms.sk.dht.asyncnsp.putwo:    libhandle 2: (`dht_asyncnsp_putwo;4);
/ dht_asyncnsp_putwo (  K anspHandleq, K keyq, K valueq, K putopthq ) 

ms.sk.dht.asyncnsp.mputwo:    libhandle 2: (`dht_asyncnsp_mputwo;4);
/ dht_asyncnsp_mputwo (  K anspHandleq, K keylistq, K valuelistq, K putopthq ) 

ms.sk.dht.asyncnsp.mput:    libhandle 2: (`dht_asyncnsp_mput;3);
/ dht_asyncnsp_mput (  K anspHandleq, K keylistq, K valuelistq ) 

ms.sk.dht.asyncnsp.mputdictwo:    libhandle 2: (`dht_asyncnsp_mputdictwo;3);
/ dht_asyncnsp_mputdictwo (  K anspHandleq, K dictq, K putopthq ) 

ms.sk.dht.asyncnsp.mputdict:    libhandle 2: (`dht_asyncnsp_mputdict;2);
/ dht_asyncnsp_mputdict (  K anspHandleq, K dictq ) 

ms.sk.dht.asyncnsp.get:    libhandle 2: (`dht_asyncnsp_get;2);
/ dht_asyncnsp_get (  K anspHandleq, K keyq )

ms.sk.dht.asyncnsp.waitfor:    libhandle 2: (`dht_asyncnsp_waitfor;2);
/ dht_asyncnsp_waitfor (  K anspHandleq, K keyq )

ms.sk.dht.asyncnsp.mget:    libhandle 2: (`dht_asyncnsp_mget;2);
/ dht_asyncnsp_mget (  K anspHandleq, K keylistq )

ms.sk.dht.asyncnsp.mwait:    libhandle 2: (`dht_asyncnsp_mwait;2);
/ dht_asyncnsp_mwait (  K anspHandleq, K keylistq )

ms.sk.dht.asyncnsp.mgetwo:    libhandle 2: (`dht_asyncnsp_mgetwo;3);
/ dht_asyncnsp_mgetwo (  K anspHandleq, K keylistq, K getoptsHandleq )

ms.sk.dht.asyncnsp.mwaitwo:    libhandle 2: (`dht_asyncnsp_mwaitwo;3);
/ dht_asyncnsp_mwaitwo (  K anspHandleq, K keylistq, K waitoptsHandleq )

ms.sk.dht.asyncnsp.snapshot:    libhandle 2: (`dht_asyncnsp_snapshot;1);
/ dht_asyncnsp_snapshot (  K anspHandleq )

ms.sk.dht.asyncnsp.snapshotver:    libhandle 2: (`dht_asyncnsp_snapshotver;2);
/ dht_asyncnsp_snapshotver (  K anspHandleq, K versionq )

ms.sk.dht.asyncnsp.syncrequest:    libhandle 2: (`dht_asyncnsp_syncrequest;1);
/ dht_asyncnsp_syncrequest (  K anspHandleq )

ms.sk.dht.asyncnsp.syncrequestver:    libhandle 2: (`dht_asyncnsp_syncrequestver;2);
/ dht_asyncnsp_syncrequestver (  K anspHandleq, K versionq )

ms.sk.dht.asyncnsp.getoptions:    libhandle 2: (`dht_asyncnsp_getoptions;1);
/ dht_asyncnsp_getoptions (  K anspHandleq )

ms.sk.dht.asyncnsp.getnamespace:    libhandle 2: (`dht_asyncnsp_getnamespace;1);
/ dht_asyncnsp_getnamespace (  K anspHandleq )

ms.sk.dht.asyncnsp.getname:    libhandle 2: (`dht_asyncnsp_getname;1);
/ dht_asyncnsp_getname (  K anspHandleq )

ms.sk.dht.asyncnsp.setoptions:                libhandle 2: (`dht_asyncnsp_setoptions;2);
/ dht_asyncnsp_setoptions( K asyncnspq , K nsPerspOptq)

ms.sk.dht.asyncnsp.setdefaultversion:         libhandle 2: (`dht_asyncnsp_setdefaultversion;2);
/ dht_asyncnsp_setdefaultversion( K asyncnspq , K versionq)

ms.sk.dht.asyncnsp.setretrievalverconstraint: libhandle 2: (`dht_asyncnsp_setretrievalverconstraint;2);
/ dht_asyncnsp_setretrievalverconstraint( K asyncnspq , K versionConstraintq)

ms.sk.dht.asyncnsp.setversionprovider:        libhandle 2: (`dht_asyncnsp_setversionprovider;2);
/ dht_asyncnsp_setversionprovider( K asyncnspq , K versionProviderq)

ms.sk.dht.asyncnsp.close:                     libhandle 2: (`dht_asyncnsp_close;1);
/ dht_asyncnsp_close( K asyncnspq )

ms.sk.dht.asyncnsp.delete:                    libhandle 2: (`dht_asyncnsp_delete;1);
/ dht_asyncnsp_delete (  K anspHandleq )

ms.sk.dht.asnapshot.getstate:    libhandle 2: (`dht_asnapshot_getstate;1);
/ dht_asnapshot_getstate (  K asnapshotHandleq )

ms.sk.dht.asnapshot.getfailurecause:    libhandle 2: (`dht_asnapshot_getfailurecause;1);
/ dht_asnapshot_getfailurecause (  K asnapshotHandleq )

ms.sk.dht.asnapshot.waitforcompletion:    libhandle 2: (`dht_asnapshot_waitforcompletion;1);
/ dht_asnapshot_waitforcompletion (  K asnapshotHandleq )

ms.sk.dht.asnapshot.waitforcompletiontm:    libhandle 2: (`dht_asnapshot_waitforcompletiontm;3);
/ dht_asnapshot_waitforcompletiontm (  K asnapshotHandleq, K timeoutq, K timeunitq )

ms.sk.dht.asnapshot.close:    libhandle 2: (`dht_asnapshot_close;1);
/ dht_asnapshot_close (  K asnapshotHandleq )

ms.sk.dht.asnapshot.delete:    libhandle 2: (`dht_asnapshot_delete;1);
/ dht_asnapshot_delete (  K asnapshotHandleq )

ms.sk.dht.asyncreq.getstate:    libhandle 2: (`dht_asyncreq_getstate;1);
/ dht_asyncreq_getstate (  K asyncreqHandleq )

ms.sk.dht.asyncreq.getfailurecause:    libhandle 2: (`dht_asyncreq_getfailurecause;1);
/ dht_asyncreq_getfailurecause (  K asyncreqHandleq )

ms.sk.dht.asyncreq.waitforcompletion:    libhandle 2: (`dht_asyncreq_waitforcompletion;1);
/ dht_asyncreq_waitforcompletion (  K asyncreqHandleq )

ms.sk.dht.asyncreq.waitforcompletiontm:    libhandle 2: (`dht_asyncreq_waitforcompletiontm;3);
/ dht_asyncreq_waitforcompletiontm (  K asyncreqHandleq, K timeoutq, K timeunitq )

ms.sk.dht.asyncreq.close:    libhandle 2: (`dht_asyncreq_close;1);
/ dht_asyncreq_close (  K asyncreqHandleq )

ms.sk.dht.asyncreq.delete:    libhandle 2: (`dht_asyncreq_delete;1);
/ dht_asyncreq_delete (  K asyncreqHandleq )

ms.sk.dht.asyncput.getstate:    libhandle 2: (`dht_asyncput_getstate;1);
/ dht_asyncput_getstate (  K asyncputHandleq )

ms.sk.dht.asyncput.getfailurecause:    libhandle 2: (`dht_asyncput_getfailurecause;1);
/ dht_asyncput_getfailurecause (  K asyncputHandleq )

ms.sk.dht.asyncput.waitforcompletion:    libhandle 2: (`dht_asyncput_waitforcompletion;1);
/ dht_asyncput_waitforcompletion (  K asyncputHandleq )

ms.sk.dht.asyncput.waitforcompletiontm:    libhandle 2: (`dht_asyncput_waitforcompletiontm;3);
/ dht_asyncput_waitforcompletiontm (  K asyncputHandleq, K timeoutq, K timeunitq )

ms.sk.dht.asyncput.close:    libhandle 2: (`dht_asyncput_close;1);
/ dht_asyncput_close (  K asyncputHandleq )

ms.sk.dht.asyncput.getoperationstate:    libhandle 2: (`dht_asyncput_getoperationstate;2);
/ dht_asyncput_getoperationstate (  K asyncputHandleq, K keyq )

ms.sk.dht.asyncput.getkeys:    libhandle 2: (`dht_asyncput_getkeys;1);
/ dht_asyncput_getkeys (  K asyncputHandleq )

ms.sk.dht.asyncput.getincompletekeys:    libhandle 2: (`dht_asyncput_getincompletekeys;1);
/ dht_asyncput_getincompletekeys (  K asyncputHandleq )

ms.sk.dht.asyncput.getoperationstatemap:    libhandle 2: (`dht_asyncput_getoperationstatemap;1);
/ dht_asyncput_getoperationstatemap (  K asyncputHandleq )

ms.sk.dht.asyncput.getnumkeys:    libhandle 2: (`dht_asyncput_getnumkeys;1);
/ dht_asyncput_getnumkeys ( K asyncputHandleq )

ms.sk.dht.asyncput.delete:    libhandle 2: (`dht_asyncput_delete;1);
/ dht_asyncput_delete (  K asyncputHandleq )

ms.sk.dht.aretrieval.getstate:    libhandle 2: (`dht_aretrieval_getstate;1);
/ dht_aretrieval_getstate (  K aretievalHandleq )

ms.sk.dht.aretrieval.getfailurecause:    libhandle 2: (`dht_aretrieval_getfailurecause;1);
/ dht_aretrieval_getfailurecause (  K aretievalHandleq )

ms.sk.dht.aretrieval.waitforcompletion:    libhandle 2: (`dht_aretrieval_waitforcompletion;1);
/ dht_aretrieval_waitforcompletion (  K aretievalHandleq )

//ms.sk.dht.aretrieval.waitforcompletiontm:    libhandle 2: (`dht_aretrieval_waitforcompletiontm;3);
/// dht_aretrieval_waitforcompletiontm (  K aretievalHandleq, K timeoutq, K timeunitq )

ms.sk.dht.aretrieval.close:    libhandle 2: (`dht_aretrieval_close;1);
/ dht_aretrieval_close (  K aretievalHandleq )

ms.sk.dht.aretrieval.getoperationstate:    libhandle 2: (`dht_aretrieval_getoperationstate;2);
/ dht_aretrieval_getoperationstate (  K aretievalHandleq, K keyq )

ms.sk.dht.aretrieval.getkeys:    libhandle 2: (`dht_aretrieval_getkeys;1);
/ dht_aretrieval_getkeys (  K aretievalHandleq )

ms.sk.dht.aretrieval.getincompletekeys:    libhandle 2: (`dht_aretrieval_getincompletekeys;1);
/ dht_aretrieval_getincompletekeys (  K aretievalHandleq )

ms.sk.dht.aretrieval.getoperationstatemap:    libhandle 2: (`dht_aretrieval_getoperationstatemap;1);
/ dht_aretrieval_getoperationstatemap (  K aretievalHandleq )

ms.sk.dht.aretrieval.getstoredvalue:    libhandle 2: (`dht_aretrieval_getstoredvalue;2);
/ dht_aretrieval_getstoredvalue (  K aretievalHandleq, K keyq )

ms.sk.dht.aretrieval.getmeta:    libhandle 2: (`dht_aretrieval_getmeta;2);
/ dht_aretrieval_getmeta (  K aretievalHandleq, K keyq )

ms.sk.dht.aretrieval.getnumkeys:    libhandle 2: (`dht_aretrieval_getnumkeys;1);
/ dht_aretrieval_getnumkeys (  KJ aretievalHandleq )

ms.sk.dht.aretrieval.delete:    libhandle 2: (`dht_aretrieval_delete;1);
/ dht_aretrieval_delete (  K aretievalHandleq )

ms.sk.dht.aretrieval.getstoredvalues:    libhandle 2: (`dht_aretrieval_getstoredvalues;1);
/ dht_aretrieval_getstoredvalues (  K aretievalHandleq )

ms.sk.dht.aretrieval.getlateststoredvalues:    libhandle 2: (`dht_aretrieval_getlateststoredvalues;1);
/ dht_aretrieval_getlateststoredvalues (  K aretievalHandleq )

ms.sk.dht.aretrieval.getstoredvalueval:    libhandle 2: (`dht_aretrieval_getstoredvalueval;2);
/ dht_aretrieval_getstoredvalueval (  K aretievalHandleq, K keyq )

ms.sk.dht.avalret.getstate:    libhandle 2: (`dht_avalret_getstate;1);
/ dht_avalret_getstate (  K avalretHandleq )

ms.sk.dht.avalret.getfailurecause:    libhandle 2: (`dht_avalret_getfailurecause;1);
/ dht_avalret_getfailurecause (  K avalretHandleq )

ms.sk.dht.avalret.waitforcompletion:    libhandle 2: (`dht_avalret_waitforcompletion;1);
/ dht_avalret_waitforcompletion (  K avalretHandleq )

//ms.sk.dht.avalret.waitforcompletiontm:    libhandle 2: (`dht_avalret_waitforcompletiontm;3);
/// dht_avalret_waitforcompletiontm (  K avalretHandleq, K timeoutq, K timeunitq )

ms.sk.dht.avalret.close:    libhandle 2: (`dht_avalret_close;1);
/ dht_avalret_close (  K avalretHandleq )

ms.sk.dht.avalret.getoperationstate:    libhandle 2: (`dht_avalret_getoperationstate;2);
/ dht_avalret_getoperationstate (  K avalretHandleq, K keyq )

ms.sk.dht.avalret.getkeys:    libhandle 2: (`dht_avalret_getkeys;1);
/ dht_avalret_getkeys (  K avalretHandleq )

ms.sk.dht.avalret.getincompletekeys:    libhandle 2: (`dht_avalret_getincompletekeys;1);
/ dht_avalret_getincompletekeys (  K avalretHandleq )

ms.sk.dht.avalret.getoperationstatemap:    libhandle 2: (`dht_avalret_getoperationstatemap;1);
/ dht_avalret_getoperationstatemap (  K avalretHandleq )

ms.sk.dht.avalret.getstoredvalue:    libhandle 2: (`dht_avalret_getstoredvalue;2);
/ dht_avalret_getstoredvalue (  K avalretHandleq, K keyq )

ms.sk.dht.avalret.getmeta:    libhandle 2: (`dht_avalret_getmeta;2);
/ dht_avalret_getmeta (  K avalretHandleq, K keyq )

ms.sk.dht.avalret.getvalue:    libhandle 2: (`dht_avalret_getvalue;2);
/ dht_avalret_getvalue (  K avalretHandleq, K keyq )

ms.sk.dht.avalret.getvalues:    libhandle 2: (`dht_avalret_getvalues;1);
/ dht_avalret_getvalues (  K avalretHandleq )

ms.sk.dht.avalret.getlatestvalues:    libhandle 2: (`dht_avalret_getlatestvalues;1);
/ dht_avalret_getlatestvalues (  K avalretHandleq )

ms.sk.dht.avalret.getnumkeys:    libhandle 2: (`dht_avalret_getnumkeys;1);
/ dht_avalret_getnumkeys (  KJ avalretHandleq )

ms.sk.dht.avalret.delete:    libhandle 2: (`dht_avalret_delete;1);
/ dht_avalret_delete (  K avalretHandleq )

ms.sk.dht.nscreationopt.parse:                   libhandle 2: (`dht_nscreationopt_parse;1);
/ dht_nscreationopt_parse (  K defstringq )

ms.sk.dht.nscreationopt.defaultoptions:          libhandle 2: (`dht_nscreationopt_defaultoptions;1);
/ dht_nscreationopt_defaultoptions ( K dummy )

ms.sk.dht.nscreationopt.canbeexplicitlycreated:  libhandle 2: (`dht_nscreationopt_canbeexplicitlycreated;2);
/ dht_nscreationopt_canbeexplicitlycreated (  K nscOptsHandleq, K nsq )

ms.sk.dht.nscreationopt.canbeautocreated:        libhandle 2: (`dht_nscreationopt_canbeautocreated;2);
/ dht_nscreationopt_canbeautocreated (  K nscOptsHandleq, K nsq )

ms.sk.dht.nscreationopt.getdefaultnsoptions:     libhandle 2: (`dht_nscreationopt_getdefaultnsoptions;2);
/ dht_nscreationopt_getdefaultnsoptions (  K nscOptsHandleq, K nsq )

ms.sk.dht.nscreationopt.new:                     libhandle 2: (`dht_nscreationopt_new;3);
/ dht_nscreationopt_new (  K nscreationmodeq, K regexq, K nsOptsHandleq )

ms.sk.dht.nscreationopt.delete:                  libhandle 2: (`dht_nscreationopt_delete;1);
/ dht_nscreationopt_delete (  K nscOptsHandleq )

ms.sk.dht.retrivalopt.new:                       libhandle 2: (`dht_retrivalopt_new;2);
/ dht_retrivalopt_new (  K retrievalTypeq, K waitModeq )

ms.sk.dht.retrivalopt.new3:                      libhandle 2: (`dht_retrivalopt_new3;3);
/ dht_retrivalopt_new3 (  K retrievalTypeq, K waitModeq, K versionConstrq )

ms.sk.dht.retrivalopt.new5:                      libhandle 2: (`dht_retrivalopt_new5;5);
/ dht_retrivalopt_new5 (  K retrievalTypeq, K waitModeq, K versionConstrq, K nonExistenceResponseq, K verifyChecksumsq )

ms.sk.dht.retrivalopt.getnonexistenceresponse:   libhandle 2: (`dht_retrivalopt_getnonexistenceresponse;1);
/ dht_retrivalopt_getnonexistenceresponse (  K retroptHandleq )

ms.sk.dht.retrivalopt.getretrievaltype:          libhandle 2: (`dht_retrivalopt_getretrievaltype;1);
/ dht_retrivalopt_getretrievaltype (  K retroptHandleq )

ms.sk.dht.retrivalopt.getwaitmode:               libhandle 2: (`dht_retrivalopt_getwaitmode;1);
/ dht_retrivalopt_getwaitmode (  K retroptHandleq )

ms.sk.dht.retrivalopt.getversionconstraint:      libhandle 2: (`dht_retrivalopt_getversionconstraint;1);
/ dht_retrivalopt_getversionconstraint (  K retroptHandleq )

ms.sk.dht.retrivalopt.getverifychecksums:        libhandle 2: (`dht_retrivalopt_getverifychecksums;1);
/ dht_retrivalopt_getverifychecksums (  K retroptHandleq )

ms.sk.dht.retrivalopt.retrievaltype:             libhandle 2: (`dht_retrivalopt_retrievaltype;2);
/ dht_retrivalopt_retrievaltype (  K retroptHandleq, K retrievalTypeq )

ms.sk.dht.retrivalopt.waitmode:                  libhandle 2: (`dht_retrivalopt_waitmode;2);
/ dht_retrivalopt_waitmode (  K retroptHandleq, K waitModeq )

ms.sk.dht.retrivalopt.nonexistenceresponse:      libhandle 2: (`dht_retrivalopt_nonexistenceresponse;2);
/ dht_retrivalopt_nonexistenceresponse (  K retroptHandleq, K nonExistenceResponseq )

ms.sk.dht.retrivalopt.versionconstraint:         libhandle 2: (`dht_retrivalopt_versionconstraint;2);
/ dht_retrivalopt_versionconstraint (  K retroptHandleq, K versionConstrq )

ms.sk.dht.retrivalopt.getforwardingmode:    libhandle 2: (`dht_retrivalopt_getforwardingmode;1);
/ dht_retrivalopt_getforwardingmode ( K retroptHandleq )

ms.sk.dht.retrivalopt.forwardingmode:    libhandle 2: (`dht_retrivalopt_forwardingmode;2);
/ dht_retrivalopt_forwardingmode ( K retroptHandleq, K forwardingModeq )

ms.sk.dht.retrivalopt.delete:                    libhandle 2: (`dht_retrivalopt_delete;1);
/ dht_retrivalopt_delete (  K retroptHandleq )

ms.sk.dht.getopts.new:    libhandle 2: (`dht_getopts_new;1);
/ dht_getopts_new (  K retrievalTypeq )

ms.sk.dht.getopts.new2:    libhandle 2: (`dht_getopts_new2;2);
/ dht_getopts_new2 (  K retrievalTypeq, K versionConstrq )

ms.sk.dht.getopts.new3:    libhandle 2: (`dht_getopts_new3;3);
/ dht_getopts_new3 ( KJ opTimeoutCtrlq, KI retrievalTypeq, KI versionConstrq )

ms.sk.dht.getopts.new7:    libhandle 2: (`dht_getopts_new7;7);
/ dht_getopts_new7 ( KJ opTimeoutCtrlq, KI retrievalTypeq, KI versionConstrq, KI nonExistenceRespq, KB verifyChkSumq, KB updateSecTgtOnMissq, KJ secondaryTgtsq)

ms.sk.dht.getopts.parse:    libhandle 2: (`dht_getopts_parse;1);
/ dht_getopts_parse ( KC defq )

ms.sk.dht.getopts.tostring:    libhandle 2: (`dht_getopts_tostring;1);
/ dht_getopts_tostring ( KJ getoptHandleq )

ms.sk.dht.getopts.getforwardingmode:    libhandle 2: (`dht_getopts_getforwardingmode;1);
/ dht_getopts_getforwardingmode ( KJ getoptHandleq )

ms.sk.dht.getopts.forwardingmode:    libhandle 2: (`dht_getopts_forwardingmode;2);
/ dht_getopts_forwardingmode ( KJ getoptHandleq, KI forwardingModeq )

ms.sk.dht.getopts.getnonexistenceresponse:    libhandle 2: (`dht_getopts_getnonexistenceresponse;1);
/ dht_getopts_getnonexistenceresponse (  K getoptHandleq )

ms.sk.dht.getopts.getretrievaltype:    libhandle 2: (`dht_getopts_getretrievaltype;1);
/ dht_getopts_getretrievaltype (  K getoptHandleq )

ms.sk.dht.getopts.getwaitmode:    libhandle 2: (`dht_getopts_getwaitmode;1);
/ dht_getopts_getwaitmode (  K getoptHandleq )

ms.sk.dht.getopts.getversionconstraint:    libhandle 2: (`dht_getopts_getversionconstraint;1);
/ dht_getopts_getversionconstraint (  K getoptHandleq )

ms.sk.dht.getopts.getverifychecksums:      libhandle 2: (`dht_getopts_getverifychecksums;1);
/ dht_getopts_getverifychecksums (  K getoptHandleq )

ms.sk.dht.getopts.retrievaltype:           libhandle 2: (`dht_getopts_retrievaltype;2);
/ dht_getopts_retrievaltype( K getOptq, K retrievalTypeq )

ms.sk.dht.getopts.versionconstraint:       libhandle 2: (`dht_getopts_versionconstraint;2);
/ dht_getopts_versionconstraint( K getOptq, K versionConstraintq )

ms.sk.dht.getopts.delete:    libhandle 2: (`dht_getopts_delete;1);
/ dht_getopts_delete (  K getoptHandleq )

ms.sk.dht.waitopts.new:    libhandle 2: (`dht_waitopts_new;1);
/ dht_waitopts_new (  K dummy )

ms.sk.dht.waitopts.new1:    libhandle 2: (`dht_waitopts_new1;1);
/ dht_waitopts_new1 (  K retrievalTypeq )

ms.sk.dht.waitopts.new2:    libhandle 2: (`dht_waitopts_new2;2);
/ dht_waitopts_new2 (  K retrievalTypeq, K versionConstrq )

ms.sk.dht.waitopts.new3:    libhandle 2: (`dht_waitopts_new3;3);
/ dht_waitopts_new3 (  K retrievalTypeq, K versionConstrq, K timeoutSecq )

ms.sk.dht.waitopts.new4:    libhandle 2: (`dht_waitopts_new4;4);
/ dht_waitopts_new4 (  K retrievalTypeq, K versionConstrq, K timeoutSecq, K thresholdq )

ms.sk.dht.waitopts.new5:    libhandle 2: (`dht_waitopts_new5;5);
/ dht_waitopts_new5 (  K retrievalTypeq, K versionConstrq, K timeoutSecq, K thresholdq, K timeoutRespq )

ms.sk.dht.waitopts.parse:    libhandle 2: (`dht_waitopts_parse;1);
/ dht_waitopts_parse (  K optDefStringq )

ms.sk.dht.waitopts.retrievaltype:    libhandle 2: (`dht_waitopts_retrievaltype;2);
/ dht_waitopts_retrievaltype (  K waitoptq,  K retrievalTypeq )

ms.sk.dht.waitopts.versionconstraint:    libhandle 2: (`dht_waitopts_versionconstraint;2);
/ dht_waitopts_versionconstraint (  K waitoptq,  K versionConstraintq )

ms.sk.dht.waitopts.timeoutseconds:    libhandle 2: (`dht_waitopts_timeoutseconds;2);
/ dht_waitopts_timeoutseconds (  K waitoptq,  K timeoutSecq )

ms.sk.dht.waitopts.threshold:    libhandle 2: (`dht_waitopts_threshold;2);
/ dht_waitopts_threshold (  K waitoptq,  K thresholdq )

ms.sk.dht.waitopts.timeoutresponse:    libhandle 2: (`dht_waitopts_timeoutresponse;2);
/ dht_waitopts_timeoutresponse (  K waitoptq,  K timeoutResponseq )

ms.sk.dht.waitopts.gettimeoutresponse:    libhandle 2: (`dht_waitopts_gettimeoutresponse;1);
/ dht_waitopts_gettimeoutresponse (  K waitoptq )

ms.sk.dht.waitopts.nonexistenceresponse:    libhandle 2: (`dht_waitopts_nonexistenceresponse;2);
/ dht_waitopts_nonexistenceresponse (  K waitoptq,  K nonexistResponseq )

ms.sk.dht.waitopts.getnonexistenceresponse:    libhandle 2: (`dht_waitopts_getnonexistenceresponse;1);
/ dht_waitopts_getnonexistenceresponse (  K waitoptq )

ms.sk.dht.waitopts.getretrievaltype:    libhandle 2: (`dht_waitopts_getretrievaltype;1);
/ dht_waitopts_getretrievaltype (  K waitoptq )

ms.sk.dht.waitopts.getwaitmode:    libhandle 2: (`dht_waitopts_getwaitmode;1);
/ dht_waitopts_getwaitmode (  K waitoptq )

ms.sk.dht.waitopts.getversionconstraint:    libhandle 2: (`dht_waitopts_getversionconstraint;1);
/ dht_waitopts_getversionconstraint (  K waitoptq )

ms.sk.dht.waitopts.getverifychecksums:    libhandle 2: (`dht_waitopts_getverifychecksums;1);
/ dht_waitopts_getverifychecksums (  K waitoptq )

ms.sk.dht.waitopts.getforwardingmode:    libhandle 2: (`dht_waitopts_getforwardingmode;1);
/ dht_waitopts_getforwardingmode ( KJ waitoptq )

ms.sk.dht.waitopts.forwardingmode:    libhandle 2: (`dht_waitopts_forwardingmode;2);
/ dht_waitopts_forwardingmode ( KJ waitoptq, KI forwardingModeq )

ms.sk.dht.waitopts.delete:    libhandle 2: (`dht_waitopts_delete;1);
/ dht_waitopts_delete (  K waitoptq )

ms.sk.dht.putopts.new:    libhandle 2: (`dht_putopts_new;7);
/ dht_putopts_new ( K opTimeoutControllerq, K compressionq, K checksumtypeq, K checksumcomprvalsq, K versionq, K secondaryTargetsq, K userdataq )
/ secondaryTargetsq : list ; will delete objects from the list

ms.sk.dht.putopts.parse:    libhandle 2: (`dht_putopts_parse;1);
/ dht_putopts_parse (  K optDefStringq )

ms.sk.dht.putopts.compression:    libhandle 2: (`dht_putopts_compression;2);
/ dht_putopts_compression (  K putoptq, K compressionq )

ms.sk.dht.putopts.version:    libhandle 2: (`dht_putopts_version;2);
/ dht_putopts_version (  K putoptq, K versionq )

ms.sk.dht.putopts.userdata:    libhandle 2: (`dht_putopts_userdata;2);
/ dht_putopts_userdata (  K putoptq, K userdataq )

ms.sk.dht.putopts.checksumtype:    libhandle 2: (`dht_putopts_checksumtype;2);
/ dht_putopts_checksumtype (  K putoptq, K userdataq )

ms.sk.dht.putopts.optimeoutcontroller:    libhandle 2: (`dht_putopts_optimeoutcontroller;2);
/dht_putopts_optimeoutcontroller ( K putoptq, K opTimeoutControllerq );

ms.sk.dht.putopts.secondarytargets:    libhandle 2: (`dht_putopts_secondarytargets;2);
/ dht_putopts_secondarytargets(K putoptq, K secondaryTargetListq);
/ this deletes objects in secondaryTargetListq;

ms.sk.dht.putopts.secondarytarget:    libhandle 2: (`dht_putopts_secondarytarget;2);
/ dht_putopts_secondarytarget(K putoptq, K secondaryTargetq);

ms.sk.dht.putopts.getsecondarytargets:    libhandle 2: (`dht_putopts_getsecondarytargets;1);
/ dht_putopts_getsecondarytargets(K putoptq);

ms.sk.dht.putopts.getchecksumcompressedvalues:    libhandle 2: (`dht_putopts_getchecksumcompressedvalues;1);
/ dht_putopts_getchecksumcompressedvalues (  K putoptq );

ms.sk.dht.putopts.getcompression:    libhandle 2: (`dht_putopts_getcompression;1);
/ dht_putopts_getcompression (  K putoptq )

ms.sk.dht.putopts.getchecksumtype:    libhandle 2: (`dht_putopts_getchecksumtype;1);
/ dht_putopts_getchecksumtype (  K putoptq )

ms.sk.dht.putopts.getversion:    libhandle 2: (`dht_putopts_getversion;1);
/ dht_putopts_getversion (  K putoptq )

ms.sk.dht.putopts.getuserdata:    libhandle 2: (`dht_putopts_getuserdata;1);
/ dht_putopts_getuserdata (  K putoptq )

ms.sk.dht.putopts.delete:    libhandle 2: (`dht_putopts_delete;1);
/ dht_putopts_delete (  K putoptq )

ms.sk.dht.sessopts.new:    libhandle 2: (`dht_sessopts_new;1);
/ dht_sessopts_new (  K dhtConfigq )

ms.sk.dht.sessopts.new2:    libhandle 2: (`dht_sessopts_new2;2);
/ dht_sessopts_new2 (  K dhtConfigq, K preferredServerq )

ms.sk.dht.sessopts.new3:    libhandle 2: (`dht_sessopts_new3;3);
/ dht_sessopts_new3 (  K dhtConfigq, K preferredServerq, K sessTmoutCtrq )
// SKSessionOptions(SKClientDHTConfiguration * dhtConfig, const char * preferredServer, SKSessionEstablishmentTimeoutController * pTimeoutController);

ms.sk.dht.sessopts.getdefaulttimeoutcontroller:    libhandle 2: (`dht_sessopts_getdefaulttimeoutcontroller;1);
/ dht_sessopts_getdefaulttimeoutcontroller (  K dummyq )
// static SKSessionEstablishmentTimeoutController * getDefaultTimeoutController();

ms.sk.dht.sessopts.setdefaulttimeoutcontroller:    libhandle 2: (`dht_sessopts_setdefaulttimeoutcontroller;2);
/ dht_sessopts_setdefaulttimeoutcontroller (  K sessionoptq, K sessTmoutCtrq )
// void setDefaultTimeoutController(SKSessionEstablishmentTimeoutController * pDefaultTimeoutController);

ms.sk.dht.sessopts.gettimeoutcontroller:    libhandle 2: (`dht_sessopts_gettimeoutcontroller;1);
/ dht_sessopts_gettimeoutcontroller (  K sessionoptq )
// SKSessionEstablishmentTimeoutController * getTimeoutController();

ms.sk.dht.sessopts.getdhtconfig:    libhandle 2: (`dht_sessopts_getdhtconfig;1);
/ dht_sessopts_getdhtconfig (  K sessionoptq )

ms.sk.dht.sessopts.getpreferredserver:    libhandle 2: (`dht_sessopts_getpreferredserver;1);
/ dht_sessopts_getpreferredserver (  K sessionoptq )

ms.sk.dht.sessopts.tostring:    libhandle 2: (`dht_sessopts_tostring;1);
/ dht_sessopts_tostring (  K sessionoptq )

ms.sk.dht.sessopts.delete:    libhandle 2: (`dht_sessopts_delete;1);
/ dht_sessopts_delete (  K sessionoptq )

ms.sk.dht.verconstraint.new:    libhandle 2: (`dht_verconstraint_new;4);
/ dht_verconstraint_new (  K minVersionq, K maxVersionq, K constrModeq, K maxStorageTimeq )

ms.sk.dht.verconstraint.new3:    libhandle 2: (`dht_verconstraint_new3;3);
/ dht_verconstraint_new3 (  K minVersionq, K maxVersionq, K constrModeq )

ms.sk.dht.verconstraint.exactmatch:    libhandle 2: (`dht_verconstraint_exactmatch;1);
/ dht_verconstraint_exactmatch (  K versionq )

ms.sk.dht.verconstraint.maxaboveorequal:    libhandle 2: (`dht_verconstraint_maxaboveorequal;1);
/ dht_verconstraint_maxaboveorequal (  K versionq )

ms.sk.dht.verconstraint.maxbeloworequal:    libhandle 2: (`dht_verconstraint_maxbeloworequal;1);
/ dht_verconstraint_maxbeloworequal (  K versionq )

ms.sk.dht.verconstraint.minaboveorequal:    libhandle 2: (`dht_verconstraint_minaboveorequal;1);
/ dht_verconstraint_minaboveorequal (  K versionq )

ms.sk.dht.verconstraint.getmax:    libhandle 2: (`dht_verconstraint_getmax;1);
/ dht_verconstraint_getmax (  K verConstraintHandleq )

ms.sk.dht.verconstraint.getmaxcreationtime:    libhandle 2: (`dht_verconstraint_getmaxcreationtime;1);
/ dht_verconstraint_getmaxcreationtime (  K verConstraintHandleq )

ms.sk.dht.verconstraint.getmin:    libhandle 2: (`dht_verconstraint_getmin;1);
/ dht_verconstraint_getmin (  K verConstraintHandleq )

ms.sk.dht.verconstraint.getmode:    libhandle 2: (`dht_verconstraint_getmode;1);
/ dht_verconstraint_getmode (  K verConstraintHandleq )

ms.sk.dht.verconstraint.matches:    libhandle 2: (`dht_verconstraint_matches;2);
/ dht_verconstraint_matches (  K verConstraintHandleq, K versionq )

ms.sk.dht.verconstraint.overlaps:    libhandle 2: (`dht_verconstraint_overlaps;2);
/ dht_verconstraint_overlaps (  K verConstraintHandleq, K otherVerConstrHandleq )

ms.sk.dht.verconstraint.equals:    libhandle 2: (`dht_verconstraint_equals;2);
/ dht_verconstraint_equals (  K verConstraintHandleq, K otherVerConstrHandleq )

ms.sk.dht.verconstraint.max:    libhandle 2: (`dht_verconstraint_max;2);
/ dht_verconstraint_max (  K verConstraintHandleq, K maxVersionq )

ms.sk.dht.verconstraint.maxcreationtime:    libhandle 2: (`dht_verconstraint_maxcreationtime;2);
/ dht_verconstraint_maxcreationtime (  K verConstraintHandleq, K creationTimeq )

ms.sk.dht.verconstraint.min:    libhandle 2: (`dht_verconstraint_min;2);
/ dht_verconstraint_min (  K verConstraintHandleq, K minVersionq )

ms.sk.dht.verconstraint.mode:    libhandle 2: (`dht_verconstraint_mode;2);
/ dht_verconstraint_mode (  K verConstraintHandleq, K vcModeq )

ms.sk.dht.verconstraint.delete:    libhandle 2: (`dht_verconstraint_delete;1);
/ dht_verconstraint_delete (  K verConstraintHandleq )

ms.sk.dht.nsperspectiveopt.new5:    libhandle 2: (`dht_nsperspectiveopt_new5;5);
/ dht_nsperspectiveopt_new5 (  K keydigesttypeq, K putOptionsq, K getOptionsq, K waitOptionsq, K versionProviderq )

ms.sk.dht.nsperspectiveopt.parse:    libhandle 2: (`dht_nsperspectiveopt_parse;2);
/ dht_nsperspectiveopt_parse (  K nspOptsHandleq, K defq )

ms.sk.dht.nsperspectiveopt.getkeydigesttype:    libhandle 2: (`dht_nsperspectiveopt_getkeydigesttype;1);
/ dht_nsperspectiveopt_getkeydigesttype (  K nspoptHandleq )

ms.sk.dht.nsperspectiveopt.getdefaultputoptions:    libhandle 2: (`dht_nsperspectiveopt_getdefaultputoptions;1);
/ dht_nsperspectiveopt_getdefaultputoptions (  K nspoptHandleq )

ms.sk.dht.nsperspectiveopt.getdefaultgetoptions:    libhandle 2: (`dht_nsperspectiveopt_getdefaultgetoptions;1);
/ dht_nsperspectiveopt_getdefaultgetoptions (  K nspoptHandleq )

ms.sk.dht.nsperspectiveopt.getdefaultwaitoptions:    libhandle 2: (`dht_nsperspectiveopt_getdefaultwaitoptions;1);
/ dht_nsperspectiveopt_getdefaultwaitoptions (  K nspoptHandleq )

ms.sk.dht.nsperspectiveopt.getdefaultversionprovider:    libhandle 2: (`dht_nsperspectiveopt_getdefaultversionprovider;1);
/ dht_nsperspectiveopt_getdefaultversionprovider (  K nspoptHandleq )

ms.sk.dht.nsperspectiveopt.keydigesttype:    libhandle 2: (`dht_nsperspectiveopt_keydigesttype;2);
/ dht_nsperspectiveopt_keydigesttype (  K nspoptHandleq , K keyDigestTypeq)

ms.sk.dht.nsperspectiveopt.defaultputoptions:    libhandle 2: (`dht_nsperspectiveopt_defaultputoptions;2);
/ dht_nsperspectiveopt_defaultputoptions (  K nspoptHandleq , K defaultPutOpsq)

ms.sk.dht.nsperspectiveopt.defaultgetoptions:    libhandle 2: (`dht_nsperspectiveopt_defaultgetoptions;2);
/ dht_nsperspectiveopt_defaultgetoptions (  K nspoptHandleq , K defaultGetOpsq)

ms.sk.dht.nsperspectiveopt.defaultwaitoptions:    libhandle 2: (`dht_nsperspectiveopt_defaultwaitoptions;2);
/ dht_nsperspectiveopt_defaultwaitoptions (  K nspoptHandleq , K defaultWaitOpsq)

ms.sk.dht.nsperspectiveopt.defaultversionprovider:    libhandle 2: (`dht_nsperspectiveopt_defaultversionprovider;2);
/ dht_nsperspectiveopt_defaultversionprovider (  K nspoptHandleq , K versionProviderq)

ms.sk.dht.nsperspectiveopt.delete:    libhandle 2: (`dht_nsperspectiveopt_delete;1);
/ dht_nsperspectiveopt_delete (  K nspoptHandleq )

ms.sk.dht.versionprovider.getversion:    libhandle 2: (`dht_versionprovider_getversion;1);
/ dht_versionprovider_getversion (  K versionProviderq )

ms.sk.dht.versionprovider.delete:    libhandle 2: (`dht_versionprovider_delete;1);
/ dht_versionprovider_delete (  K versionProviderq )

ms.sk.dht.syncwritablensp.put:    libhandle 2: (`dht_syncwritablensp_put;3);
/ dht_syncwritablensp_put (  K swnspHandleq, K keyq, K valueq ) 

ms.sk.dht.syncwritablensp.putwo:    libhandle 2: (`dht_syncwritablensp_putwo;4);
/ dht_syncwritablensp_putwo (  K swnspHandleq, K keyq, K valueq, K putopthq ) 

ms.sk.dht.syncwritablensp.mputwo:    libhandle 2: (`dht_syncwritablensp_mputwo;4);
/ dht_syncwritablensp_mputwo (  K swnspHandleq, K keylistq, K valuelistq, K putopthq ) 

ms.sk.dht.syncwritablensp.mput:    libhandle 2: (`dht_syncwritablensp_mput;3);
/ dht_syncwritablensp_mput (  K swnspHandleq, K keylistq, K valuelistq ) 

ms.sk.dht.syncwritablensp.mputdictwo:    libhandle 2: (`dht_syncwritablensp_mputdictwo;3);
/ dht_syncwritablensp_mputdictwo (  K swnspHandleq, K dictq, K putopthq ) 

ms.sk.dht.syncwritablensp.mputdict:    libhandle 2: (`dht_syncwritablensp_mputdict;2);
/ dht_syncwritablensp_mputdict (  K swnspHandleq, K dictq ) 

ms.sk.dht.syncwritablensp.snapshot:    libhandle 2: (`dht_syncwritablensp_snapshot;1);
/ dht_syncwritablensp_snapshot (  K swnspHandleq )

ms.sk.dht.syncwritablensp.snapshotver:    libhandle 2: (`dht_syncwritablensp_snapshotver;2);
/ dht_syncwritablensp_snapshotver (  K swnspHandleq, K versionq )

ms.sk.dht.syncwritablensp.syncrequest:    libhandle 2: (`dht_syncwritablensp_syncrequest;1);
/ dht_syncwritablensp_syncrequest (  K swnspHandleq )

ms.sk.dht.syncwritablensp.syncrequestver:    libhandle 2: (`dht_syncwritablensp_syncrequestver;2);
/ dht_syncwritablensp_syncrequestver (  K swnspHandleq, K versionq )

ms.sk.dht.syncwritablensp.getoptions:    libhandle 2: (`dht_syncwritablensp_getoptions;1);
/ dht_syncwritablensp_getoptions (  K swnspHandleq )

ms.sk.dht.syncwritablensp.getnamespace:    libhandle 2: (`dht_syncwritablensp_getnamespace;1);
/ dht_syncwritablensp_getnamespace (  K swnspHandleq )

ms.sk.dht.syncwritablensp.getname:    libhandle 2: (`dht_syncwritablensp_getname;1);
/ dht_syncwritablensp_getname (  K swnspHandleq )

ms.sk.dht.syncwritablensp.setoptions:    libhandle 2: (`dht_syncwritablensp_setoptions;2);
/ dht_syncwritablensp_setoptions( K swnspHandleq , K nsPerspOptq )

ms.sk.dht.syncwritablensp.setdefaultversion:    libhandle 2: (`dht_syncwritablensp_setdefaultversion;2);
/ dht_syncwritablensp_setdefaultversion( K swnspHandleq , K versionq )

ms.sk.dht.syncwritablensp.setretrievalverconstraint:    libhandle 2: (`dht_syncwritablensp_setretrievalverconstraint;2);
/ dht_syncwritablensp_setretrievalverconstraint( K swnspHandleq , K versionConstraintq )

ms.sk.dht.syncwritablensp.setversionprovider:    libhandle 2: (`dht_syncwritablensp_setversionprovider;2);
/ dht_syncwritablensp_setversionprovider( K swnspHandleq , K versionProviderq )

ms.sk.dht.syncwritablensp.close:    libhandle 2: (`dht_syncwritablensp_close;1);
/ dht_syncwritablensp_close( K swnspHandleq )

ms.sk.dht.syncwritablensp.delete:    libhandle 2: (`dht_syncwritablensp_delete;1);
/ dht_syncwritablensp_delete (  K swnspHandleq )

ms.sk.dht.syncreadablensp.get:    libhandle 2: (`dht_syncreadablensp_get;2);
/ dht_syncreadablensp_get (  K srnspHandleq, K keyq )

ms.sk.dht.syncreadablensp.waitfor:    libhandle 2: (`dht_syncreadablensp_waitfor;2);
/ dht_syncreadablensp_waitfor (  K srnspHandleq, K keyq )

ms.sk.dht.syncreadablensp.getwo:    libhandle 2: (`dht_syncreadablensp_getwo;3);
/ dht_syncreadablensp_getwo (  K srnspHandleq, K keyq, K getopsHandleq )

ms.sk.dht.syncreadablensp.waitforwo:    libhandle 2: (`dht_syncreadablensp_waitforwo;3);
/ dht_syncreadablensp_waitforwo (  K srnspHandleq, K keyq, K waitopsHandleq )

ms.sk.dht.syncreadablensp.getasdictwo:    libhandle 2: (`dht_syncreadablensp_getasdictwo;3);
/ dht_syncreadablensp_getasdictwo (  K srnspHandleq, K keyq, K getopsHandleq )

ms.sk.dht.syncreadablensp.waitforasdictwo:    libhandle 2: (`dht_syncreadablensp_waitforasdictwo;3);
/ dht_syncreadablensp_waitforasdictwo (  K srnspHandleq, K keyq, K waitopsHandleq )

ms.sk.dht.syncreadablensp.mget:    libhandle 2: (`dht_syncreadablensp_mget;2);
/ dht_syncreadablensp_mget (  K srnspHandleq, K keylistq )

ms.sk.dht.syncreadablensp.mwait:    libhandle 2: (`dht_syncreadablensp_mwait;2);
/ dht_syncreadablensp_mwait (  K srnspHandleq, K keylistq )

ms.sk.dht.syncreadablensp.getoptions:    libhandle 2: (`dht_syncreadablensp_getoptions;1);
/ dht_syncreadablensp_getoptions (  K srnspHandleq )

ms.sk.dht.syncreadablensp.getnamespace:    libhandle 2: (`dht_syncreadablensp_getnamespace;1);
/ dht_syncreadablensp_getnamespace (  K srnspHandleq )

ms.sk.dht.syncreadablensp.getname:    libhandle 2: (`dht_syncreadablensp_getname;1);
/ dht_syncreadablensp_getname (  K srnspHandleq )

ms.sk.dht.syncreadablensp.setoptions:    libhandle 2: (`dht_syncreadablensp_setoptions;2);
/ dht_syncreadablensp_setoptions( K srnspHandleq , K nsPerspOptq )

ms.sk.dht.syncreadablensp.setdefaultversion:    libhandle 2: (`dht_syncreadablensp_setdefaultversion;2);
/ dht_syncreadablensp_setdefaultversion( K srnspHandleq , K versionq )

ms.sk.dht.syncreadablensp.setretrievalverconstraint:    libhandle 2: (`dht_syncreadablensp_setretrievalverconstraint;2);
/ dht_syncreadablensp_setretrievalverconstraint( K srnspHandleq , K versionConstraintq )

ms.sk.dht.syncreadablensp.setversionprovider:    libhandle 2: (`dht_syncreadablensp_setversionprovider;2);
/ dht_syncreadablensp_setversionprovider( K srnspHandleq , K versionProviderq )

ms.sk.dht.syncreadablensp.close:    libhandle 2: (`dht_syncreadablensp_close;1);
/ dht_syncreadablensp_close( K srnspHandleq )

ms.sk.dht.syncreadablensp.delete:    libhandle 2: (`dht_syncreadablensp_delete;1);
/ dht_syncreadablensp_delete (  K srnspHandleq )

ms.sk.dht.asyncwritablensp.put:    libhandle 2: (`dht_asyncwritablensp_put;3);
/ dht_asyncwritablensp_put (  K awnspHandleq, K keyq, K valueq ) 

ms.sk.dht.asyncwritablensp.putwo:    libhandle 2: (`dht_asyncwritablensp_putwo;4);
/ dht_asyncwritablensp_putwo (  K awnspHandleq, K keyq, K valueq, K putopthq ) 

ms.sk.dht.asyncwritablensp.mputwo:    libhandle 2: (`dht_asyncwritablensp_mputwo;4);
/ dht_asyncwritablensp_mputwo (  K awnspHandleq, K keylistq, K valuelistq, K putopthq ) 

ms.sk.dht.asyncwritablensp.mput:    libhandle 2: (`dht_asyncwritablensp_mput;3);
/ dht_asyncwritablensp_mput (  K awnspHandleq, K keylistq, K valuelistq ) 

ms.sk.dht.asyncwritablensp.mputdictwo:    libhandle 2: (`dht_asyncwritablensp_mputdictwo;3);
/ dht_asyncwritablensp_mputdictwo (  K awnspHandleq, K dictq, K putopthq ) 

ms.sk.dht.asyncwritablensp.mputdict:    libhandle 2: (`dht_asyncwritablensp_mputdict;2);
/ dht_asyncwritablensp_mputdict (  K awnspHandleq, K dictq ) 

ms.sk.dht.asyncwritablensp.snapshot:    libhandle 2: (`dht_asyncwritablensp_snapshot;1);
/ dht_asyncwritablensp_snapshot (  K awnspHandleq )

ms.sk.dht.asyncwritablensp.snapshotver:    libhandle 2: (`dht_asyncwritablensp_snapshotver;2);
/ dht_asyncwritablensp_snapshotver (  K awnspHandleq, K versionq )

ms.sk.dht.asyncwritablensp.syncrequest:    libhandle 2: (`dht_asyncwritablensp_syncrequest;1);
/ dht_asyncwritablensp_syncrequest (  K awnspHandleq )

ms.sk.dht.asyncwritablensp.syncrequestver:    libhandle 2: (`dht_asyncwritablensp_syncrequestver;2);
/ dht_asyncwritablensp_syncrequestver (  K awnspHandleq, K versionq )

ms.sk.dht.asyncwritablensp.getoptions:    libhandle 2: (`dht_asyncwritablensp_getoptions;1);
/ dht_asyncwritablensp_getoptions (  K awnspHandleq )

ms.sk.dht.asyncwritablensp.getnamespace:    libhandle 2: (`dht_asyncwritablensp_getnamespace;1);
/ dht_asyncwritablensp_getnamespace (  K awnspHandleq )

ms.sk.dht.asyncwritablensp.getname:    libhandle 2: (`dht_asyncwritablensp_getname;1);
/ dht_asyncwritablensp_getname (  K awnspHandleq )

ms.sk.dht.asyncwritablensp.setoptions:    libhandle 2: (`dht_asyncwritablensp_setoptions;2);
/ dht_asyncwritablensp_setoptions( K awnspHandleq , K nsPerspOptq )

ms.sk.dht.asyncwritablensp.setdefaultversion:    libhandle 2: (`dht_asyncwritablensp_setdefaultversion;2);
/ dht_asyncwritablensp_setdefaultversion( K awnspHandleq , K versionq )

ms.sk.dht.asyncwritablensp.setretrievalverconstraint:    libhandle 2: (`dht_asyncwritablensp_setretrievalverconstraint;2);
/ dht_asyncwritablensp_setretrievalverconstraint( K awnspHandleq , K versionConstraintq )

ms.sk.dht.asyncwritablensp.setversionprovider:    libhandle 2: (`dht_asyncwritablensp_setversionprovider;2);
/ dht_asyncwritablensp_setversionprovider( K awnspHandleq , K versionProviderq )

ms.sk.dht.asyncwritablensp.close:    libhandle 2: (`dht_asyncwritablensp_close;1);
/ dht_asyncwritablensp_close( K awnspHandleq )

ms.sk.dht.asyncwritablensp.delete:    libhandle 2: (`dht_asyncwritablensp_delete;1);
/ dht_asyncwritablensp_delete (  K awnspHandleq )

ms.sk.dht.asyncreadablensp.get:    libhandle 2: (`dht_asyncreadablensp_get;2);
/ dht_asyncreadablensp_get (  K arnspHandleq, K keyq )

ms.sk.dht.asyncreadablensp.waitfor:    libhandle 2: (`dht_asyncreadablensp_waitfor;2);
/ dht_asyncreadablensp_waitfor (  K arnspHandleq, K keyq )

ms.sk.dht.asyncreadablensp.mget:    libhandle 2: (`dht_asyncreadablensp_mget;2);
/ dht_asyncreadablensp_mget (  K arnspHandleq, K keylistq )

ms.sk.dht.asyncreadablensp.mwait:    libhandle 2: (`dht_asyncreadablensp_mwait;2);
/ dht_asyncreadablensp_mwait (  K arnspHandleq, K keylistq )

ms.sk.dht.asyncreadablensp.mgetwo:    libhandle 2: (`dht_asyncreadablensp_mgetwo;3);
/ dht_asyncreadablensp_mgetwo (  K arnspHandleq, K keylistq, K getoptsHandleq )

ms.sk.dht.asyncreadablensp.mwaitwo:    libhandle 2: (`dht_asyncreadablensp_mwaitwo;3);
/ dht_asyncreadablensp_mwaitwo (  K arnspHandleq, K keylistq, K waitoptsHandleq )

ms.sk.dht.asyncreadablensp.getoptions:    libhandle 2: (`dht_asyncreadablensp_getoptions;1);
/ dht_asyncreadablensp_getoptions (  K arnspHandleq )

ms.sk.dht.asyncreadablensp.getnamespace:    libhandle 2: (`dht_asyncreadablensp_getnamespace;1);
/ dht_asyncreadablensp_getnamespace (  K arnspHandleq )

ms.sk.dht.asyncreadablensp.getname:    libhandle 2: (`dht_asyncreadablensp_getname;1);
/ dht_asyncreadablensp_getname (  K arnspHandleq )

ms.sk.dht.asyncreadablensp.setoptions:    libhandle 2: (`dht_asyncreadablensp_setoptions;2);
/ dht_asyncreadablensp_setoptions( K arnspHandleq , K nsPerspOptq )

ms.sk.dht.asyncreadablensp.setdefaultversion:    libhandle 2: (`dht_asyncreadablensp_setdefaultversion;2);
/ dht_asyncreadablensp_setdefaultversion( K arnspHandleq , K versionq )

ms.sk.dht.asyncreadablensp.setretrievalverconstraint:    libhandle 2: (`dht_asyncreadablensp_setretrievalverconstraint;2);
/ dht_asyncreadablensp_setretrievalverconstraint( K arnspHandleq , K versionConstraintq )

ms.sk.dht.asyncreadablensp.setversionprovider:    libhandle 2: (`dht_asyncreadablensp_setversionprovider;2);
/ dht_asyncreadablensp_setversionprovider( K arnspHandleq , K versionProviderq )

ms.sk.dht.asyncreadablensp.close:    libhandle 2: (`dht_asyncreadablensp_close;1);
/ dht_asyncreadablensp_close( K arnspHandleq )

ms.sk.dht.asyncreadablensp.delete:    libhandle 2: (`dht_asyncreadablensp_delete;1);
/ dht_asyncreadablensp_delete (  K arnspHandleq )


// -------------------- SKSecondaryTarget ---------------------------------
ms.sk.dht.secondarytarget.new:    libhandle 2: (`dht_secondarytarget_new;2);
/ dht_secondarytarget_new(KI targetTypeq, KC targetq);

ms.sk.dht.secondarytarget.gettype:    libhandle 2: (`dht_secondarytarget_gettype;1);
/ dht_secondarytarget_gettype (  KJ secondaryTgtHandleq )

ms.sk.dht.secondarytarget.gettarget:    libhandle 2: (`dht_secondarytarget_gettarget;1);
/ dht_secondarytarget_gettarget(  KJ secondaryTgtHandleq);

ms.sk.dht.secondarytarget.tostring:    libhandle 2: (`dht_secondarytarget_tostring;1);
/ dht_secondarytarget_tostring( KJ secondaryTgtHandleq);

ms.sk.dht.secondarytarget.delete:    libhandle 2: (`dht_secondarytarget_delete;1);
/ dht_secondarytarget_delete (  KJ secondaryTgtHandleq )


// -------------------- SKOpSizeBasedTimeoutController ---------------------------------
ms.sk.dht.opsizebasedtimeoutcontroller.new:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_new;1);
/ dht_opsizebasedtimeoutcontroller_new(K dummy);
//SKOpSizeBasedTimeoutController();

ms.sk.dht.opsizebasedtimeoutcontroller.new4:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_new4;4);
/ dht_opsizebasedtimeoutcontroller_new4(KI maxAttemptsq, KI constantTimeMillisq, KI itemTimeMillisq, KI maxRelTimeoutMillisq);
// SKOpSizeBasedTimeoutController(int maxAttempts, int constantTimeMillis, int itemTimeMillis, int maxRelTimeoutMillis);

ms.sk.dht.opsizebasedtimeoutcontroller.parse:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_parse;1);
/ dht_opsizebasedtimeoutcontroller_parse(KC defq);
// static SKOpSizeBasedTimeoutController * parse(string def); 

ms.sk.dht.opsizebasedtimeoutcontroller.getmaxattempts:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_getmaxattempts;2);
/ dht_opsizebasedtimeoutcontroller_getmaxattempts(KJ opControllerq, KJ asyncOperationHandleq);
// int getMaxAttempts(SKAsyncOperation * op);

ms.sk.dht.opsizebasedtimeoutcontroller.getrelativetimeoutmillisforattempt:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_getrelativetimeoutmillisforattempt;3);
/ dht_opsizebasedtimeoutcontroller_getrelativetimeoutmillisforattempt(KJ opControllerq, KJ asyncOperationHandleq, KI attemptIndexq);
// int getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex);

ms.sk.dht.opsizebasedtimeoutcontroller.getmaxrelativetimeoutmillis:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_getmaxrelativetimeoutmillis;2);
/ dht_opsizebasedtimeoutcontroller_getmaxrelativetimeoutmillis(KJ opControllerq, KJ asyncOperationHandleq);
// int getMaxRelativeTimeoutMillis(SKAsyncOperation *op);

ms.sk.dht.opsizebasedtimeoutcontroller.itemtimemillis:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_itemtimemillis;2);
/ dht_opsizebasedtimeoutcontroller_itemtimemillis(KJ opControllerq, KI itemTimeMillisq);
// SKOpSizeBasedTimeoutController * itemTimeMillis(int itemTimeMillis);

ms.sk.dht.opsizebasedtimeoutcontroller.constanttimemillis:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_constanttimemillis;2);
/ dht_opsizebasedtimeoutcontroller_constanttimemillis(KJ opControllerq, KI constantTimeMillisq);
//SKOpSizeBasedTimeoutController * constantTimeMillis(int constantTimeMillis);

ms.sk.dht.opsizebasedtimeoutcontroller.maxreltimeoutmillis:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_maxreltimeoutmillis;2);
/ dht_opsizebasedtimeoutcontroller_maxreltimeoutmillis(KJ opControllerq, KI constantTimeMillisq);
// SKOpSizeBasedTimeoutController * maxRelTimeoutMillis(int maxRelTimeoutMillis);

ms.sk.dht.opsizebasedtimeoutcontroller.maxattempts:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_maxattempts;2);
/ dht_opsizebasedtimeoutcontroller_maxattempts(KJ opControllerq, KI maxAttemptsq);
// SKOpSizeBasedTimeoutController * maxAttempts(int maxAttempts);

ms.sk.dht.opsizebasedtimeoutcontroller.tostring:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_tostring;1);
/ dht_opsizebasedtimeoutcontroller_tostring(KJ opControllerq);
//string toString();

ms.sk.dht.opsizebasedtimeoutcontroller.delete:    libhandle 2: (`dht_opsizebasedtimeoutcontroller_delete;1);
/ dht_opsizebasedtimeoutcontroller_delete(KJ opControllerq);
// virtual ~SKOpSizeBasedTimeoutController();


// -------------------- SKSimpleTimeoutController ---------------------------------
ms.sk.dht.simpletimeoutcontroller.new:    libhandle 2: (`dht_simpletimeoutcontroller_new;2);
/ dht_simpletimeoutcontroller_new(KI maxAttemptsq, KI maxRelativeTimeoutMillisq);
// SKSimpleTimeoutController(int maxAttempts, int maxRelativeTimeoutMillis);

ms.sk.dht.simpletimeoutcontroller.parse:    libhandle 2: (`dht_simpletimeoutcontroller_parse;1);
/ dht_simpletimeoutcontroller_parse(KS def);
// static SKSimpleTimeoutController * parse(const char * def); 

ms.sk.dht.simpletimeoutcontroller.getmaxattempts:    libhandle 2: (`dht_simpletimeoutcontroller_getmaxattempts;2);
/ dht_simpletimeoutcontroller_getmaxattempts(KJ opControllerq, KJ asyncOperationq);
// virtual int getMaxAttempts(SKAsyncOperation * op);

ms.sk.dht.simpletimeoutcontroller.getrelativetimeoutmillisforattempt:    libhandle 2: (`dht_simpletimeoutcontroller_getrelativetimeoutmillisforattempt;3);
/ dht_simpletimeoutcontroller_getrelativetimeoutmillisforattempt(KJ opControllerq, KJ asyncOperationq, KI attemptIndexq);
// virtual int getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex);

ms.sk.dht.simpletimeoutcontroller.getmaxrelativetimeoutmillis:    libhandle 2: (`dht_simpletimeoutcontroller_getmaxrelativetimeoutmillis;2);
/ dht_simpletimeoutcontroller_getmaxrelativetimeoutmillis(KJ opControllerq, KJ asyncOperationq);
// virtual int getMaxRelativeTimeoutMillis(SKAsyncOperation *op);

ms.sk.dht.simpletimeoutcontroller.maxattempts:    libhandle 2: (`dht_simpletimeoutcontroller_maxattempts;2);
/ dht_simpletimeoutcontroller_maxattempts(KJ opControllerq, KI maxAttemptsq);
// SKSimpleTimeoutController * maxAttempts(int maxAttempts);

ms.sk.dht.simpletimeoutcontroller.maxrelativetimeoutmillis:    libhandle 2: (`dht_simpletimeoutcontroller_maxrelativetimeoutmillis;2);
/ dht_simpletimeoutcontroller_maxrelativetimeoutmillis(KJ opControllerq, KI maxRelativeTimeoutMillisq);
// SKSimpleTimeoutController * maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis);

ms.sk.dht.simpletimeoutcontroller.tostring:    libhandle 2: (`dht_simpletimeoutcontroller_tostring;1);
/ dht_simpletimeoutcontroller_tostring(KJ opControllerq);
// string toString();

ms.sk.dht.simpletimeoutcontroller.delete:    libhandle 2: (`dht_simpletimeoutcontroller_delete;1);
/ dht_simpletimeoutcontroller_delete(KJ opControllerq);
// virtual ~SKSimpleTimeoutController();


// -------------------- SKWaitForTimeoutController ---------------------------------
ms.sk.dht.waitfortimeoutcontroller.new:    libhandle 2: (`dht_waitfortimeoutcontroller_new;1);
/ dht_waitfortimeoutcontroller_new(K dummy);
// SKWaitForTimeoutController();

ms.sk.dht.waitfortimeoutcontroller.new1:    libhandle 2: (`dht_waitfortimeoutcontroller_new1;1);
/ dht_waitfortimeoutcontroller_new1(KI internalRetryIntervalSecondsq);
// SKWaitForTimeoutController(int internalRetryIntervalSeconds);

ms.sk.dht.waitfortimeoutcontroller.getmaxattempts:    libhandle 2: (`dht_waitfortimeoutcontroller_getmaxattempts;2);
/ dht_waitfortimeoutcontroller_getmaxattempts(KJ opControllerq, KJ asyncOperationq);
// int getMaxAttempts(SKAsyncOperation * op);

ms.sk.dht.waitfortimeoutcontroller.getrelativetimeoutmillisforattempt:    libhandle 2: (`dht_waitfortimeoutcontroller_getrelativetimeoutmillisforattempt;3);
/ dht_waitfortimeoutcontroller_getrelativetimeoutmillisforattempt(KJ opControllerq, KJ asyncOperationq, KI attemptIndexq);
// int getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex);

ms.sk.dht.waitfortimeoutcontroller.getmaxrelativetimeoutmillis:    libhandle 2: (`dht_waitfortimeoutcontroller_getmaxrelativetimeoutmillis;2);
/ dht_waitfortimeoutcontroller_getmaxrelativetimeoutmillis(KJ opControllerq, KJ asyncOperationq);
// int getMaxRelativeTimeoutMillis(SKAsyncOperation *op);

ms.sk.dht.waitfortimeoutcontroller.tostring:    libhandle 2: (`dht_waitfortimeoutcontroller_tostring;1);
/ dht_waitfortimeoutcontroller_tostring(KJ opControllerq);
// toString();

ms.sk.dht.waitfortimeoutcontroller.delete:    libhandle 2: (`dht_waitfortimeoutcontroller_delete;1);
/ dht_waitfortimeoutcontroller_delete(KJ opControllerq);
// ~SKWaitForTimeoutController();


// -------------------- SKOpTimeoutController ---------------------------------
ms.sk.dht.optimeoutcontroller.getmaxattempts:    libhandle 2: (`dht_optimeoutcontroller_getmaxattempts;2);
/ dht_optimeoutcontroller_getmaxattempts(KJ opControllerq, KJ asyncOperationq);
// getMaxAttempts(SKAsyncOperation * op)

ms.sk.dht.optimeoutcontroller.getrelativetimeoutmillisforattempt:    libhandle 2: (`dht_optimeoutcontroller_getrelativetimeoutmillisforattempt;3);
/ dht_optimeoutcontroller_getrelativetimeoutmillisforattempt(KJ opControllerq, KJ asyncOperationq, KI attemptIndexq);
// getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex)

ms.sk.dht.optimeoutcontroller.getmaxrelativetimeoutmillis:    libhandle 2: (`dht_optimeoutcontroller_getmaxrelativetimeoutmillis;2);
/ dht_optimeoutcontroller_getmaxrelativetimeoutmillis(KJ opControllerq, KJ asyncOperationq);
// getMaxRelativeTimeoutMillis(SKAsyncOperation *op)

ms.sk.dht.optimeoutcontroller.delete:    libhandle 2: (`dht_optimeoutcontroller_delete;1);
/ dht_optimeoutcontroller_delete(KJ opControllerq);
// ~SKOpTimeoutController()

// -------------------- SKSessionEstablishmentTimeoutController ---------------------------------
ms.sk.dht.sessiontimeoutcontroller.getmaxattempts:    libhandle 2: (`dht_sessiontimeoutcontroller_getmaxattempts;2);
/ dht_sessiontimeoutcontroller_getmaxattempts(KJ sessControllerq, KJ psessoptsq);
// getMaxAttempts(SKSessionOptions * pSessOpts)

ms.sk.dht.sessiontimeoutcontroller.getrelativetimeoutmillisforattempt:    libhandle 2: (`dht_sessiontimeoutcontroller_getrelativetimeoutmillisforattempt;3);
/ dht_sessiontimeoutcontroller_getrelativetimeoutmillisforattempt(KJ sessControllerq, KJ psessoptsq, KI attemptIndexq);
// getRelativeTimeoutMillisForAttempt(SKSessionOptions * pSessOpts, int attemptIndex)

ms.sk.dht.sessiontimeoutcontroller.getmaxrelativetimeoutmillis:    libhandle 2: (`dht_sessiontimeoutcontroller_getmaxrelativetimeoutmillis;2);
/ dht_sessiontimeoutcontroller_getmaxrelativetimeoutmillis(KJ sessControllerq, KJ psessoptsq);
// getMaxRelativeTimeoutMillis(SKSessionOptions *pSessOpts)

ms.sk.dht.sessiontimeoutcontroller.delete:    libhandle 2: (`dht_sessiontimeoutcontroller_delete;1);
/ dht_sessiontimeoutcontroller_delete(KJ sessControllerq);
// ~SKSessionEstablishmentTimeoutController()


// -------------------- SKSimpleSessionEstablishmentTimeoutController ---------------------------------
ms.sk.dht.simplesessiontimeoutcontroller.new:    libhandle 2: (`dht_simplesessiontimeoutcontroller_new;3);
/ dht_simplesessiontimeoutcontroller_new(KI maxAttemptsq, KI attemptRelativeTimeoutMillisq, KI maxRelativeTimeoutMillisq);
// SKSimpleSessionEstablishmentTimeoutController(int maxAttempts, int maxRelativeTimeoutMillis);

ms.sk.dht.simplesessiontimeoutcontroller.parse:    libhandle 2: (`dht_simplesessiontimeoutcontroller_parse;1);
/ dht_simplesessiontimeoutcontroller_parse(KS def);
// static SKSimpleSessionEstablishmentTimeoutController * parse(const char * def); 

ms.sk.dht.simplesessiontimeoutcontroller.getmaxattempts:    libhandle 2: (`dht_simplesessiontimeoutcontroller_getmaxattempts;2);
/ dht_simplesessiontimeoutcontroller_getmaxattempts(KJ simpSessControllerq, KJ pSessOptsq);
// virtual int getMaxAttempts(SKSessionOptions * op);

ms.sk.dht.simplesessiontimeoutcontroller.getrelativetimeoutmillisforattempt:    libhandle 2: (`dht_simplesessiontimeoutcontroller_getrelativetimeoutmillisforattempt;3);
/ dht_simplesessiontimeoutcontroller_getrelativetimeoutmillisforattempt(KJ simpSessControllerq, KJ pSessOptsq, KI attemptIndexq);
// virtual int getRelativeTimeoutMillisForAttempt(SKSessionOptions * op, int attemptIndex);

ms.sk.dht.simplesessiontimeoutcontroller.getmaxrelativetimeoutmillis:    libhandle 2: (`dht_simplesessiontimeoutcontroller_getmaxrelativetimeoutmillis;2);
/ dht_simplesessiontimeoutcontroller_getmaxrelativetimeoutmillis(KJ simpSessControllerq, KJ pSessOptsq);
// virtual int getMaxRelativeTimeoutMillis(SKSessionOptions *op);

ms.sk.dht.simplesessiontimeoutcontroller.maxattempts:    libhandle 2: (`dht_simplesessiontimeoutcontroller_maxattempts;2);
/ dht_simplesessiontimeoutcontroller_maxattempts(KJ simpSessControllerq, KI maxAttemptsq);
// SKSimpleSessionEstablishmentTimeoutController * maxAttempts(int maxAttempts);

ms.sk.dht.simplesessiontimeoutcontroller.maxrelativetimeoutmillis:    libhandle 2: (`dht_simplesessiontimeoutcontroller_maxrelativetimeoutmillis;2);
/ dht_simplesessiontimeoutcontroller_maxrelativetimeoutmillis(KJ simpSessControllerq, KI maxRelativeTimeoutMillisq);
// SKSimpleSessionEstablishmentTimeoutController * maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis);

ms.sk.dht.simplesessiontimeoutcontroller.attemptrelativetimeoutmillis:    libhandle 2: (`dht_simplesessiontimeoutcontroller_attemptrelativetimeoutmillis;2);
/ dht_simplesessiontimeoutcontroller_attemptrelativetimeoutmillis(KJ simpSessControllerq, KI attemptRelativeTimeoutMillisq);
// SKSimpleSessionEstablishmentTimeoutController * maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis);

ms.sk.dht.simplesessiontimeoutcontroller.tostring:    libhandle 2: (`dht_simplesessiontimeoutcontroller_tostring;1);
/ dht_simplesessiontimeoutcontroller_tostring(KJ simpSessControllerq);
// string toString();

ms.sk.dht.simplesessiontimeoutcontroller.delete:    libhandle 2: (`dht_simplesessiontimeoutcontroller_delete;1);
/ dht_simplesessiontimeoutcontroller_delete(KJ simpSessControllerq);
// virtual ~SKSimpleSessionEstablishmentTimeoutController();



