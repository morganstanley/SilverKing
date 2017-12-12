#include "Util.h"

void usage(char const * const name_, char const * const pMsg_) {
	if (pMsg_) fprintf(stderr, "%s\n", pMsg_);
	fprintf(stderr, "usage:\n");
	fprintf(stderr, "%s <OPTIONS>\n", name_);
	fprintf(stderr, "\t-g GCNAME      Grid Configuration Name\n");
	fprintf(stderr, "\t-H HOST        DHT node server name\n");
	fprintf(stderr, "\t-a ACTION      put|mput|get|waitfor|mget|mwaitfor|getmeta|mgetmeta|sync|snapshot|amput|amget|amwaitfor|amgetmeta|asnapshot|async|createns|clone|linkto|deletens|recoverns\n");
	fprintf(stderr, "\t-n NAMESPACE\n");
	fprintf(stderr, "\t-k KEY\n");
	fprintf(stderr, "\t-v VALUE\n");
	fprintf(stderr, "\t-F FILE        put the content of the file\n");
	fprintf(stderr, "\t-c COMPRESSION none|zip|bzip2|snappy|lz4\n");
	fprintf(stderr, "\t-T SECONDS     waitfor timeout seconds\n");
	fprintf(stderr, "\t-V             turn on verbose logging\n");
	fprintf(stderr, "\t-f FILE        file to send the log to\n");
	fprintf(stderr, "\t-R number_of_runs\n");
	fprintf(stderr, "\t-t threshold_percentage  (for mwaitfor)\n");
	fprintf(stderr, "\t-i number      version number for stored key\n");
	fprintf(stderr, "\t-o nsOptions   namespace options storageType=[FILE|RAM],consistencyProtocol=[TWO_PHASE_COMMIT|LOOSE],versionMode=[SINGLE_VERSION|CLIENT_SPECIFIED|SEQUENTIAL|...]\n");
	fprintf(stderr, "\t-P PARENT      for clone and linkto: parent namespace name, default is none\n");
	fprintf(stderr, "\t-m TYPE        getmeta Retrieval type VALUE|META_DATA|VALUE_AND_META_DATA|EXISTENCE, default is VALUE_AND_META_DATA\n");

	fprintf(stderr, "\t-K number      number of keys (for m* operations)\n");

	fprintf(stderr, "\t-s             use file storage\n");
	fprintf(stderr, "\t-J jvmOptions    options to jvm e.g. \'-Xmx1024M,-Xcheck:jni\'\n");

	if (pMsg_) exit(1);
	exit(0);
}

void exhandler( const char * msg, const char * filename, const int lineNum, const char * nsName) {
	ostringstream str ;
	str << string(msg) << " " << filename << ":" << lineNum << " ";
	try {
		throw;
	} catch (SKRetrievalException & e) {
		e.printStackTrace();
		fprintf( stderr, "SKRetrievalException %s %s:%d\n%s\n failed keys are :\n", msg, filename, lineNum, e.getDetailedFailureMessage().c_str());
		SKVector<string> * failKeys = e.getFailedKeys();
		if (failKeys->size() > 0) {
			int nKeys = failKeys->size();
			for (int ikey = 0; ikey < nKeys; ++ikey) {
				SKOperationState::SKOperationState opst = e.getOperationState(failKeys->at(ikey));
				if (opst == SKOperationState::FAILED) {
					// these keys are failed with cause
					SKFailureCause::SKFailureCause fc = e.getFailureCause(failKeys->at(ikey));
					fprintf( stdout, "\t key:%s -> state:%d, cause:%d\n", failKeys->at(ikey).c_str(), (int)opst, (int)fc);
				} else {
					if(opst == SKOperationState::INCOMPLETE) {
						// these are timed-out keys
						fprintf( stdout, "\t key:%s -> state:%d\n", failKeys->at(ikey).c_str(), (int)opst);
					} else {
						// these are successfully retrieved keys
						SKStoredValue * pStoredVal = e.getStoredValue(failKeys->at(ikey));
						SKVal* pVal = pStoredVal->getValue();
						fprintf(stdout, "got %s : %s => value: %s\n", nsName, failKeys->at(ikey).c_str(),  (char*)pVal->m_pVal);
						//if (retrievalType==VALUE_AND_META_DATA || retrievalType == META_DATA)
						//    print_meta( SKOpResult::SUCCEEDED, nsName, failKeys->at(ikey).c_str(), pStoredVal);  //FIXME!
						sk_destroy_val(&pVal);
						delete pStoredVal;
					}
				}
			}
		}
		delete failKeys;
	} catch (SKPutException & e) {
		fprintf( stderr, "SKPutException %s %s:%d\n%s\n failed keys are :\n", msg, filename, lineNum, e.getDetailedFailureMessage().c_str());
		SKVector<string> * failKeys = e.getFailedKeys();
		if (failKeys->size() > 0) {
			int nKeys = failKeys->size();
			for(int ikey = 0; ikey < nKeys; ++ikey) {
				SKOperationState::SKOperationState opst = e.getOperationState(failKeys->at(ikey));
				if(opst == SKOperationState::FAILED) {
					// these keys are failed with cause
					SKFailureCause::SKFailureCause fc = e.getFailureCause(failKeys->at(ikey));
					fprintf( stdout, "\t key:%s -> state:%d, cause:%d\n", failKeys->at(ikey).c_str(), (int)opst, (int)fc);
				} else {
					fprintf( stdout, "\t key:%s -> state:%d\n", failKeys->at(ikey).c_str(), (int)opst);
				}
			}
		}
		if(failKeys)
			delete failKeys;

		e.printStackTrace();
		print_stacktrace("exhandler");
	} catch (SKNamespaceCreationException & e) {
		e.printStackTrace();
	} catch (SKSyncRequestException & e) {
		e.printStackTrace();
		fprintf( stderr, "SKSyncRequestException %s %s:%d\n%s\n", msg, filename, lineNum, e.what() );
	} catch (SKSnapshotException & e) {
		e.printStackTrace();
		fprintf( stderr, "SKSnapshotException %s %s:%d\n%s\n", msg, filename, lineNum, e.what() );
	} catch (SKWaitForCompletionException & e) {
		e.printStackTrace();
		fprintf( stderr, "SKWaitForCompletionException %s %s:%d\n%s\n", msg, filename, lineNum, e.what() );
	} catch (SKClientException & e) {
		e.printStackTrace();
		fprintf( stderr, "SKClientException %s %s:%d\n%s\n", msg, filename, lineNum, e.what());
		//exit (-1);
	} catch (exception const& e) {
		print_stacktrace("exhandler");
		fprintf( stderr, "std::exception %s in %s:%d\n%s\n", msg, filename, lineNum, e.what());
		//exit (-1);
	}
}

CmdLineOptions 
Util::parseCmdLine(int argc, char ** argv) {
	CmdLineOptions options;
	return Util::parseCmdLine(argc, argv, options);
}

CmdLineOptions 
Util::parseCmdLineMeta(int argc, char ** argv) {
	CmdLineOptions options;
	options.retrievalType = META_DATA;
	return Util::parseCmdLine(argc, argv, options);
}

CmdLineOptions 
Util::parseCmdLine(int argc, char ** argv, CmdLineOptions& options) {
	string compress;
	string retrieve;
	int c;
	extern char *optarg;
	while ((c = getopt(argc, argv, "g:H:a:n:k:v:F:Vf:c:s:T:K:m:t:rR:i:o:P:J:")) != -1) {
		switch (c) {
			case 'g' :
				options.gcName = optarg;
				break;
			case 'H' :
				options.host = optarg;
				break;
			case 'a' :
				options.action = optarg;
				break;
			case 'n' :
				options.ns = optarg;
				break;
			case 'k' :
				options.key = optarg;
				break;
			case 'v' :
				options.value = optarg;
				break;
			case 'f' :
				options.logfile = optarg;
				break;
			case 'o' :
				options.nsOptions = optarg;
				break;
			case 'J' :
				options.jvmOptions = optarg;
				break;
			case 'c' :
				compress = optarg;
				if(!compress.compare(0, 4, "none"))
					options.compressType = SKCompression::NONE;
				else if(!compress.compare(0, 3, "zip"))
					options.compressType = SKCompression::ZIP;
				else if(!compress.compare(0, 5, "bzip2"))
					options.compressType = SKCompression::BZIP2;
				else if(!compress.compare(0, 6, "snappy"))
					options.compressType = SKCompression::SNAPPY;
				else if(!compress.compare(0, 3, "lz4"))
					options.compressType = SKCompression::LZ4;
				else {
					fprintf(stderr, "Wrong compression argument '%s' \n", optarg);
					usage( argv[0], "wrong compression argument");
					exit(1);
				}
				break;
			case 'm' :
				retrieve = optarg;
				if(!retrieve.compare(0, 19, "VALUE_AND_META_DATA"))
					options.retrievalType = VALUE_AND_META_DATA;
				else if(!retrieve.compare(0, 9, "META_DATA"))
					options.retrievalType = META_DATA;
				else if(!retrieve.compare(0, 5, "VALUE"))
					options.retrievalType = VALUE;
				else if(!retrieve.compare(0, 9, "EXISTENCE"))
					options.retrievalType = EXISTENCE;
				else {
					fprintf(stderr, "Wrong retrieval type argument '%s' \n", optarg);
					usage( argv[0], "wrong retrieval type argument");
					exit(1);
				}
				break;
			case 'i' :
				char * pEnd;
				options.valueVersion = strtoll(optarg, &pEnd, 10) ;
				break;
			case 't':
				options.threshold = atoi(optarg);
				break;
			case 'T' :
				options.timeout = atoi(optarg);
				break;
			case 'V' :
				options.verbose = 1;
				break;
			default :
				fprintf(stderr, "Unknown option '%c'", c);
				usage(argv[0], "unknown option");
				exit(0);
		}
	}

	if (options.gcName.empty())
		usage(argv[0], "missing Grid Configuration Name");
	if (options.host.empty())
		usage(argv[0], "missing Host Name");
	if (options.ns.empty())
		usage(argv[0], "missing namespace");
	
	return options;
}

SKCompression::SKCompression
Util::getCompressionType(string compress) {
  SKCompression::SKCompression compressType = SKCompression::NONE;
  if(!compress.compare(0, 4, "none"))
    compressType = SKCompression::NONE;
  else if(!compress.compare(0, 3, "zip"))
    compressType = SKCompression::ZIP;
  else if(!compress.compare(0, 5, "bzip2"))
    compressType = SKCompression::BZIP2;
  else if(!compress.compare(0, 6, "snappy"))
    compressType = SKCompression::SNAPPY;
  else if(!compress.compare(0, 3, "lz4"))
    compressType = SKCompression::LZ4;
  else {
    string err("Wrong compression arg" + compress);
    throw runtime_error("Wrong compression arg " + compress);
  }
  return compressType;
}

StrValMap
Util::getStrValMap(const vector<string>& ks, const vector<string>& vs) {
  StrValMap vals;
  for(unsigned i = 0; i < ks.size(); ++i) {
    string key(ks[i]);
    string value(vs[i]);
    SKVal* val = sk_create_val();
    sk_set_val(val, value.length(), (void*)value.data());
    vals.insert(StrValMap::value_type(key, val));
  }
  return vals;
}

void
Util::getKeyValues(StrValMap& valMap, const map<string, string>& kvs) {
  map<string, string>::const_iterator it;
  for(it = kvs.begin(); it != kvs.end(); ++it) {
    string key(it->first);
    string value(it->second);
    SKVal* val = sk_create_val();
    sk_set_val(val, value.length(), (void*)value.data());
    valMap.insert(StrValMap::value_type(key, val));
  }

}

StrVector 
Util::getStrKeys(const vector<string>& ks) {
  StrVector keys;
  for(unsigned i = 0; i < ks.size(); ++i) {
    //keys.push_back(string(ks[i]).append("_").append(to_string(i)));
    keys.push_back(string(ks[i]));
  }
  return keys;
}

vector<string>
Util::getValues(StrValMap* vals) {
  vector<string> values;
  StrValMap::const_iterator cit ;
  for(cit = vals->begin(); cit != vals->end(); cit++ ){
    SKVal* pVal = cit->second;
    if( pVal != NULL ){
      values.push_back(string((char*)pVal->m_pVal, pVal->m_len).c_str());
      sk_destroy_val(&pVal);
    }
  }
  return values;
}

vector<string>
Util::getValues(StrSVMap* svMap, SKRetrievalType retrievalType) { 
  vector<string> values;
  StrSVMap::const_iterator cit ;
  for(cit = svMap->begin(); cit != svMap->end(); cit++ ){
    SKStoredValue* pStoredVal = cit->second;
    if(!pStoredVal) {
      cout << "getValues(StrSVMap* svMap, SKRetrievalType retrievalType), got key: " << cit->first << ", missing value" << endl;
      exit(1);
    } else if(retrievalType == VALUE_AND_META_DATA) {
      SKVal* pVal = pStoredVal->getValue();
      if( pVal != NULL ){
        values.push_back(string((char*)pVal->m_pVal, pVal->m_len).c_str());
        sk_destroy_val(&pVal);
      } else {
        cout << "getValues(StrSVMap* svMap, SKRetrievalType retrievalType), key: " << cit->first << ", failed to get value" << endl;
        exit(1);
      }
    } else if(retrievalType == META_DATA) {
      const char* meta = pStoredVal->toString(true);
      values.push_back(meta);
      free((void*) meta);
      meta = NULL;
    } else {
      cout << "getValues(StrSVMap* svMap, SKRetrievalType retrievalType), wrong SKRetrievalType: " << retrievalType << endl;
      exit(1);
    }
    delete pStoredVal;
  }
  return values;
}

map<string, string>
Util::getStrMap(StrValMap* strValMap) {
  map<string, string> strMap;
  StrValMap::const_iterator it ;
  for(it = strValMap->begin(); it != strValMap->end(); it++ ){
    SKVal* pVal = it->second;
    if( pVal != NULL ){
      strMap[it->first] = string((char*)pVal->m_pVal, pVal->m_len);
      sk_destroy_val(&pVal);
    }
  }
  return strMap;
}

map<string, string>
Util::getStrMap(StrSVMap* svMap, SKRetrievalType retrievalType) {
  map<string, string> strMap;
  StrSVMap::const_iterator cit ;
  for(cit = svMap->begin(); cit != svMap->end(); cit++ ){
    SKStoredValue* pStoredVal = cit->second;
    if(!pStoredVal) {
      cout << "getStrMap(StrSVMap* svMap, SKRetrievalType retrievalType), got key: " << cit->first << ", missing value" << endl;
      exit(1);
    } else if(retrievalType == VALUE_AND_META_DATA) {
      SKVal* pVal = pStoredVal->getValue();
      if( pVal != NULL ){
        strMap[cit->first] = string((char*)pVal->m_pVal, pVal->m_len);
        sk_destroy_val(&pVal);
      } else {
        cout << "getStrMap(StrSVMap* svMap, SKRetrievalType retrievalType), key: " << cit->first << ", failed to get value" << endl;
        exit(1);
      }
    } else if(retrievalType == META_DATA) {
      const char* meta = pStoredVal->toString(true);
      strMap[cit->first] = string(meta);
      free((void*) meta);
      meta = NULL;
    } else {
      cout << "getStrMap(StrSVMap* svMap, SKRetrievalType retrievalType), wrong SKRetrievalType: " << retrievalType << endl;
      exit(1);
    }
    delete pStoredVal;
  }
  return strMap;
}

void Util::logElapsedTime(const time_point beginTime_, const std::string & dhtOp_, const std::string & ns_, const std::string & key_) {
	const auto & endTime = high_resolution_clock::now();
	const auto & elapsedMs = duration_cast<milliseconds>(endTime - beginTime_);
	std::stringstream msg;
	msg << dhtOp_ << " took " << elapsedMs.count() << " milliseconds";

	if ( ! ns_.empty() )
	msg << " for ns " << ns_;

	if ( ! key_.empty() )
	msg << " and key " << key_;

	cout << msg.str() << endl;
}
