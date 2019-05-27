#ifndef SKGRIDCONFIGURATION_H
#define SKGRIDCONFIGURATION_H

#include <string>
#include <map>
using std::string;
using std::map;

#include "skconstants.h"
#include "SKClientDHTConfigurationProvider.h"

//class GridConfiguration;

class SKGridConfiguration : public SKClientDHTConfigurationProvider 
{
public:
    SKAPI static SKGridConfiguration * parseFile(const char * gcBase, const char * gcName); //1st arg was File
    SKAPI static SKGridConfiguration * parseFile(const char * gcName);
    SKAPI static map<string, string> * readEnvFile(const char * envFile);   //the arg was File

    SKAPI SKGridConfiguration(const char * name, map<string,string> * envMap); 

    /* Config Name; client responsible for freeing string */
    SKAPI char * getName();      

    /* Env variable ;  client responsible for freeing input and output strings */
    SKAPI char * get(const char * envKey);

    /*String representation of Grid Config;  client responsible for freeing of output string */
    SKAPI char * toString() ; 
    
    /* Returns map of env. variables; client responsible for freeing the returned object */
    SKAPI map<string,string> * getEnvMap();

    SKAPI virtual SKClientDHTConfiguration * getClientDHTConfiguration();

    SKAPI ~SKGridConfiguration();

    SKGridConfiguration(void * pGridConfiguration);
};

#endif // SKGRIDCONFIGURATION_H
