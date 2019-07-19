#!XXX_PERL_PATH_XXX

##invocation example:
## ./testdht.pl --gcname=GC_xxx_TEST --ns=testpl --host=localhost --key=plHello --val=plWorld --action=put

use strict;
use warnings;
$| = 1;


use Carp;
use Data::Dumper;
use Getopt::Long;
use FindBin;
use lib "$FindBin::Bin/../lib/perl5";
use POSIX;

use SKClientImpl;

my %opt = ();
my @opt_list = qw(host=s ns=s key=s keys=s val=s action=s parent=s gcname=s logfile=s  timeout=i compress=s threshold=i version=i nsopts=s jvmopts=s verbose help);
GetOptions(\%opt, @opt_list) or usage();
usage() if $opt{help};
usage("missing action option") unless $opt{action};
usage("missing namespace") unless $opt{ns};
usage("missing gcname") unless $opt{gcname};

my $defaultTimeout = 24*3600;  ##one day in seconds

my $host     = $opt{host} || 'localhost';
my $waitTimeout = $opt{timeout} || $defaultTimeout;  ##in seconds
my $waitThreshold = $opt{threshold} || 100;  ##in percents
my $vVersion = $opt{version} || -1;
my $action   = $opt{action};
my $gcname   = $opt{gcname};
my $ns       = $opt{ns};
my $verbose  = $opt{verbose} ? $SKClientImpl::LVL_ALL : $SKClientImpl::LVL_OFF;
my $comprstr = $opt{compress} || 'NOCOMPRESSION';
my $compr ;
if($comprstr eq 'NOCOMPRESSION') {
    $compr   = $SKClientImpl::NOCOMPRESSION;
} 
elsif ($comprstr eq 'ZIP') {
    $compr   = $SKClientImpl::ZIP;
} 
elsif ($comprstr eq 'BZIP2') {
    $compr   = $SKClientImpl::BZIP2;
} 
elsif ($comprstr eq 'SNAPPY') {
    $compr   = $SKClientImpl::SNAPPY;
}
elsif ($comprstr eq 'LZ4') {
    $compr   = $SKClientImpl::LZ4;
}
my $nsopts     = $opt{nsopts} || "storageType=RAM,consistencyProtocol=TWO_PHASE_COMMIT,versionMode=SINGLE_VERSION";
##my $nsopts     = $opt{nsopts} || "storageType=RAM,consistencyProtocol=TWO_PHASE_COMMIT,versionMode=CLIENT_SPECIFIED,revisionMode=NO_REVISIONS,defaultPutOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=6,constantTime_ms=60000,itemTime_ms=303,maxRelTimeout_ms=2147483646},compression=LZ4,checksumType=MURMUR3_32,checksumCompressedValues=false,version=-1,},defaultGetOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=6,constantTime_ms=60000,itemTime_ms=303,maxRelTimeout_ms=2147483646},retrievalType=VALUE,waitMode=GET,versionConstraint={min=-9223372036854775808,max=9223372036854775807,mode=GREATEST,maxStorageTime=9223372036854775807},nonExistenceResponse=NULL_VALUE,verifyChecksums=true,forwardingMode=FORWARD,},defaultWaitOptions={opTimeoutController=<WaitForTimeoutController>{internalRetryIntervalSeconds=20},retrievalType=VALUE,waitMode=WAIT_FOR,versionConstraint={min=-9223372036854775808,max=9223372036854775807,mode=GREATEST,maxStorageTime=9223372036854775807},nonExistenceResponse=NULL_VALUE,verifyChecksums=true,forwardingMode=FORWARD,timeoutSeconds=2147483647,threshold=100,timeoutResponse=EXCEPTION},secondarySyncIntervalSeconds=1800,segmentSize=67108864,allowLinks=false"
##my $nsopts     = $opt{nsopts} || "storageType=RAM,consistencyProtocol=TWO_PHASE_COMMIT,versionMode=CLIENT_SPECIFIED,revisionMode=NO_REVISIONS,defaultPutOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=6,constantTime_ms=60000,itemTime_ms=303,maxRelTimeout_ms=2147483646},compression=LZ4,checksumType=MURMUR3_32,checksumCompressedValues=false,version=-1,},defaultGetOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=6,constantTime_ms=60000,itemTime_ms=303,maxRelTimeout_ms=2147483646},retrievalType=VALUE,waitMode=GET,versionConstraint={min=-9223372036854775808,max=9223372036854775807,mode=GREATEST,maxStorageTime=9223372036854775807},nonExistenceResponse=NULL_VALUE,verifyChecksums=true,forwardingMode=FORWARD,},defaultWaitOptions={opTimeoutController=<WaitForTimeoutController>{internalRetryIntervalSeconds=20},retrievalType=VALUE,waitMode=WAIT_FOR,versionConstraint={min=-9223372036854775808,max=9223372036854775807,mode=GREATEST,maxStorageTime=9223372036854775807},nonExistenceResponse=NULL_VALUE,verifyChecksums=true,forwardingMode=FORWARD,timeoutSeconds=30,threshold=100,timeoutResponse=IGNORE},secondarySyncIntervalSeconds=1800,segmentSize=67108864,allowLinks=false"
my $jvmopts     = $opt{jvmopts} || '';


## starts here
my $dhtclient  = SKClientImpl::SKClient::getClient($verbose, $jvmopts);
my $gridConf  = SKClientImpl::SKGridConfiguration::parseFile($gcname);
my $dhtsession = $dhtclient->openSession( $gridConf, $host );

##if ($action !~ m/createns|clone|linkto|deletens|recoverns/ ) { 
if ($action =~ m/createns|clone|linkto|deletens|recoverns/ ) { 
 if($action eq 'createns') {
    my $nsOpt = SKClientImpl::SKNamespaceOptions::parse($nsopts);
    my $nameSpace = $dhtsession->createNamespace($ns, $nsOpt);
    print "created  nameSpace $ns \n";
    undef $nsOpt;
    undef $nameSpace;
 }
 if($action eq 'clone') {
    usage("missing parent") unless $opt{parent};
    my $parentNs = $dhtsession->getNamespace($opt{parent});
    my $nameSpace = 0;
    if(defined $opt{version} ) {
        $nameSpace = $parentNs->clone($ns, $opt{version}) ;
        print "cloned versioned nameSpace into $ns\n";
    }
    else {
        $nameSpace = $parentNs->clone($ns) ;
    }
    undef $nameSpace;
    undef $parentNs;
 }
 if($action eq 'linkto') {
    usage("missing parent") unless $opt{parent};
    my $nameSpace = $dhtsession->getNamespace($ns);
    print "Linking $ns to parent $opt{parent} \n";
    $nameSpace->linkTo($opt{parent});
    print "linked nameSpace $ns \n";
    undef $nameSpace;
 }
 if($action eq 'deletens') {
    $dhtsession->deleteNamespace($ns);
    print "deleted nameSpace $ns \n";
 }
 if($action eq 'recoverns') {
    $dhtsession->recoverNamespace($ns);
    print "recovered nameSpace $ns \n";
 }
 undef $gridConf;
 undef $dhtsession;
 undef $dhtclient;
 print "Namespace op done \n";
 exit 0;
}

my $putOpt = SKClientImpl::SKPutOptions::parse("compression=SNAPPY,checksumType=MURMUR3_32,checksumCompressedValues=false,version=-1");
$putOpt->compression($compr);
$putOpt->checksumType($SKClientImpl::NO_CHECK);
my $namespace1 = $dhtsession->getNamespace($ns);
my $nspOptions = $namespace1->getDefaultNSPOptions();
print "got nspOptions\n";
$nspOptions->defaultPutOptions($putOpt);   ## other options are set similarly
my $waitOpt = SKClientImpl::SKWaitOptions->new();
$waitOpt->timeoutSeconds($waitTimeout);
$waitOpt->threshold($waitThreshold);
$waitOpt->timeoutResponse($SKClientImpl::TIMEOUT_IGNORE);
$waitOpt->retrievalType($SKClientImpl::VALUE_AND_META_DATA);
$nspOptions->defaultWaitOptions($waitOpt); 
print "set waitOpt\n";
undef $namespace1;
undef $gridConf;

if ($action !~ m/async/ ) { 
    ## Synchronous Namespace operations
    my $syncNsp    = $dhtsession->openSyncNamespacePerspective($ns, $nspOptions);
    print "got syncNSP\n";
    if ($action eq 'put')
    {
      usage("missing key") unless $opt{key};
      usage("missing value") unless $opt{val};
        $syncNsp->put( $opt{key}, $opt{val} );
        #$syncNsp->put( $opt{key}, $opt{val} , $putOpt );
      print "put value " . $opt{val} . " " . $opt{val} . " into $ns completed \n";
    }
    if ($action eq 'multi_put')
    {
      my %vals = ();
      #my $keyValueFileName = $opt{keys};
      #if(defined $keyValueFileName ) {
        #getKeyVals( $keyValueFileName , $vals);
      #}
      #else
      #{
          usage("missing key") unless $opt{key};
          usage("missing value") unless $opt{val};
          my $key = $opt{key};
          my $val = $opt{val};
          foreach (0..4)
          {
            my $mkey = $_ ? "$key$_" : $key;
            my $mval = $_ ? "$val$_" : $val;
            $vals{$mkey} = $mval;
            print "prep key $mkey value $mval \n";
          }
      #}
      print "calling multi_put \n";
      $syncNsp->put( \%vals );
      print "multi_put into $ns completed \n";
      #undef $vals;
    }
    elsif ($action eq 'get')
    {
      usage("missing key") unless $opt{key};
      my $gres = $syncNsp->get( $opt{key} );
      if (defined $gres )
      {
        print " key : ". $opt{key}  ." --> value : " ;
        ( length( $gres ) > 0 ) ? print $gres . "\n" : print "<EMPTY>\n";
      }
      else
      {
        print "error getting value for " . $opt{key} . "\n";
      }
      
    }
    elsif ($action eq 'waitfor')
    {
      usage("missing key") unless $opt{key};
      if($waitTimeout!=$defaultTimeout || $waitThreshold!=100 || $vVersion >0 ){
          if($vVersion>0) {
            my $verConstraint = SKClientImpl::SKVersionConstraint::maxBelowOrEqual($vVersion);
            $waitOpt->versionConstraint($verConstraint);
          }
          print "calling syncNsp->waitFor( key , waitOpt) \n";
          my $wres = $syncNsp->waitFor( $opt{key} , $waitOpt);
          if(defined $wres ) {
            my $dhtValue = $wres->getValue();
            print " key : ". $opt{key}  ." --> value : " ;
            ( defined $dhtValue and length($dhtValue) > 0 ) ? print $dhtValue . "\n" : print "<EMPTY>\n";
          }
          else{
            print "error getting value for " . $opt{key} . "\n";
          }
      }
      else {
          my $wres = $syncNsp->waitFor( $opt{key} );
          if (defined $wres )
          {
            print " key : ". $opt{key}  ." --> value : " ;
            ( length( $wres ) > 0 ) ? print $wres . "\n" : print "<EMPTY>\n";
          }
          else
          {
            print "error getting value for " . $opt{key} . "\n";
          }
      }
    }
    elsif ($action eq 'multi_get')
    {
      usage("missing key") unless ((defined $opt{key}) || (defined $opt{keys}) );
      my @keys = @{getKeys( $opt{keys} , $opt{key})}; ##getKeys returns ref
      my $mgres = $syncNsp->get( \@keys );
      if (defined $mgres)
      {
        #print "multi_get result, ", Dumper($mgres);
        for my $i ( 0..4 ) {
            print " key : ". $keys[$i]  ." --> value : " . $mgres->{ $keys[$i] } . " \n";
            ##print " key : ". $keys[$i]  ." --> value : " . $mgres->get( $keys[$i] )->toString() . " \n";
        }
    
      }
      else
      {
        print "error getting values from $ns\n";
      }
    }
    elsif ($action eq 'multi_waitfor')
    {
      usage("missing key") unless ((defined $opt{key}) || (defined $opt{keys}) );
      my @keys = @{getKeys( $opt{keys} , $opt{key})}; ##getKeys returns ref

      if($waitTimeout!=$defaultTimeout || $waitThreshold!=100 || $vVersion >0) 
      {
          print "calling syncNsp->waitFor( key , waitOpt) \n";
          if($vVersion>0) {
            my $verConstraint = SKClientImpl::SKVersionConstraint::maxBelowOrEqual($vVersion);
            $waitOpt->versionConstraint($verConstraint);
          }
          print " waitOpt : " . $waitOpt->toString() . " \n";
          my $mwres = $syncNsp->waitFor( \@keys , $waitOpt);
          if(defined $mwres ) {
            print "multi_waitfor result, ", Dumper($mwres);
            for my $i (0..4)
            {
                my $storedVal = $mwres->get( $keys[$i] );
                if( defined $storedVal ) {
                    print " key : ". $keys[$i]  ." --> value : " . $storedVal->getValue() . " \n";
                    ##print " key : ". $keys[$i]  ." --> value : " . $storedVal->getValue()->toString() . " \n";
                } else {
                    print "error getting value for " . $keys[$i] . "\n";
                }
            }
            undef $mwres;
          }
          else{
            print "error while waiting for values\n";
          }
      }
      else {
          my $mwres = $syncNsp->waitFor( \@keys );
          if (defined $mwres)
          {
            #print "multi_waitfor result: ", Dumper($mwres);
            for my $i ( 0..4 ) {
                print " key : ". $keys[$i]  ." --> value : " . $mwres->{ $keys[$i] } . " \n";
                ##print " key : ". $keys[$i]  ." --> value : " . $mwres->get( $keys[$i] )->toString() . " \n";
            }
            
          }
          else
          {
            print "error getting value for keys: ", Dumper( \@keys );
          }
      }
    }
    elsif ($action eq 'snaphot')
    {
        my $rc = ( $vVersion > 0 ) ? $syncNsp->snapshot($vVersion) : $syncNsp->snapshot() ;
        print "Snaphot op completed.\n";
    }
    elsif ($action eq 'sync')
    {
        $syncNsp->syncRequest();
        print "sync request op completed.\n";
    }
    elsif ($action eq 'get_meta')
    {
      usage("missing key") unless $opt{key};
      my $key = $opt{key};
      my $getOpt = SKClientImpl::SKGetOptions->new($SKClientImpl::VALUE_AND_META_DATA);
      if($vVersion>0) {
        my $verConstraint = SKClientImpl::SKVersionConstraint::maxBelowOrEqual($vVersion);
        $getOpt->versionConstraint($verConstraint);
      }
      my $storedVal = $syncNsp->get( $key, $getOpt );
      if(defined $storedVal)  {
        print " StoredValue key : ". $opt{key} . " --> value : "; 
        printMeta($storedVal);
      }
      else {
        print  "<error getting value> \n";
      }
      undef  $getOpt;
    }
    elsif ($action eq 'multi_get_meta')
    {
      usage("missing key") unless ((defined $opt{key}) || (defined $opt{keys}) );
      my @keys = @{getKeys( $opt{keys} , $opt{key})}; ##getKeys returns ref
      my $getOpt = SKClientImpl::SKGetOptions->new($SKClientImpl::VALUE_AND_META_DATA);
      if($vVersion>0) {
        my $verConstraint = SKClientImpl::SKVersionConstraint::maxBelowOrEqual($vVersion);
        $getOpt->versionConstraint($verConstraint);
      }
      my $mres = $syncNsp->get( \@keys, $getOpt );
      if (defined $mres)
      {
        foreach my $i (0..$mres->size()-1) {
            my $storedVal = $mres->get( $keys[$i] );
            print "key : ". $keys[$i] . " --> value : ";
            if(defined $storedVal)  {
                printMeta($storedVal);
            }
            else {
                print  "<error getting value> \n";
            }
        }
      }
      else
      {
        print "error getting metadata results\n";
      }
    }
    undef $syncNsp;
}
else {
    my $asyncNsp    = $dhtsession->openAsyncNamespacePerspective($ns, $nspOptions);
    print "got asyncNSP\n";
    if ($action eq 'async_put')
    {
      usage("missing key") unless $opt{key};
      usage("missing value") unless $opt{val};
      my $asyncPut    = $asyncNsp->put( $opt{key}, $opt{val} );
      print "async Put op started \n";
      
      ## do any processing here... And then wait for complete :
      $asyncPut->waitForCompletion(1000, $SKClientImpl::MILLISECONDS); 
      ##$asyncPut->waitForCompletion();
      print "async Put op completed with code : " . $asyncPut->getState() . " \n";
      $asyncPut->close(); 
    }
    if ($action eq 'async_multi_put')
    {
      my %vals = ();
      my $keyValueFileName = $opt{keys};
      if($keyValueFileName ) {
        getKeyVals( $keyValueFileName , \%vals);
      }
      else
      {
          usage("missing key") unless $opt{key};
          usage("missing value") unless $opt{val};
          my $key     = $opt{key};
          my $val     = $opt{val};
          foreach (0..4)
          {
            my $mkey  = $_ ? "$key$_" : $key;
            my $mval  = $_ ? "$val$_" : $val;
            $vals{$mkey} = $mval;
          }
      }
      my $asyncPut    = $asyncNsp->put( \%vals );
      print "async Put op started \n";
      ## do any processing here... And then wait for complete :
      $asyncPut->waitForCompletion(); 
      print "async Put op completed with code : " . $asyncPut->getState() . " \n";
      $asyncPut->close(); 
    }
    elsif ($action eq 'async_get')
    {
        usage("missing key") unless $opt{key};
        my $asyncValRetrieve = $asyncNsp->get( $opt{key} );
        if (defined $asyncValRetrieve)
        {
            my $isDone = $asyncValRetrieve->waitForCompletion(100, $SKClientImpl::MILLISECONDS);
            print "asyncValRetrieve op waitForCompletion got : " . $isDone . " \n";
            ##my $res = $asyncValRetrieve->getValues(); my $storedVal = $res->get( $opt{key} );
            my $retVal = $asyncValRetrieve->getValue( );
            print " StoredValue key : ". $opt{key} .  " --> value : " . $retVal . " \n";            
            $asyncValRetrieve->close(); 
        }
        else
        {
            print "error getting value for $opt{key}\n";
        }
    }
    elsif ($action eq 'async_multi_get')
    {
        usage("missing key") unless ((defined $opt{key}) || (defined $opt{keys}) );
        my @keys = @{getKeys( $opt{keys} , $opt{key})}; ##getKeys returns ref
        my $asyncValRetrieve   = $asyncNsp->get( \@keys );
        if (defined $asyncValRetrieve)
        {
            my $isDone = $asyncValRetrieve->waitForCompletion(5, $SKClientImpl::SECONDS);
            print "asyncValRetrieve op waitForCompletion got : " . $isDone . " \n";
            my %res6 = %{$asyncValRetrieve->getValues()};
            #print Dumper($res6);
            foreach my $i (keys %res6) {
                print "  key : ". $i  ." --> value : " . $res6{ $i } . " \n";
            }
            ###the following also works
            #foreach my $i ( @keys ) {
            #    print " getValue key : ". $i  ." --> value : " . $asyncValRetrieve->getValue( $i ) . " \n";
            #}
            $asyncValRetrieve->close(); 
        }
        else
        {
            print "error getting values with async_multi_get\n";
        }
    }
    elsif ($action eq 'async_waitfor')
    {
        usage("missing key") unless $opt{key};
        my $asyncValRetrieve = $asyncNsp->waitFor( $opt{key} );
        if (defined $asyncValRetrieve)
        {
            $asyncValRetrieve->waitForCompletion();
            my $res = $asyncValRetrieve->getValue();
            print " key : ". $opt{key}  ." --> value : " . $res . " \n";
            $asyncValRetrieve->close(); 
        }
        else
        {
            print "error getting value for $opt{key} \n";
        }
    }
    elsif ($action eq 'async_multi_waitfor')
    {
        usage("missing key") unless ((defined $opt{key}) || (defined $opt{keys}) );
        my @keys = @{getKeys( $opt{keys} , $opt{key})}; ##getKeys returns ref
        my $asyncValRetrieve   = $asyncNsp->waitFor( \@keys );
        if (defined $asyncValRetrieve)
        {
            print "asyncWaitFor waitForCompletion\n";
            $asyncValRetrieve->waitForCompletion();
            print "asyncWaitFor getKeys\n";
            my $reskeys6 = $asyncValRetrieve->getKeys();  
            print "asyncWaitFor getValues\n";
            my $res6 = $asyncValRetrieve->getValues();
            #print Dumper($res6);
            foreach my $i ( @keys ) {
                print " getValue key : ". $i  ." --> value : " . $asyncValRetrieve->getValue( $i ) . " \n";
            }
            $asyncValRetrieve->close(); 
        }
        else
        {
        print "error getting values with async_multi_waitfor\n";
        }
    }
    elsif ($action eq 'async_snapshot')
    {
        my $asyncSnapshot = ( $vVersion > 0 ) ? $asyncNsp->snapshot($vVersion) : $asyncNsp->snapshot();
        print "asyncSnaphot : waiting for completion \n";
        if (defined $opt{timeout} )
        {
            my $isDone = $asyncSnapshot->waitForCompletion($opt{timeout}, $SKClientImpl::SECONDS);
            print "asyncSnaphot op waitForCompletion got : " . $isDone . " \n" ;
        }
        else 
        {
            $asyncSnapshot->waitForCompletion();
        }
            
        my $completionState = $asyncSnapshot->getState();
        print "asyncSnaphot op completed with : " . $completionState . " \n";
        print "asyncSnaphot op Failure Cause : " . $asyncSnapshot->getFailureCause() . " \n" if $completionState == $SKClientImpl::OPST_FAILED ;
        $asyncSnapshot->close() ;
    }
    elsif ($action eq 'async_request'){
        my $asyncRequest = ( $vVersion > 0 ) ? $asyncNsp->syncRequest($vVersion) : $asyncNsp->syncRequest();
        print "asyncSyncRequest op : waiting for completion \n";
        if(defined $opt{timeout} )
        {
            my $isReqDone = $asyncRequest->waitForCompletion($opt{timeout}, $SKClientImpl::SECONDS);
            print "asyncSnaphot op waitForCompletion got : " . $$isReqDone  . " \n" unless $isReqDone ;
        }
        else
        {
            $asyncRequest->waitForCompletion();
        }
        my $reqComplitionState = $asyncRequest->getState();
        print "asyncSyncRequest op completed with : " . $reqComplitionState . " \n";
        print "asyncSyncRequest op Failure Cause : " . $asyncRequest->getFailureCause() . " \n" if $reqComplitionState == $SKClientImpl::OPST_FAILED ;
        $asyncRequest->close() ;
    }
    elsif ($action eq 'async_mget_meta')
    {
        usage("missing key") unless ((defined $opt{key}) || (defined $opt{keys}) );
        my @keys = @{getKeys( $opt{keys} , $opt{key})}; ##getKeys returns ref
        my $getOpt = 0 ;
        if(defined $opt{version} &&  $opt{version} > 0) {
            my $verConstraint = SKClientImpl::SKVersionConstraint::maxBelowOrEqual($vVersion);
            $getOpt = SKClientImpl::SKGetOptions->new($SKClientImpl::VALUE_AND_META_DATA, $verConstraint);
            undef $verConstraint;
        }
        else {
            $getOpt = SKClientImpl::SKGetOptions->new($SKClientImpl::VALUE_AND_META_DATA);
        }
        my $asyncRetrieve = $asyncNsp->get( \@keys, $getOpt );
        if (defined $asyncRetrieve)
        {
            if (defined $opt{timeout} ) {
                my $isDone = $asyncRetrieve->waitForCompletion($opt{timeout}, $SKClientImpl::SECONDS);
                print "asyncRetrieve op waitForCompletion got : " . $isDone . " \n";
            } else { 
                $asyncRetrieve->waitForCompletion();
            }
            my $res = $asyncRetrieve->getStoredValues();
            if (defined $res)
            {
                foreach my $i (0..$res->size()-1) {
                    my $storedVal = $res->get( $keys[$i] );
                    print "key : ". $keys[$i] . " --> value : ";
                    if(defined $storedVal)  {
                        printMeta($storedVal);
                    }
                    else {
                        print  "<error getting value> \n";
                    }
                }
            }
            else
            {
                print "error getting metadata for keys". Dumper(@keys) ."\n";
            }
            $asyncRetrieve->close(); 
            undef $asyncRetrieve;
            undef $getOpt;
        }
        else
        {
            print "error creating async retrieval operation\n";
        }
    }
    undef $asyncNsp;

}

sub printMeta {
    my ($storedVal) = @_;
    my $printWithLabels = 1;
    my $retVal =  $storedVal->getValue();
    (defined $retVal) ? print $retVal . " \n" : print "<EMPTY>\n";

    ## print formatted string:
    print $storedVal->toString($printWithLabels) ."\n";
    
    ##alternative approach below. verify correct print of creation times 
    #my $BirthOffset = 946699200 ;##to 2000/01/01 11:00:00 (start time is 11:00, it seems)
    #print "\tCreation Time :" .  scalar localtime($storedVal->getCreationTime()/1000000000 + $BirthOffset) . " + " . ($storedVal->getCreationTime()%1000000000) / 1000000 . " ms\n";
    #print "\tStored Length : ". $storedVal->getStoredLength() ."\n";
    #print "\tLength        : ". $storedVal->getUncompressedLength() ."\n";
    #print "\tVersion       : ". $storedVal->getVersion() ."\n";
    #print "\tCreation Time : ". $storedVal->getCreationTime() ."\n";
    #print "\tCompression   : ". $storedVal->getCompression() ."\n";
    #print "\tChecksumType  : ". $storedVal->getChecksumType() ."\n";
    #print "\tCreator PID   : ". $storedVal->getCreator()->getID() ."\n";
    ###print "\tCreator IP    : ". $storedVal->getCreator()->getIP() ."\n";
    #my $userDt = $storedVal->getUserData();
    #print "\tUser Data     : "; 
    #(defined $userDt and length($userDt)>0) ? print $userDt ."\n" : print "<EMPTY>\n";
    #my $cksm = $storedVal->getChecksum();
    #print "\tChecksum      : ";
    #(defined $ckcm and length($cksm)>0 ) ? print unpack ("H*", $cksm) ."\n" : print "<EMPTY>\n" ;
}

sub getKeys                                 
{
    my ($keysFileName, $key) = @_;
    my $keys = []; ## array ref
    if ($keysFileName) {
      open(KEYSFILE, "<$keysFileName") or die "Can't open keys file: $keysFileName!\n";
      while (my $k = <KEYSFILE>)
      {
        chomp($k);
        push(@$keys,$k);
      }
    }
    elsif (defined $key) {
        foreach (0..4)
        {
            my $mkey = $_ ? "$key$_" : $key;
            push(@$keys,$mkey);
        }
    }
    return $keys;
}


sub getKeyVals
{
    my ($keyValueFileName, %vals) = @_;
    if ($keyValueFileName)
      {
        open(KEYSFILE, "<$keyValueFileName") or die "Can't open tab-separated keys-values file: $            !\n";

        while (my $k = <KEYSFILE>)
        {
            chomp($k);
            my @kv = split('\t', $k, 2);
            $vals{$kv[0]} = $kv[1];
        }
      }
    else
    {
       usage("missing keys.  need key or keys option");
    }

}

sub usage
{
  my $msg = shift;
  (my $name = $0) =~ s{.*/}{};

  print $msg, "\n" if $msg;
  print STDERR <<EOS;
Usage: $name [OPTIONS]
test DHT Perl client

OPTIONS:
  --help            print this help information
  --host=HOSTNAME   DHT node hostname (localhost)
  --action=ACTION   action string (put, multi_put, get, waitfor, multi_get, multi_waitfor, get_meta, multi_get_meta, sync, snapshot, async_get, async_multi_get, async_multi_waitfor, async_mget_meta, async_sync, async_snapshot, createns, clone, linkto, deletens, recoverns)
  --ns=STRING       namespace
  --key=STRING      key
  --keys=FILE       file is optionally used to provide keys for multi_* functions, one per line.
  --val=STRING      value
  --parent=STRING   parent namespace
  --gcname=STRING   Grid Configuration name
  --timeout=NUMBER  timeout in seconds
  --threshold=NUMBER Min percentage of values to be returned by multi-value ops
  --compress=TYPE   compression type, one of NOCOMPRESSION, ZIP, BZIP2, SNAPPY, LZ4 
  --version=NUMBER  version of vlue to retrieve
  --nsopts=STRING   NameSpace options, default: storageType=RAM,consistencyProtocol=TWO_PHASE_COMMIT,versionMode=SINGLE_VERSION
  --jvmopts=STRING  jvm options
  --logfile=STRING  log file
  --verbose         be verbose when logging
EOS
  exit 1;
}

undef $nspOptions;    
undef $dhtsession;
undef $dhtclient;
##SKClientImpl::SKClient::shutdown();
print "all done\n\n";

