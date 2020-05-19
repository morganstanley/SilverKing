package com.ms.silverking.text.test;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.client.SecondaryTargetType;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.text.ObjectDefParser2;

public class ObjectDefParserTest {
  public ObjectDefParserTest() {
  }

  public void test(String def) {
    //NamespaceOptions                    options;
    //ObjectDefParser<NamespaceOptions>   parser;
    //ObjectDefParser<PutOptions>   parser;
    ImmutableSet<String> optionalFields;

    //options = new NamespaceOptions(StorageType.RAM, ConsistencyProtocol.TWO_PHASE_COMMIT, NamespaceVersionMode
    // .CLIENT_SPECIFIED, null);
    optionalFields = ImmutableSet.of();
    //parser = new ObjectDefParser<>(NamespaceOptions.class, FieldsRequirement.ALLOW_INCOMPLETE, optionalFields);
    //parser = new ObjectDefParser<>(PutOptions.class, FieldsRequirement.ALLOW_INCOMPLETE, optionalFields);
    //System.out.println("result: "+ parser.parse(def));
    System.out.println("result: " + ObjectDefParser2.parse(NamespaceOptions.class, def));
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      ObjectDefParserTest test;
      String def;
      NamespaceOptions nsOptions;

      DHTUtil.currentTimeMillis();

      System.out.println();

      System.out.println("\n\n=================================\n\n");
      //System.out.println(DHTConstants.standardPutOptions);
      //System.out.println(DHTConstants.standardGetOptions);
            

            /*
            try {
                NamespaceOptions.parse("");
            } catch (Exception e) {
            }
            try {
                def = new SecondaryTarget(SecondaryTargetType.AncestorClass, "").toString();
            } catch (Exception e) {
            }
            */

      new SecondaryTarget(null, null);
      def = "defaultPutOptions={secondaryTargets={{type=AncestorClass,target=Campus}}}";
      nsOptions = ObjectDefParser2.parse(NamespaceOptions.class, def);
      System.exit(0);

      def = new NamespaceOptions(DHTConstants.defaultStorageType, DHTConstants.defaultConsistencyProtocol,
          DHTConstants.defaultVersionMode, DHTConstants.defaultRevisionMode,
          DHTConstants.standardPutOptions.secondaryTargets(
              new SecondaryTarget(SecondaryTargetType.AncestorClass, "Region")),
          DHTConstants.standardInvalidationOptions, DHTConstants.standardGetOptions, DHTConstants.standardWaitOptions,
          0, 0, 0, false).toString();
            /*
            def = new NamespaceOptions(DHTConstants.defaultStorageType, DHTConstants.defaultConsistencyProtocol, 
                    DHTConstants.defaultVersionMode, DHTConstants.standardPutOptions,
                    DHTConstants.standardGetOptions, DHTConstants.standardWaitOptions).toString();
                    */
      System.out.println(def);
      System.out.println();

      nsOptions = ObjectDefParser2.parse(NamespaceOptions.class, def);
      System.out.println(nsOptions);
      for (SecondaryTarget target : nsOptions.getDefaultPutOptions().getSecondaryTargets()) {
        System.out.println(target);
      }

      //test = new ObjectDefParserTest();
      //test.test(def);
            
            /*
            for (String arg : args) {
                test.test(arg);
            }
            */
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
