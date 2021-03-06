/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package de.mpii.fsm.driver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Parameters;

import de.mpii.fsm.util.Constants;
import de.mpii.fsm.util.Dictionary;


/**
 *-------------------------------------------------------------------------------------
 * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
 *------------------------------------------------------------------------------------- 
 * Utilizes the org.apache.mahout.commmon.Parameters and
 * org.apache.mahout.common.AbstractJob for running the 
 * MG-FSM algorithm according to the user specified parameters.
 * 
 * Argument List :
 * 
 *   1. --support (-s)     (Optional) The minimum number of times the 
 *  	 				             sequence to be mined must be present in the Database
 *             	          (minimum support)
 *        		             Default Value: 1
 *         
 *   2. --gamma   (-g) 	  (Optional) The maximum amount of gap that can be 
 *                    		 taken for a sequence to be mined by MG-FSM,
 *                    		 Default Value: 2
 *                       
 *   3. --lambda  (-l) 		(Optional) The maximum length of the sequence 
 *  				   		         to be mined is determined by the this parameter.
 *                    		 Default Value: 5 
 *                     
 *   4. --execMode(-m)     (Optional) Method of execution 
 *                         viz. (s)equential or (d)istributed 
 *                         Default Value: (s)equential    
 *                         
 *   5. --type      (-t)   (Optional) Specify the output type.
 *                         Expected values for type:
 *                         1. (a)ll 2. (m)aximal 3. (c)losed
 *                         Default Value : (a)ll
 *          
 *   6. --keepFiles (-k)   (Optional) Keep the intermediary files for later 
 *  						           use or runs. The files stored are: 
 *  						           1. Dictionary 2. Encoded Sequences
 *     		                      
 *   7. --resume    (-r)   (Optional) Resume running further runs of 
 *  						           the MG-FSM algorithm on already encoded transaction
 *  						           file located in the folder specified in input.                    
 *                      
 *   8. --input     (-i)   (Optional) Path where the input transactions / database
 *  						           text file is located.
 *  
 *   9. --output    (-o)    Path where the output files are to written.
 *  
 *  10. --tempDir   (-tempDir) (Optional) Specify the temporary directory to be 
 *                              used for the map--reduce jobs.
 *                              
 * 	11. --numReducers (-N)  (Optional) Number of reducers to be used by MG-FSM.
 * 							Default value : 90      
 * 
 *  12. --timestampInput (-ti) (Optional) Specify whether you would like to use a timestamp-encoded input format like:
 *  							seqId timestamp1 item1 timestamp2 item2 timestampN itemN
 *  
 *  13. --temporalGap (-tg) (Optional, required when using -ti) Specify a temporal maximum gap for timestampInput
 *  
 *-------------------------------------------------------------------------------------  
 *  References :
 *-------------------------------------------------------------------------------------
 *  [1] Miliaraki, I., Berberich, K., Gemulla, R., & Zoupanos, 
 *      S. (2013). Mind the Gap: Large-Scale Frequent Sequence Mining.
 *-------------------------------------------------------------------------------------
 *  Notes :
 *-------------------------------------------------------------------------------------   
 *  1. -r and -k are mutually exclusive.    
 *  2. -i and -r are mutually exclusive.
 *  3. Only -(o)utput is the compulsory option. All other are options are optional.  
 *-------------------------------------------------------------------------------------
 */
public final class FsmDriver extends AbstractJob {

  //  private static final Logger log = LoggerFactory.getLogger(FsmDriver.class);

  //Use this configuration object
  //to communicate between every 
  //class
  public FsmConfig commonConfig;


  /* Empty Constructor */
  public FsmDriver() {
    commonConfig = new FsmConfig();
  }

  /**
   * (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   * 
   * Add the appropriate options here. Execute the MG-FSM algorithm 
   * according to the parameters specified at run time.
   * 
   * @param String[] args 
   * @return int
   */
  @Override
  public int run(String[] args) throws Exception {      
    /* Here parameters that will be available to the user 
     * during run time are specified and intialized. */

    /* Hadooop-config options */
    addOutputOption();
    addInputOption();
    
    /*User-interesting options*/
    /* - replaced by hadoop default option
     * addOption("input",
              "i",
              "(Optional) Specify the path from where the input is to be read"
              + "\n NOTE: This option can not be used with -(r)esume option.",
              null);*/
    
    addOption("support", 
              "s", 
              "(Optional) Minimum support (sigma) " 
              + "\nDefault Value: 1\n", 
              FsmConfig.SIGMA_DEFAULT_STRING);

    addOption("gamma", 
              "g", 
              "(Optional) Maximum allowed for mining frequent sequences (gamma)" +
              " by MG-FSM " + "\nDefault Value: 2\n", 
              FsmConfig.GAMMA_DEFAULT_STRING);

    addOption("lambda", 
              "l",
              "(Optional) Maximum length for mining frequent sequences (lambda)"  +
              "\nDefault Value: 5\n", 
              FsmConfig.LAMBDA_DEFAULT_STRING);

    addOption("execMode",
              "m",
              "Method of execution viz. s -(s)equential or d -(d)istributed" +
              "\nDefault Value: (s)-sequential\n", 
              FsmConfig.DEFAULT_EXEC_MODE);

    addOption("type",
              "t", 
              "(Optional) Specify the mining mode."+
              "\nExpected values for input:" +
              "\n1. a -(a)ll\n2. m -(m)aximal \n3. c -(c)losed" +
              "\nDefault Value : a -(a)ll\n",
              FsmConfig.DEFAULT_TYPE);

    /* keepEncoded default value is null.
     * It will be set to the output location in case
     * no path is specified.*/
    addFlag("keepEncoded", "k", "Specify whether to store the intermediary files for later use or runs. If no parameter is passed, the intermediary files are stored in a sub directory of the output folder. If a path is passed as a parameter, the files will be stored at the passed path. The files stored are: 1. Dictionary 2. Encoded Sequences");
    
    addOption("keepEncodedPath", 
            "kp", 
            "Specify where to store the encoded intermediary files.");

    /* resume points to the location where the 
     * intermediary files are located*/
    addOption("resume", "r", "(Optional) Resume running further "
        + "runs of the MG-FSM algorithm on"
        + " already encoded transaction file located in the folder specified in input.\n",
          null);
    

    /* Only do the mining */
    addFlag("mineOnly", "mo", "If this flag is given, only the encoding phase is executed. MG-FSM then stores the dictionary and the encoded sequences in the specified output folder.");
    
    /* Only do the encoding */
    addFlag("encodeOnly", "eo", "If this flag is given, only the mining phase is executed. MG-FSM then uses the dictionary and the encoded sequences at the specified input folder and mines the sequences with the given parameters.");
    
    /* timestampInput for reading timestamp-encoded input files */
    addFlag("timestampInput", "ti", "(Optional) Specify whether to use the timestamp-encoded input format like this:"
    			+ "\nseqId timestamp1 item1 timestamp2 item2 timestampN itemN");
    
    addOption("temporalGap", 
              "tg", 
              "(Optional, required when using -ti) Specify a temporal maximum gap for timestampInput. \n"
              + "The gap will be converted to a gamma value internally");

    /*Developer-interesting options*/
    addOption("partitionSize", "p",
              "(Optional) Explicitly specify the partition size." 
            + "\nDefault Value: 10000", 
              FsmConfig.DEFAULT_PARTITION_SIZE);
    
    addOption("indexing", "id", "(Optional) Specify the indexing mode."
            + "\nExpected values for input:"
            + "\n1. none\n2. minmax \n3. full" 
            + "\nDefault Value : full\n", 
              FsmConfig.DEFAULT_INDEXING_METHOD);

    /* split flag is false by default*/
    addFlag("split", "sp", "(Optional) Explicitly specify "
          + "whether or not to allow split by setting this flag.");

    
    addOption("numReducers", "N", "(Optional) Number of reducers to be used by MG-FSM. Default value: 90 ", "90");
    
    addOption("miningAlgorithm", "ma", "(Optional) Specify the mining algorithm. Possible values: bfs, dfs, psm", "bfs");

    /*------------------------------------------------------------
     * ERROR CHECKS
     *------------------------------------------------------------*/

    /* Parse the arguments received from 
     * the user during run-time.*/
    if (parseArguments(args) == null) {
      System.out.println("\n------------\n"+
                         " E R R O R " +
                         "\n------------\n");
      System.out.println("One of the mandatory options is NOT specified");
      System.out.println("e.g. the output and input options MUST be specified.");
      //Return a non-zero exit status to indicate failure
      return 1;
    }

    Parameters params = new Parameters();
    if(hasOption("tempDir")){
      String tempDirPath = getOption("tempDir");
      params.set("tempDir", tempDirPath);
    }
    if (hasOption("support")) {
      String supportString = getOption("support");
      /* 
       * Checks & constraints on the value that can
       * be assigned to support, gamma, & lambda.
       * 
       * NOTE: refer [1]
       */
      if(Integer.parseInt(supportString) < 1) {
        System.out.println("Value of support should be greater than or equal to 1");
        //Return a non-zero exit status to indicate failure
        return (1);
      }
      params.set("support", supportString);

    }
    if (hasOption("gamma")) {
      String gammaString = getOption("gamma");

      if(Integer.parseInt(gammaString) < 0)
      {
        System.out.println("Value of gap should be greater than or equal to 0");
        //Return a non-zero exit status to indicate failure
        return (1);
      }
      params.set("gamma", gammaString);
    }
    if (hasOption("lambda")) {
      String lambdaString = getOption("lambda");

      if(Integer.parseInt(lambdaString) < 2) {
        System.out.println("Value of length should be greater than or equal to 2");
        //Return a non-zero exit status to indicate failure
        return (1);
      }
      params.set("lambda", lambdaString);
    }  
    if(hasOption("mineOnly")) {
        params.set("mineOnly", "true");
      } 
    if(hasOption("encodeOnly")) {
        params.set("encodeOnly", "true");
        commonConfig.setEncodeOnlyOption(true);
      } 
    if(hasOption("execMode")){
      String modeString = getOption("execMode");
      params.set("execMode", modeString);
    }
    if(hasOption("type")){
      String modeString = getOption("type");
      params.set("type", modeString);
    }
    if(hasOption("indexing")){
      String indexingString = getOption("indexing");
      params.set("indexing", indexingString);
    }
    if(hasOption("partitionSize")) {
      String partitionString = getOption("partitionSize");
      params.set("partitionSize", partitionString);
    }
    if(hasOption("split")) {
      params.set("split", "true");
    }
    else {
      params.set("split", "false");
    }
    if (hasOption("keepEncoded")) {
      params.set("keepEncoded", "true");
      commonConfig.setKeepEncodedOption(true);
    } 
    else {
      params.set("keepEncoded", null);
    }
    if (hasOption("keepEncodedPath")) {
      String keepEncodedString = getOption("keepEncodedPath");
      params.set("keepEncodedPath", keepEncodedString);
    } 
    else {
      params.set("keepEncodedPath", null);
    }
    if(hasOption("timestampInput")) {
      params.set("timestampInput", "true");
    } 
    else {
      params.set("timestampInput", "false");
    }
    if (hasOption("temporalGap")) {
	    String temporalGapString = getOption("temporalGap");
	
	    if(Integer.parseInt(temporalGapString) < 0)
	    {
	      System.out.println("Value of temporal gap should be greater than or equal to 0");
	      //Return a non-zero exit status to indicate failure
	      return (1);
	    }
	    params.set("temporalGap", temporalGapString);
	  }
    
    if(hasOption("numReducers")){
    	String numReducersString = getOption("numReducers");
    	params.set("numReducers", numReducersString);
    } else {
    	params.set("numReducers", null);
    }
    
    if(hasOption("miningAlgorithm")){
    	String miningAlgorithmString = getOption("miningAlgorithm");
    	params.set("miningAlgorithm", miningAlgorithmString);
    } else {
    	params.set("miningAlgorithm", "bfs");
    }
    
    
    Path inputDir  = getInputPath();
    Path outputDir = getOutputPath();
    
    /* ---------------------------------------------------------------------
     * ERROR CHECKS ON COMBINATION OF OPTIONS SUPPLIED TO THE DRIVER
     * --------------------------------------------------------------------*/
    
     //Complain if the '-(t)ype' is equal to '-(m)aximal' or '-(c)losed' and 
     //the 'tempDir' is not specified
     /*if((params.get("tempDir")==null||params.get("tempDir").contentEquals("temp"))&&
        ((params.get("type").toCharArray()[0]=='m')||(params.get("type").toCharArray()[0]=='c'))){
       System.out
          .println("If -(t)ype is -(m)aximal or -(c)losed then a -tempDir path must be specified");
     }*/
     if((params.get("mo")!=null)&&(params.get("keepEncoded")!=null)){
       System.out.println("-(m)ine(O)nly & -(k)eepEncoded are mutually exclusive options");
       System.out.println("Exiting...");
       //Return a non-zero exit status to indicate failure
       return (1);
     }
     if((params.get("timestampInput")!="false") && (params.get("temporalGap")==null)){
         System.out.println("No temporalGap (-tg) specified despite using timestampInput (-ti). Please specify a temporal gap.");
         System.out.println("Exiting...");
         //Return a non-zero exit status to indicate failure
         return (1);
     }
     if((params.get("mineOnly")=="true") && (params.get("encodeOnly")=="true")) {
         System.out.println("You set both the flags encodeOnly and mineOnly. Please remove at least one of the flags. If you wish to do both encoding and mining, please remove both flags.");
         System.out.println("Exiting...");
         //Return a non-zero exit status to indicate failure
         return (1);
     }
    /* ---------------------------------------------------------------------
     * Checks to make sure the i/o paths
     * exist and are consistent.
     * --------------------------------------------------------------------
     */
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    //If the output paths exist clean them up
    if(fs.exists(outputDir)){
      System.out.println("Deleting existing output path");
      fs.delete(outputDir, true);
    }
    //Create the necessary output paths afresh now
    fs.mkdirs(outputDir);
    
    //Complain if the input path doesn't exist
    if(!fs.exists(inputDir)){
      System.out.println("\n------------\n"+
          " E R R O R " +
          "\n------------\n");

      System.out.println("Input path does not exist OR input option not specified. Exiting...");
      //Return a non-zero exit status to indicate failure
      return (1);
    }
    
    if(inputDir.toString().compareTo(outputDir.toString()) == 0){
      System.out.println("\n------------\n"+
                         " E R R O R " +
                         "\n------------\n");

      System.out.println("The input and output path can NOT be same."
          + "\nThe output path is deleted prior to running the Hadoop jobs."
          + "\nHence, the input would be also deleted if paths are same."
          + "\nExiting...");
      //Return a non-zero exit status to indicate failure
      return (1);
    } 
    
    params.set("input",  inputDir.toString());
    params.set("output", outputDir.toString());

    /*---------------------------------------------------------------------
     * END OF ERROR CHECKS
     * --------------------------------------------------------------------*/
    
    /* Execute the FSM Job depending upon the parameters specified. */ 
    String executionMethod    = getOption("execMode");
    
    // Set the mine/encoding only options in the commonConfig
    //Also, set the intermediateOutput path accordingly.
    if(params.get("mineOnly")!=null)
      commonConfig.setMineOnlyOption(true);
    else
      commonConfig.setMineOnlyOption(false);
    
    if(params.get("encodeOnly")!=null)
        commonConfig.setEncodeOnlyOption(true);
      else
        commonConfig.setEncodeOnlyOption(false);
    
    if(params.get("keepEncoded")!=null || params.get("encodeOnly")!=null){
      commonConfig.setKeepEncodedOption(true); 
      
      Path intermediateDir;
      // If the user specified a specific path, store the encoded output there.
      if(params.get("keepEncodedPath")!=null) {
    	  intermediateDir = new Path(params.get("keepEncodedPath"));
      }
      // If it is encode only, store it at the root level
      else if(params.get("encodeOnly")!=null) {
    	  intermediateDir = new Path(outputDir.toString());
      }
      // Otherwise, store it in the /encoded/
      else {
    	  intermediateDir = new Path(outputDir+"/encoded/");
      }
     
      if(fs.exists(intermediateDir)){
    	  fs.delete(intermediateDir, true);
      }
      commonConfig.setIntermediatePath(intermediateDir.toString());
    }
    else {
      File intermediateOutputPath = File.createTempFile("MG_FSM_INTRM_OP_", "");
      
      //Below JDK 7 we are only allowed to create temporary files.
      //Hence, turn the file into a directory in temporary folder.
      intermediateOutputPath.delete();
      intermediateOutputPath.mkdir();
      
      commonConfig.setIntermediatePath(intermediateOutputPath.getAbsolutePath().toString());
      
      System.out.println("The intermediate output will be written \n"
                          + "to this temporary path :"
                          + intermediateOutputPath);
      
      commonConfig.setKeepEncodedOption(false);
    }
    
    //Set the 'tempDir' if its null
    if(params.get("tempDir")==null || params.get("tempDir").contentEquals("temp")){
     
      File tempOutputPath = File.createTempFile("MG_FSM_TEMP_OP_", "");
      
      
      
      tempOutputPath.delete();
      //tempOutputPath.mkdir();
      
      
      commonConfig.setTmpPath(tempOutputPath.getAbsolutePath().toString());
      
      System.out.println("The temporary output associated with the internal map -reduce\n"
                          + "jobs will be written to this temporary path :"
                          + commonConfig.getTmpPath());
    }
    else{
          commonConfig.setTmpPath(params.get("tempDir"));
    }
    
    //Set the input and output paths of the commonConfig
    commonConfig.setInputPath(params.get("input"));
    commonConfig.setOutputPath(params.get("output"));
    commonConfig.setDictionaryPath(commonConfig
                                  .getIntermediatePath()
                                  .concat("/"+Constants.OUTPUT_DICTIONARY_FILE_PATH));
    commonConfig.setMaximumFrequencyPath(commonConfig
    							  .getIntermediatePath()
    							  .concat("/"+Constants.MAXIMUM_FREQUENCY_FILE_PATH));
    
    
    //Supply the rest of the algorithm specific options to commonConfig
    commonConfig.setSigma(Integer.parseInt(params.get("support")));
    commonConfig.setGamma(Integer.parseInt(params.get("gamma")));
    commonConfig.setLambda(Integer.parseInt(params.get("lambda")));

    commonConfig.setPartitionSize(Long.parseLong(params.get("partitionSize")));
    commonConfig.setAllowSplits(Boolean.parseBoolean(params.get("splits")));
    commonConfig.setTimestampInputOption(Boolean.parseBoolean(params.get("timestampInput")));
    
    if(params.get("temporalGap") != null) {
    	commonConfig.setTemporalGap(Integer.parseInt(params.get("temporalGap")));
    }
    
    if(params.get("numReducers") != null){
    	commonConfig.setNumberOfReducers(Integer.parseInt(params.get("numReducers")));
    }
    

    switch(params.get("type").toCharArray()[0]){
    case 'a': { commonConfig.setType(FsmConfig.Type.ALL);break; }
    case 'm': { commonConfig.setType(FsmConfig.Type.MAXIMAL); break;}
    case 'c': { commonConfig.setType(FsmConfig.Type.CLOSED); break;}
    default : { commonConfig.setType(FsmConfig.Type.ALL); break;}
    }

    switch(params.get("indexing").toCharArray()[0]){
    case 'n': { commonConfig.setIndexingMethod(FsmConfig.IndexingMethod.NONE); break; }
    case 'm': { commonConfig.setIndexingMethod(FsmConfig.IndexingMethod.MINMAX); break;}
    case 'f': { commonConfig.setIndexingMethod(FsmConfig.IndexingMethod.FULL); break;}
    default : { commonConfig.setIndexingMethod(FsmConfig.IndexingMethod.FULL); break;}
    }
    
    
    // Config debug
    if(commonConfig.isEncodeOnlyOption())
    	System.out.println("Encode only.");
    if(commonConfig.isMineOnlyOption())
    	System.out.println("Mine only.");
    if(commonConfig.isKeepEncodedOption())
    	System.out.println("Keep Encoded");
    System.out.println("Intermediate Path: " + commonConfig.getIntermediatePath());
    
    
    //SEQUENTIAL EXECUTION MODE
    
    
    if ("s".equalsIgnoreCase(executionMethod)) {    	
      SequentialMode mySequentialMiner;
      
      mySequentialMiner = new SequentialMode(commonConfig);
      
      // Step 1: Construct the dictionary and encode sequences
      if(!commonConfig.isMineOnlyOption()) {
    	System.out.println("#### ENCODE");
        mySequentialMiner.createDictionary(commonConfig.getInputPath());
        mySequentialMiner.createIdToItemMap(); 
        
        mySequentialMiner.encodeFiles();
      }
      
      // MINING
      // If the user specified to mine the dataset, run the mining
      if(!commonConfig.isEncodeOnlyOption()) {
    	  System.out.println("#### MINE");
    	  
    	// Read encoded files (if we are using pre-encoded files)
    	if(commonConfig.isMineOnlyOption()) {
    		// Test dictionary reading
    		/*
    		System.out.println("====> Testing dic read: classic");
    		Map<Integer, String> dict = new Dictionary().readDictionary(commonConfig.getInputPath().concat("/"+Constants.OUTPUT_DICTIONARY_FILE_PATH));
    		for(Map.Entry<Integer, String> entry : dict.entrySet()) {
    			System.out.println(entry.getKey() + "\t" + entry.getValue());
    		}
    		
    		System.out.println("====> Testing dic read: json");
    		dict = new Dictionary().readJSONDictionary(commonConfig.getInputPath().concat("/"+Constants.OUTPUT_DICTIONARY_FILE_PATH+".json"));
    		for(Map.Entry<Integer, String> entry : dict.entrySet()) {
    			System.out.println(entry.getKey() + "\t" + entry.getValue());
    		}
    		
    		System.out.println("==>Testing finished.");
    		System.exit(0);
    		*/
    		
    		mySequentialMiner.setIdToItemMap(new Dictionary()
                                        .readJSONDictionary(commonConfig
                                                       .getInputPath()
                                                       .concat("/"+Constants.OUTPUT_DICTIONARY_FILE_PATH+".json")));
    	}
    	
    	// If using timestampInput, calculate gamma from temporal gap and the maximum frequency
    	if(commonConfig.isTimestampInputOption()) {
    		int tg = commonConfig.getTemporalGap();
    		/*int mf = 0;
    		
    		// If we are using pre-encoded files, read the maximum frequency from the maxFreq file
    		 if(commonConfig.isMineOnlyOption()){
        		String mfPath = commonConfig.getInputPath().concat("/"+Constants.MAXIMUM_FREQUENCY_FILE_PATH);
    			BufferedReader reader = new BufferedReader(new FileReader(mfPath));
    			try {
    			  String line = reader.readLine();
    			  mf = Integer.parseInt(line.split("\t")[1]);
    			}
    			catch (IOException e) {
    			  System.err.println("Maximum frequency file not found at " + mfPath);
    			  e.printStackTrace();
    			}
    			finally {
    			  reader.close();
    			}
    		}
    		
    		// If we just encoded the input, use the calculated maxFreq
    		else {
    			mf = mySequentialMiner.getDictionary().getMaximumFrequency();
    		} */
    		
    		// Maximum frequency is already stored in dictionary object 
    		// (when mining only, maxFreq value is read when JSON dict is loaded)
    		int mf = mySequentialMiner.getDictionary().getMaximumFrequency();
    		
    		// Calculate gamma and pass it the config and to the miner
    		int gamma = (tg-1) * (2*mf-1) + (3*mf-3);
			mySequentialMiner.commonConfig.setGamma( gamma );
			mySequentialMiner.myBfsMiner.setParametersAndClear(commonConfig.getSigma(), gamma, commonConfig.getLambda());
			System.out.println("Gamma calculated from temporalGap(="+tg+") and maximumFrequency(="+mf+"): "+gamma);
    		
    	}
    	
    	// Mine encoded files
    	mySequentialMiner.mineFiles();
    	
    	/* DEPRECATED
    	// Mine previously encoded input
    	if(commonConfig.isMineOnlyOption()){
    		mySequentialMiner.encodeAndMine(mySequentialMiner.getCommonConfig().getInputPath());
    	}
    	// Mine just-now encoded input
    	else {
    		mySequentialMiner.runSeqJob(new File(commonConfig.getInputPath()));
    	}
    	*/
		
        
        
      }
    } 	 
    
    
    //DISTRIBUTED EXECUTION MODE
    else if ("d".equalsIgnoreCase(executionMethod)) {
      
      DistributedMode myDistributedMiner = new DistributedMode(commonConfig);
      /*Execute the appropriate job based on whether we need to 
       * encode the input sequences or not.
       */
      
       // Mine
       if(commonConfig.isMineOnlyOption())
           myDistributedMiner.resumeJobs();
       // Encode + Mine
       else
           myDistributedMiner.runJobs();
       
    }
    //END OF EXECUTING FSM JOB
    //Return a zero exit status to indicate successful completion
    return 0;
  }
  
  /**
   * The main method receives the cmd arguments
   * and initiates the Hadoop job.
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception { 
    ToolRunner.run(new Configuration(), new FsmDriver(), args);
  }

}