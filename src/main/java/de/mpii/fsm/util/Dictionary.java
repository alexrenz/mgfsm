package de.mpii.fsm.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.eclipse.jdt.core.dom.ThisExpression;

/**
 * 
 * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
 * 
 * -------------------------------------------------------------------------
 * NOTE 1 : Objective of dictionary class
 * ------------------------------------------------------------------------- 
 * This class combines the earlier :
 * 1. DicReader 2.DicReaderImpl 3.DicWriter
 * 
 * The DicReader interface is elimnated, exposing
 * a clean Dictionary class for all the Dictionary
 * creation, reading, & other needs.
 * -------------------------------------------------------------------------
 * NOTE 2 : Refer to this note for Dictionary Reading related functionality 
 * ------------------------------------------------------------------------- 
 * @author Spyros Zoupanos
 * This class shows the basic set of available variables and methods of the 
 * classes extending a dictionary reader. The dictionary is a 4-column file 
 * containing the terms, their document frequency, their collection frequency 
 * and their term ids (the order of the columns is the one mentioned). 
 * The file is sorted lexicographically on the first column (terms). 
 * After the creation of the object the method load has to be called before 
 * calling any other method or accessing any of the variables.
 * -------------------------------------------------------------------------
 * NOTE 3 : Refer to this note for dictionary writing related documentation
 * -------------------------------------------------------------------------
 * -------------------------------------------------------------------------
 * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
 * 
 * The following class writes out the dictionary to a local 
 * 4-column tab separated file in the following format :
 * -------------------------------------------------------------------------
 *                      DICTIONARY FORMAT
 * -------------------------------------------------------------------------
 * <TERM>   <COLL. FREQ.>	 <DOC. FREQ.>    <ID>
 * -------------------------------------------------------------------------
 * NOTE 4: The dictionary is lexicographically sorted on <TERM>
 * NOTE 5: This dictionary will only be used for the sequential mode
 *         as the ID is calculated in a sequential mode. No Map/Reduce
 *         wordcount is used.
 * -------------------------------------------------------------------------
 */

/**
 * @author hduser
 *
 */
public class Dictionary {

  //Attributes 

  //Required for reading the dictionary from files
  //efficiently
  public int[] itemIds;
  public int[] docFreqs;
  public int[] colFreqs;
  public String[] items;

  //Dictionary map
  private HashMap <String, DicItem> dictionary;

  // Used for writing the final lexicographically sorted 
  // dictionary on local disk.
  private ArrayList<DicItem> dictionaryFinal;

  // Contains the path name where the 
  // document files of the corpus are contained.
  private String corpusFolderPath;
  
  // Flag whether to use timestamp-encoded input format or not
  private boolean useTimestampInput;
  
  // Maximum number of items per one timestamp
  private int maximumFrequency;
  private List<Integer> maximumFrequenciesList = new ArrayList<Integer>();

  /**
   * @author Dhruv Gupta (dhgupta@mpi-inf.mpg.de)
   * Inner Class Declaration :
   * 
   * Constructs the basic struct of the Dictionary.
   * DicWriter will utilize this class for manipulating
   * the DicItems and constructing the required format of 
   * dictionary.
   */
  public class DicItem {

    /* Contains the dictionary term */
    private String term;
    /* Stores the document frequency */
    private long documentFreq;
    /* Stores the collection frequency */
    private long collectionFreq;
    /* Stores the id of the term */
    private int  id;
    
    
    DicItem(String term, int cf, int df){
    	this.term = term;
    	this.collectionFreq = cf;
    	this.documentFreq = df;
    }

    /*
     * Getter and Setter methods 
     * for the fields of the inner class
     */
    public String getTerm() {
      return term;
    }
    public void setTerm(String term) {
      this.term = term;
    }
    public long getDocumentFreq() {
      return documentFreq;
    }
    public void setDocumentFreq(long documentFreq) {
      this.documentFreq = documentFreq;
    }
    public long getCollectionFreq() {
      return collectionFreq;
    }
    public void setCollectionFreq(long collectionFreq) {
      this.collectionFreq = collectionFreq;
    }
    public int getId() {
      return id;
    }
    public void setId(int id) {
      this.id = id;
    }
    @Override
    public String toString() {
      return "DicItem [term=" + term + ", documentFreq=" + documentFreq
          + ", collectionFreq=" + collectionFreq + ", id=" + id + "]";
    }
  }//END OF INNER CLASS

  private Map<Integer, Integer> itemIdToPos;
  //END OF ATTRIBUTES

  //CONSTRUCTORS
  /**
   * Empty constructor
   * 
   * @param void
   */
  public Dictionary()
  {
    this.dictionary       = new HashMap<String, DicItem>();
    this.dictionaryFinal  = new ArrayList<DicItem>();
    this.corpusFolderPath = null;
    this.useTimestampInput = false;
  }

  /**
   * @param String corpusFolderPath - value to be assigned to the corpusFolderPath attribute
   */
  public Dictionary(String corpusFolderPath)
  {
    this.dictionary       = new HashMap<String, DicItem>();
    this.dictionaryFinal  = new ArrayList<DicItem>();
    this.corpusFolderPath = corpusFolderPath;
    this.useTimestampInput = false;
  }
  
  /**
   * @param String corpusFolderPath - value to be assigned to the corpusFolderPath attribute
   */
  public Dictionary(String corpusFolderPath, boolean useTimestampInput)
  {
    this.dictionary       = new HashMap<String, DicItem>();
    this.dictionaryFinal  = new ArrayList<DicItem>();
    this.corpusFolderPath = corpusFolderPath;
    this.useTimestampInput = useTimestampInput;
  }


  //END OF CONSTRUCTORS

  //GETTER & SETTER METHODS

  /**
   * @return HashMap<String, DicItem>
   * @param void
   */
  public HashMap<String, DicItem> getDictionary() {
    return dictionary;
  }

  /**
   * @return void
   * @param HashMap<String, DicItem>
   */
  public void setDictionary(HashMap<String, DicItem> dictionary) {
    this.dictionary = dictionary;
  }

  /**
   * @return String
   * @param void
   */
  public String getCorpusFolderPath() {
    return corpusFolderPath;
  }

  /**
   * @return void
   * @param String corpusFolderPath - value to be assigned to the corpusFolderPath attribute
   */
  public void setCorpusFolderPath(String corpusFolderPath) {
    this.corpusFolderPath = corpusFolderPath;
  }
  
  /**
   * @return Int
   * @param void
   */
  public int getMaximumFrequency() {
	  return maximumFrequency;
  }
  //END OF GETTER & SETTERS

  /**
   * Read the dictionary from the local disk
   * 
   * @param Configuration conf
   * @param String fileName
   * @param int[] colsToLoad
   * @param int minDocFreq
   * @param int minColFreq
   * @throws IOException
   */
  public void load(Configuration conf, String fileName, int[] colsToLoad, 
      int minDocFreq, int minColFreq) throws IOException {

    // understanding what to load
    boolean loadItem = false;
    boolean loadDocFreq = false;
    boolean loadColFreq = false;
    boolean loadItemId = false;
    for(int col : colsToLoad) {
      if(col == Constants.ITEM)
        loadItem = true;
      if(col == Constants.DOC_FREQ)
        loadDocFreq = true;
      if(col == Constants.COL_FREQ)
        loadColFreq = true;
      if(col == Constants.ITEM_ID)
        loadItemId = true;
    }

    // creating some temporary lists needed during the loading
    // of the dictionary
    List<Integer> itemIdsList = new ArrayList<Integer>();
    List<Integer> docFreqsList = new ArrayList<Integer>();
    List<Integer> colFreqsList = new ArrayList<Integer>();
    List<String> itemList = new ArrayList<String>();

    // loading the data to the lists
    /* 
     * Depending upon whether the 
     * configuration is passed as a argument or 
     * not pass the appropriate stream to 
     * buffered reader.
     */
    BufferedReader br = null;
    if(conf == null){
      @SuppressWarnings("resource")
      FileInputStream fstream = new FileInputStream(fileName);
      // Get the object of DataInputStream
      DataInputStream in      = new DataInputStream(fstream);
      br                      = new BufferedReader(new InputStreamReader(in));
    }
    else { 
      FileSystem fs          = FileSystem.get(conf);
      FSDataInputStream  dis = fs.open(new Path(fileName));
      br                     = new BufferedReader(new InputStreamReader(dis));
    }

    String line = null;
    int colFreqValue = -1;
    int docFreqValue = -1;
    
    while((line = br.readLine()) != null) {
      String[] splits = line.split("\t");

      // we check if the line satisfies the minimum given frequencies 
      if(minColFreq > -1) {
        colFreqValue = Integer.parseInt(splits[1]);
        if(colFreqValue < minColFreq)
          continue;
      }
      if(minDocFreq > -1) {
        docFreqValue = Integer.parseInt(splits[2]);
        if(docFreqValue < minDocFreq)
          continue;
      }

      // we keep the columns that we want
      if(loadColFreq) {
        if(colFreqValue == -1)
          colFreqsList.add(Integer.parseInt(splits[1]));
        else
          colFreqsList.add(colFreqValue);
        colFreqValue = -1;
      }
      if(loadDocFreq) {
        if(docFreqValue == -1)
          docFreqsList.add(Integer.parseInt(splits[2]));
        else
          docFreqsList.add(docFreqValue);
        docFreqValue = -1;
      }
      if(loadItem)
        itemList.add(splits[0]);
      if(loadItemId)
        itemIdsList.add(Integer.parseInt(splits[3]));
    }
    
    // filling the arrays with data
    if(!itemIdsList.isEmpty()) {
      itemIds = new int[itemIdsList.size()];
      int pos = 0;
      for(Integer tempInt : itemIdsList) {
        itemIds[pos] = tempInt;
        pos++;
      }
      itemIdsList = null;
    }
    if(!docFreqsList.isEmpty()) {
      docFreqs = new int[docFreqsList.size()];
      int pos = 0;
      for(Integer tempInt : docFreqsList) {
        docFreqs[pos] = tempInt;
        pos++;
      }
      docFreqsList = null;
    }
    if(!colFreqsList.isEmpty()) {
      colFreqs = new int[colFreqsList.size()];
      int pos = 0;
      for(Integer tempInt : colFreqsList) {
        colFreqs[pos] = tempInt;
        pos++;
      }
      colFreqsList = null;
    }
    if(!itemList.isEmpty()) {
      items = new String[itemList.size()];
      int pos = 0;
      for(String tempStr : itemList) {
        items[pos] = tempStr;
        pos++;
      }
      itemList = null;

    }

    // if the termIDs are present we create a map from
    // the termId to the array positions of these termIds
    if(itemIds != null) {
      itemIdToPos = new HashMap<Integer, Integer>();
      int pos = 0;
      for(int termId : itemIds) {
        itemIdToPos.put(termId, pos);
        pos++;
      }
    }
  
  
  
  }
  
  
  public void loadJSON(Configuration conf, String fileName, int[] colsToLoad, 
    int minDocFreq, int minColFreq) throws IOException {
	  
	System.out.println("Starting json read;");

    // understanding what to load
    boolean loadItem = false;
    boolean loadDocFreq = false;
    boolean loadColFreq = false;
    boolean loadItemId = false;
    for(int col : colsToLoad) {
      if(col == Constants.ITEM)
        loadItem = true;
      if(col == Constants.DOC_FREQ)
        loadDocFreq = true;
      if(col == Constants.COL_FREQ)
        loadColFreq = true;
      if(col == Constants.ITEM_ID)
        loadItemId = true;
    }

    // creating some temporary lists needed during the loading
    // of the dictionary
    List<Integer> itemIdsList = new ArrayList<Integer>();
    List<Integer> docFreqsList = new ArrayList<Integer>();
    List<Integer> colFreqsList = new ArrayList<Integer>();
    List<String> itemList = new ArrayList<String>();

    // loading the data to the lists
    /* 
     * Depending upon whether the 
     * configuration is passed as a argument or 
     * not pass the appropriate stream to 
     * buffered reader.
     */
    JsonFactory jsonfactory = new JsonFactory();
    File source = new File(fileName);
    
    JsonParser parser = null;
    if(conf == null){
      @SuppressWarnings("resource")
      FileInputStream fstream = new FileInputStream(fileName);
      parser = jsonfactory.createJsonParser(fstream);
    }
    else { 
      FileSystem fs          = FileSystem.get(conf);
      FSDataInputStream  dis = fs.open(new Path(fileName));
      parser = jsonfactory.createJsonParser(dis);
    }


    String line = null;
    int colFreqValue = -1;
    int docFreqValue = -1;
    
    int idValue = -1;
    String termValue = null;
    
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String token = parser.getCurrentName();
      
      if("maxRepetitionsPerTimestamp".equals(token)) {
    	  parser.nextToken();
    	  this.maximumFrequency = parser.getValueAsInt();
    	  System.out.println("Max Frequency: "+this.maximumFrequency);
      }
      
      if("dictionary".equals(token)) {
    	  
    	  // Parse tokens until we hit the end of the array - ']'
    	  while(parser.nextToken() != JsonToken.END_ARRAY){
    		  /* Structure of the JSON object of one item:
    		   * {
    		   * 	id: 1,
    		   * 	term: "term",
    		   * 	colFreq: 3,
    		   * 	docFreq: 2
    		   * }
    		   */
    		  
    		  // Handle one item in one run
    		  if(parser.getCurrentToken() == JsonToken.START_OBJECT) {
    			  
    			  // Step 1: extract the information of this term from the json
    			  while(parser.nextToken() != JsonToken.END_OBJECT) {
		    		  String element_token = parser.getCurrentName();
		    		  
		    		  if("id".equals(element_token)) {
		    	    	  parser.nextToken();
		    	    	  idValue = parser.getValueAsInt();
		    	      }
		    		  if("term".equals(element_token)) {
		    	    	  parser.nextToken();
		    	    	  termValue = parser.getText();
		    	      }
		    		  if("colFreq".equals(element_token)) {
		    	    	  parser.nextToken();
		    	    	  colFreqValue = parser.getValueAsInt();
		    	      }
		    		  if("docFreq".equals(element_token)) {
		    	    	  parser.nextToken();
		    	    	  docFreqValue = parser.getValueAsInt();
		    	      }
    			  }
    			  
    			  System.out.println("- Item: " + idValue + ", " + termValue + ", " + colFreqValue + ", " + docFreqValue);
        		  
    			  
    			  
    			  // Step 2: Add this term to the lists
    			  
    			  // we check if the line satisfies the minimum given frequencies 
    			  if(minColFreq > -1) {
    				  if(colFreqValue < minColFreq)
    					  continue;
        		  }
    			  if(minDocFreq > -1) {
    				  if(docFreqValue < minDocFreq)
    					  continue;
    			  }
    			  
    			  // keep the columns we are interested in
    			  if(loadColFreq) {
    		        colFreqsList.add(colFreqValue);
    		        colFreqValue = -1;
    		      }
    		      if(loadDocFreq) {
    		    	docFreqsList.add(docFreqValue);
    		        docFreqValue = -1;
    		      }
    		      if(loadItem)
    		        itemList.add(termValue);
    		      if(loadItemId)
    		        itemIdsList.add(idValue);
    		  }
    		  
    		  
		  }
      }
        
    }
    
    parser.close();
    
      
    
    // filling the arrays with data
    if(!itemIdsList.isEmpty()) {
      itemIds = new int[itemIdsList.size()];
      int pos = 0;
      for(Integer tempInt : itemIdsList) {
        itemIds[pos] = tempInt;
        pos++;
      }
      itemIdsList = null;
    }
    if(!docFreqsList.isEmpty()) {
      docFreqs = new int[docFreqsList.size()];
      int pos = 0;
      for(Integer tempInt : docFreqsList) {
        docFreqs[pos] = tempInt;
        pos++;
      }
      docFreqsList = null;
    }
    if(!colFreqsList.isEmpty()) {
      colFreqs = new int[colFreqsList.size()];
      int pos = 0;
      for(Integer tempInt : colFreqsList) {
        colFreqs[pos] = tempInt;
        pos++;
      }
      colFreqsList = null;
    }
    if(!itemList.isEmpty()) {
      items = new String[itemList.size()];
      int pos = 0;
      for(String tempStr : itemList) {
        items[pos] = tempStr;
        pos++;
      }
      itemList = null;

    }

    // if the termIDs are present we create a map from
    // the termId to the array positions of these termIds
    if(itemIds != null) {
      itemIdToPos = new HashMap<Integer, Integer>();
      int pos = 0;
      for(int termId : itemIds) {
        itemIdToPos.put(termId, pos);
        pos++;
      }
    }
  
  
  
  }
  

  public int itemID(int pos) {
    if(itemIds != null)
      return itemIds[pos];
    else
      return -1;
  }

  public int posOf(int termID) {
    if(itemIdToPos != null)
      return itemIdToPos.get(termID);
    else
      return -1;
  }

  public int posOf(String term) {
    return Arrays.binarySearch(items, term);
  }


  //Dictionary Construction Methods
  
  /**
   * Recursive function to descend into the directory tree and find all the files 
   * that end with ".txt"
   * @param dir A file object defining the top directory
 * @throws IOException 
   **/
  public void processCorpus(File dir) throws IOException {
    String pattern = ".txt";

    if(dir.isFile() && dir.getName().endsWith(pattern)){
      computeFrequency(dir);
    }
    else{
      File listFile[] = dir.listFiles();
      if (listFile != null) {
        for (int i=0; i<listFile.length; i++) {
          if (listFile[i].isDirectory()) {
            processCorpus(listFile[i]);
          } else {
            if (listFile[i].getName().endsWith(pattern)) {
              computeFrequency(listFile[i]);
            }
          }
        }
      }
    }
   
  }


  /**
   * Construct the dictionary 
   * from a given corpus.
   * 
   * @return void 
   * @param void
 * @throws IOException 
   */
	public void constructDictionary() throws IOException {

		File file = new File(this.corpusFolderPath);

		// Recursively process all the text files in the corpus
		processCorpus(file);
		
		// If using temporal input, determine the maximumFrequency
		if(this.useTimestampInput) {
			this.maximumFrequency = Collections.max(this.maximumFrequenciesList);
		}

		// Fill out the values of ID in the final dictionary
		// sort terms in descending order of their collection frequency
		Set<String> temp = dictionary.keySet();
		
		String[] terms = temp.toArray(new String[temp.size()]);
		

		Arrays.sort(terms, new Comparator<String>() {

			@Override
			public int compare(String t, String u) {
				return (int) (dictionary.get(u).getCollectionFreq() - dictionary
						.get(t).getCollectionFreq());
			}
		});

		// assign term identifiers
		for (int i = 0; i < terms.length; i++) {
			DicItem dicItem = dictionary.get(terms[i]);
			dicItem.setId(i + 1);
		}
		
		Arrays.sort(terms);
		for (String term : terms) {
			DicItem dicItem = dictionary.get(term);
			dictionaryFinal.add(dicItem);
	      }
	}

  /**
   * The following function aids in calculating the document frequency (df) &
   * the collection frequency (cf) of the items in the input sequences contained
   * with the argument to this function viz. <i> File child </i>.
   * 
   * @return void
   * @param File child 
   * @throws IOException 
   */
  public void computeFrequency(File child) throws IOException
  {
      FileInputStream fstream = new FileInputStream(child);
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      
      String strLine;
      int maxItemsAtOneTimestamp;
      int itemsAtCurrentTimestamp;
      String previousTimestamp;
      String timestamp;
      String item;
      
      while ((strLine = br.readLine()) != null) {
    	  
    	  String[] items = strLine.split("\\s+");
    	  
    	  OpenObjectIntHashMap<String> itemCounts = new OpenObjectIntHashMap<String>();
    	  
    	  // standard input format
    	  if(!this.useTimestampInput) {
    	  
				// ignore pos 0 which contains a sequence identifier
				for (int i = 1; i < items.length; i++) {
				  // update counts of items
				  item = items[i];
				  itemCounts.adjustOrPutValue(item, +1, +1);
				}
    	  }
    	  // timestamp-encoded input format
    	  else {
    		  	maxItemsAtOneTimestamp = 0;
				itemsAtCurrentTimestamp = 0;
				previousTimestamp = "";
				
				// ignore pos 0 which contains a sequence identifier
				// increase counter by 2 in each run due to the timestamp+item combination
				for (int i = 1; i < items.length; i=i+2) {
					// update counts of items
					timestamp = items[i];
					item = items[i+1];
					if(timestamp.equals(previousTimestamp)) {
						itemsAtCurrentTimestamp++;
					}
					else {
						itemsAtCurrentTimestamp = 1;
					}
					if(itemsAtCurrentTimestamp > maxItemsAtOneTimestamp) {
						maxItemsAtOneTimestamp = itemsAtCurrentTimestamp;
					}
					itemCounts.adjustOrPutValue(item, +1, +1);
					previousTimestamp = timestamp;
  			}
  			
			// Store maximum frequency in the dictionary class
			this.maximumFrequenciesList.add(maxItemsAtOneTimestamp);
    	  }
    	  
          
          for (String term : itemCounts.keys()) {
        	  addItemToDictionary(term, itemCounts.get(term));
          }
      }
      br.close();
  }

  private void addItemToDictionary(String term, int cf) {
	  DicItem dicItem = dictionary.get(term);
	  if(dicItem == null){
		  dicItem = new DicItem(term, cf, 1);
		  dictionary.put(term, dicItem);
	  } else {
		  // timestamp-input dummy item
		  if(this.useTimestampInput && term.equals("#")) {
			  dicItem.setCollectionFreq(Math.max(cf, dicItem.getCollectionFreq()));
			  dicItem.setDocumentFreq(dicItem.getCollectionFreq());
		  }
		  else {
			  dicItem.setCollectionFreq(cf + dicItem.getCollectionFreq());
			  dicItem.setDocumentFreq(dicItem.getDocumentFreq() + 1);
		  }
	  }
}

  /**
   * The following function writes out the dictionary to 
   * the local disk on the path specified by the constant
   * <i> OUTPUT_DICTIONARY_FILE_PATH </i> located in 
   * org.apache.mahout.fsm.util.Constants
   * 
   * @return void
   * @param String outputFolderName
   */
  public void writeDictionary(String outputFolderName)
  {
	// Write the dictionary output
    String outputFileName = outputFolderName;

    File outFile    = new File(outputFileName.concat("/" + Constants.OUTPUT_DICTIONARY_FILE_PATH));
    File parentFile = outFile.getParentFile();

    outFile.delete();
    parentFile.delete();
    parentFile.mkdirs();

    try 
    {
      // Open the file
      OutputStream fstreamOutput = new FileOutputStream(outFile);

      // Get the object of DataOutputStream
      DataOutputStream out = new DataOutputStream(fstreamOutput);
      BufferedWriter br    = new BufferedWriter(new OutputStreamWriter(out));       

      // Perform the writing to the file
      for(DicItem item : this.dictionaryFinal)
      {     
        br.write(item.getTerm() + "\t" + item.getCollectionFreq() +
            "\t" + item.getDocumentFreq() + "\t" + item.getId() + "\n");
      } 
      br.close();
    }   
    catch (IOException e) 
    {         
      e.printStackTrace();
    }
    // End of writing to the dictionary output file.
    
    
    // For Timestamp-encoded input format: write maximum frequency
    if(this.useTimestampInput) {
    	outFile = new File(outputFileName.concat("/" + Constants.MAXIMUM_FREQUENCY_FILE_PATH));
    	outFile.getParentFile().mkdirs();
    	try 
	    {
	      // Open the file
	      OutputStream fstreamOutput = new FileOutputStream(outFile);

	      // Get the object of DataOutputStream
	      DataOutputStream out = new DataOutputStream(fstreamOutput);
	      BufferedWriter br    = new BufferedWriter(new OutputStreamWriter(out));       
	      
	      br.write("MaximumFrequency" + "\t" + String.valueOf(this.maximumFrequency));
	      br.close();
	    }   
	    catch (IOException e) 
	    {         
	      e.printStackTrace();
	    }
    	
    }
    
    writeJSONDictionary(outputFolderName);
  }
  
  public void writeJSONDictionary(String outputFolderName) {
	  try {
		  // Example from here: http://javarevisited.blogspot.de/2015/03/parsing-large-json-files-using-jackson.html
		  
		  JsonFactory jsonfactory = new JsonFactory();
		  String fileName = outputFolderName.concat("/" + Constants.OUTPUT_DICTIONARY_FILE_PATH + ".json");
		  File jsonDoc = new File(fileName);
		  
		  JsonGenerator gen = jsonfactory.createJsonGenerator(jsonDoc, JsonEncoding.UTF8);
		  
		  gen.writeStartObject();
		  gen.writeStringField("inputFile",corpusFolderPath);
		  gen.writeStringField("date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
		  if(this.useTimestampInput) {
			  gen.writeBooleanField("timestampInput", true);
			  gen.writeNumberField("maxRepetitionsPerTimestamp", this.maximumFrequency);
		  }
		  else {
			  gen.writeBooleanField("timestampInput", false);
		  }
		  
		  gen.writeFieldName("dictionary");
		  gen.writeStartArray();
		  
		  for(DicItem item: this.dictionaryFinal) {
			  gen.writeStartObject();
			  gen.writeNumberField("id", item.getId());
			  gen.writeStringField("term", item.getTerm());
			  gen.writeNumberField("colFreq", item.getCollectionFreq());
			  gen.writeNumberField("docFreq", item.getDocumentFreq());
			  gen.writeEndObject();
		  }
		  
		  gen.writeEndArray();
		  gen.writeEndObject();
		  
		  gen.close();
		  
		  System.out.println("JSON file written to " + fileName);
	  } catch (Exception e) {
		  e.printStackTrace();
	  }
	  
	  
  }

  /**
   * The following functions reads the dictionary using DicReaderImpl 
   * and constructs a "reverse dictionary" (idToItem Map) which is
   * returned.
   * 
   * @return void
   * @param String dictionaryFilePath
   */
  public Map<Integer, String> readDictionary(String dictionaryFilePath) 
  {
    int[] colsToLoad = new int[4];
    // Map each item id to its name
    Map<Integer, String> itemIdToName = new HashMap<Integer, String>();
    try {

      colsToLoad[0] = Constants.ITEM;
      colsToLoad[3] = Constants.ITEM_ID;
      this.load( null,dictionaryFilePath, colsToLoad, 0, -1);

      int[] itemIdList = this.itemIds;

      for (int itemId : itemIdList) {
        String itemName = this.items[this.posOf(itemId)];
        itemIdToName.put(itemId, itemName);
      }
    } catch (Exception e) {

      e.printStackTrace();
    }
    return itemIdToName;
  }
  
  public Map<Integer, String> readJSONDictionary(String dictionaryFilePath) 
  {
    int[] colsToLoad = new int[4];
    // Map each item id to its name
    Map<Integer, String> itemIdToName = new HashMap<Integer, String>();
    try {

      colsToLoad[0] = Constants.ITEM;
      colsToLoad[3] = Constants.ITEM_ID;
      this.loadJSON( null,dictionaryFilePath, colsToLoad, 0, -1);

      int[] itemIdList = this.itemIds;

      for (int itemId : itemIdList) {
        String itemName = this.items[this.posOf(itemId)];
        itemIdToName.put(itemId, itemName);
      }
    } catch (Exception e) {

      e.printStackTrace();
    }
    return itemIdToName;
  }
  
  /**
   * NOTE 1: This is a overloaded method of the above.
   * NOTE 2: Call this method in ~.output.SequenceTranslator.java
   *         for distributed mode so that the configuration of the 
   *         Hadoop fs is passed to the load method.
   *          
   * The following functions reads the dictionary using DicReaderImpl 
   * and constructs a "reverse dictionary" (idToItem Map) which is
   * returned.
   * 
   * @return void
   * @param String dictionaryFilePath
   */
  public Map<Integer, String> readDictionary(Configuration conf, String dictionaryFilePath) 
  {
    int[] colsToLoad = new int[4];
    // Map each item id to its name
    Map<Integer, String> itemIdToName = new HashMap<Integer, String>();
    try {

      colsToLoad[0] = Constants.ITEM;
      colsToLoad[3] = Constants.ITEM_ID;
      this.load( conf,dictionaryFilePath, colsToLoad, 0, -1);

      int[] itemIdList = this.itemIds;

      for (int itemId : itemIdList) {
        String itemName = this.items[this.posOf(itemId)];
        itemIdToName.put(itemId, itemName);
      }
    } catch (Exception e) {

      e.printStackTrace();
    }
    return itemIdToName;
  }
  
  /**
 * @param dictionaryFilePath
 * @param sigma
 * @return
 * @throws IOException
 */
public OpenIntIntHashMap getFListMap(String dictionaryFilePath, int sigma) throws IOException {
	  OpenIntIntHashMap fListMap = new OpenIntIntHashMap();
	  
	  int[] colsToLoad = new int[2];
	  colsToLoad[0] = Constants.DOC_FREQ;
      colsToLoad[1] = Constants.ITEM_ID;
      
      this.load(null, dictionaryFilePath, colsToLoad, sigma, -1);
	  
      int[] itemIdList = this.itemIds;
      for(int itemId : itemIdList) {
    	  int support = this.docFreqs[this.posOf(itemId)];
    	  fListMap.put(itemId, support);
      }
      return fListMap;
	  
  }
  //END OF DICTIONARY CONSTRUCTION METHODS
  
}//END OF CLASS
