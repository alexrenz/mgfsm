package de.mpii.fsm.tools;

import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.list.IntArrayList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.mpii.fsm.util.Constants;
import de.mpii.fsm.util.DfsUtils;
import de.mpii.fsm.util.IntArrayWritable;

/**
 * A general converter for converting various sequence datasets Assume a text
 * input file with a n-length sequence per line: seqid item1 item2 ... itemn
 * Default item separator is the tab character
 * 
 * Items equal to a negative number are considered gaps and are ignored during
 * word counting & dictionary construction
 * 
 * @author Klaus Berberich
 * @author Iris Miliaraki
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 */
public class ConvertTimestampSequences extends Configured implements Tool {

	static final Logger LOGGER = Logger.getLogger(ConvertTimestampSequences.class.getSimpleName());
	
	public static enum DICTIONARY {
		  MAXIMUMFREQUENCY
	};

	//////
	/////
	//// PHASE 1: Perform simple word count
	///
	//
	public static final class WordCountMapper extends Mapper<LongWritable, Text, Writable, IntWritable> {

		// singleton output key -- for efficiency reasons
		private final Text outKey = new Text();

		// singleton output value -- for efficiency reasons
		private final IntWritable outValue = new IntWritable();

		// item separator: default is the tab character
		String itemSeparator = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			itemSeparator = context.getConfiguration().get("de.mpii.tools.itemSeparator", "\t");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] items = value.toString().split(itemSeparator);

			OpenObjectIntHashMap<String> itemCounts = new OpenObjectIntHashMap<String>();
			
			int maxItemsAtOneTimestamp = 0;
			int itemsAtCurrentTimestamp = 0;
			String previousTimestamp = "";

			// ignore pos 0 which contains a sequence identifier
			// increase counter by 2 in each run due to the timestamp+item combination
			for (int i = 1; i < items.length; i=i+2) {
				// update counts of items
				String timestamp = items[i];
				String item = items[i+1];
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
			
			// add dummy item "#" as max items value
			itemCounts.put("#", maxItemsAtOneTimestamp);

			// emit item and frequency
			for (String term : itemCounts.keys()) {
				outKey.set(term);
				outValue.set(itemCounts.get(term));
				context.write(outKey, outValue);
			}
		}
	}

	public static final class WordCountReducer extends Reducer<Text, IntWritable, Text, Text> {

		// singleton output key -- for efficiency reasons
		private final Text outKey = new Text();

		// singleton output value -- for efficiency reasons
		private final Text outValue = new Text();

		// collection frequencies
		private final OpenObjectIntHashMap<String> cfs = new OpenObjectIntHashMap<String>();

		// document frequencies
		private final OpenObjectIntHashMap<String> dfs = new OpenObjectIntHashMap<String>();
		
		// special key
		private final Text specialKey = new Text("#");
		
		// Multiple Outputs in order to output maximumFrequency to other file
		private MultipleOutputs<Text, Text> out;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			out = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int cf = 0;
			int df = 0;
			
			// all "normal" keys
			if(!key.equals(specialKey)) {
				for (IntWritable value : values) {
					cf += value.get();
					df++;
				}
				
				cfs.put(key.toString(), cf);
				dfs.put(key.toString(), df);
			}
			// the special maxItemsAtOneTimestamp key
			else {
				for (IntWritable value : values) {
					cf = Math.max(cf, value.get());
				}

				cfs.put(key.toString(), cf);
			}
			
			
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Extract the special case "#" (maxFrequency)
			int maxItemsAtOneTimestamp = cfs.get("#");
			cfs.removeKey("#");
			
			// sort terms in descending order of their collection frequency
			List<String> temp = cfs.keys();
			String[] terms = temp.toArray(new String[temp.size()]);

			Arrays.sort(terms, new Comparator<String>() {

				@Override
				public int compare(String t, String u) {
					return cfs.get(u) - cfs.get(t);
				}
			});

			// assign term identifiers
			OpenObjectIntHashMap<String> tids = new OpenObjectIntHashMap<String>();
			for (int i = 0; i < terms.length; i++) {
				tids.put(terms[i], (i + 1));
			}

			// sort terms in lexicographic order and produce output
			Arrays.sort(terms);
			for (String term : terms) {
				outKey.set(term);
				outValue.set(cfs.get(term) + "\t" + dfs.get(term) + "\t" + tids.get(term));
				context.write(outKey, outValue);
			}
			
			// Write maximum frequency to separate output
			outKey.set("maximumFrequency");
			outValue.set(Integer.toString(maxItemsAtOneTimestamp));
			out.write(outKey, outValue, "mf/maximumFrequency");
			out.close();
		}
	}

	//////
	/////
	//// PHASE 2: Transform input collection into integer sequences
	///
	//
	public static final class TransformationMapper extends Mapper<LongWritable, Text, LongWritable, IntArrayWritable> {

		// singleton output key -- for efficiency reasons
		private final LongWritable outKey = new LongWritable();

		// singleton output value -- for efficiency reasons
		private final IntArrayWritable outValue = new IntArrayWritable();

		// mapping from terms to their corresponding term identifiers
		private final OpenObjectIntHashMap<String> itemTIdMap = new OpenObjectIntHashMap<String>();
		
		// the maximum number of items per at one timestamp (identified in the first MapReduce job)
		private int maxFrequency; 
		private int multiplyFactor;
		

		String itemSeparator = "\t";

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			itemSeparator = context.getConfiguration().get("de.mpii.tools.itemSeparator", "\t");
			maxFrequency = context.getConfiguration().getInt("de.mpii.tools.maxFrequency", 0);
			
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("dictionary")));
				while (br.ready()) {
					String[] tokens = br.readLine().split("\t");
					itemTIdMap.put(tokens[0], Integer.parseInt(tokens[3]));
				}
				br.close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			// retrieve the maxFrequency from the Dictionary
			multiplyFactor = (2 * maxFrequency) - 1;
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] tokens = value.toString().split(itemSeparator);

			IntArrayList itemIds = new IntArrayList();

			String sequenceId = tokens[0];


			long id = 0;
			try {
				id = Long.parseLong(sequenceId);
			} catch (NumberFormatException e) {
				// if this is not a number, hash it
				id = sequenceId.hashCode();
			}
			
			StringBuilder sb = new StringBuilder();
			sb.append(sequenceId + " ");
			
			long currTime = 0;
			long prevTime = 0;
			long timeDelta = 0;
			long timeGap = 0;
			//long item = 0;
			
			int repeats = 0;

			
			for (int i = 1; i < tokens.length; i=i+2) {

		      // multiply each time-stamp by m=2f-1, where f is the maximum allowed frequency
		      // f identical repeated time-stamps [t,t,...,t] will be replaced by [m*t, m*(t+1), m*(t+2), ... m*(t+f-1)]
		      currTime = Long.parseLong(tokens[i]) * multiplyFactor;

		      if (i != 1) {

		        //item = Long.parseLong(tokens[i + 1]);
		        timeDelta = currTime - prevTime;
		        
		        
		        if (timeDelta < 0) {
		          System.err.println("Wrongly formatted input! TimeDelta is " + timeDelta + " (" + currTime + " - " + prevTime + ")");
		          // ToDo - error handling in Hadoop
		          //return null;
		        }

		        if (timeDelta == 0) {

		          // replace consecutive identical time-stamps
		          repeats++;
		          //sb.append(item + (i != tokens.length - 1 ? " " : ""));
		          itemIds.add(itemTIdMap.get(tokens[i+1]));

		        } else {

		          // new increasing time-stamp, reset repeat counter
		          timeGap = timeDelta - repeats - 1;
		          repeats = 0;
		          if (timeGap > 0) {
		            //sb.append(-timeGap + " " + item + (i != tokens.length - 1 ? " " : ""));
		        	// ToDo: clear whether we want to convert itemIds to long
		        	itemIds.add((int) -timeGap);
		        	itemIds.add(itemTIdMap.get(tokens[i+1]));
		          } else {
		        	itemIds.add(itemTIdMap.get(tokens[i+1]));
		          }

		        }

		        prevTime = currTime;

		      } else {

		        // first item, no time delta appended
		        prevTime = currTime;
		        itemIds.add(itemTIdMap.get(tokens[i+1]));
		      }

		    }
			
			if (itemIds.size() > 0) {

				outKey.set(id);
				outValue.setContents(itemIds.toArray(new int[0]));
				
				context.write(outKey, outValue);
			}

		}
	}

	@Override
	public int run(String[] args) throws Exception {

		LOGGER.setLevel(Level.INFO);
		if (args.length < 3) {
			LOGGER.log(Level.WARNING, "Usage: ConvertTimestampSequences <input> <output> <numReducers> (<itemSeparator>)");
			System.exit(-1);
		}

		// read job parameters from commandline arguments
		String input = args[0];
		String output = args[1];
		int numReducers = Integer.parseInt(args[2]);
		String itemSeparator = "\t";

		// item separator can be passed as an optional argument
		if (args.length > 3) {
			itemSeparator = args[3];
		}

		boolean dictionaryExists = false;
		if (!dictionaryExists) {

			// delete output directory if it exists
			FileSystem.get(getConf()).delete(new Path(args[1]), true);

			/////
			//// PHASE 1: Compute word counts
			///
			// Job job1 = new Job(getConf());
			Job job1 = Job.getInstance(getConf());

			// set job name and options
			job1.setJobName("sequence collection conversion (phase 1)");
			job1.setJarByClass(this.getClass());

			job1.getConfiguration().setStrings("de.mpii.tools.itemSeparator", itemSeparator);

			// set input and output paths
			FileInputFormat.setInputPaths(job1, DfsUtils.traverse(new Path(input), job1.getConfiguration()));
			TextOutputFormat.setOutputPath(job1, new Path(output + "/wc"));

			// set input and output format
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			// set mapper and reducer class
			job1.setMapperClass(WordCountMapper.class);
			job1.setReducerClass(WordCountReducer.class);

			// set number of reducers
			job1.setNumReduceTasks(1);

			// map output classes
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(IntWritable.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			// Larger resource limit for maps.
			job1.getConfiguration().set("mapreduce.cluster.mapmemory.mb", "4096");
			// Larger resource limit for reduces.
			job1.getConfiguration().set("mapreduce.cluster.reducememory.mb", "4096");

			// start job
			job1.waitForCompletion(true);
		}
		
		
		/////
		//// PHASE 2: Transform document collection
		///
		

		// Read maximum frequency from the created file
		int maxFrequency = 0;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(FileSystem.get(getConf()).open(new Path(output + "/" + Constants.MAXIMUM_FREQUENCY_FILE_PATH))));
			String[] tokens = br.readLine().split("\t");
			maxFrequency = Integer.parseInt(tokens[1]);
			br.close();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Maximum Frequency File not found at " + output + "/" + Constants.MAXIMUM_FREQUENCY_FILE_PATH);
			throw new RuntimeException(e);
		}

		
		// Job job2 = new Job(getConf());
		Job job2 = Job.getInstance(getConf());

		// set job name and options
		job2.setJobName("document collection conversion (phase 2)");
		job2.setJarByClass(this.getClass());

		job2.getConfiguration().setStrings("de.mpii.tools.itemSeparator", itemSeparator);
		job2.getConfiguration().setStrings("de.mpii.tools.maxFrequency", String.valueOf(maxFrequency));

		// set input and output paths
		FileInputFormat.setInputPaths(job2, DfsUtils.traverse(new Path(input), job2.getConfiguration()));
		SequenceFileOutputFormat.setOutputPath(job2, new Path(output + "/raw"));
		SequenceFileOutputFormat.setCompressOutput(job2, false);

		// set input and output format
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		// set mapper and reducer class
		job2.setMapperClass(TransformationMapper.class);

		// set number of reducers
		job2.setNumReduceTasks(numReducers);

		// map output classes
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(IntArrayWritable.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(IntArrayWritable.class);

		// add files to distributed cache
		for (FileStatus file : FileSystem.get(getConf()).listStatus(new Path(output + "/wc"))) {
			if (file.getPath().toString().contains("part")) {
				DistributedCache.addCacheFile(new URI(file.getPath().toUri() + "#dictionary"), job2.getConfiguration());
			}
		}

		// Larger resource limit for maps.
		job2.getConfiguration().set("mapreduce.cluster.mapmemory.mb", "4096");
		// Larger resource limit for reduces.
		job2.getConfiguration().set("mapreduce.cluster.reducememory.mb", "4096");

		// start job
		job2.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ConvertTimestampSequences(), args);
		System.exit(exitCode);
	}
}

