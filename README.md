MG-FSM
======

Introduction
------------

Sequential pattern mining aims to discover hidden patterns and relationships in collections of sequential data; it has been successfully applied in a number of data mining tasks including text mining (e.g., finding frequent phrases), market basket analysis (e.g., finding frequent sequences of product sales), and web usage mining (e.g., finding frequent sequences of page visits). In particular, in the context of text mining, frequent phrases (commonly referred to as n-grams) are widely used in applications such as machine translation, speech recognition and information extraction.

[MG-FSM][1] is a scalable, general-purpose frequent sequence mining algorithm built for MapReduce. It takes as input a collection of sequences (e.g., a text collection or Web usage logs) and mines frequent sequences subject to a number of constraints such as minimum frequency, maximum length, or proximity constraints (position-based or temporal). A detailed description of MG-FSM can be found [here][2].

###Contributors
[Iris Miliaraki] [4], [Klaus Berberich] [5], [Rainer Gemulla] [6], [Kaustubh Beedkar] [7], [Dhruv Gupta] [8] and [Alexander Renz-Wieland] [9].


MG-FSM overview
---------------
Given a collection of input sequences (a sequence database), MG-FSM finds frequent subsequences that

+  Occur in at least σ ≥ 1  sequences (support threshold).
+  Have length at most λ ≥ 2 (length threshold).
+  Have gap at most γ ≥ 0 between consecutive items (gap threshold).

In addition to these constraints, MG-FSM also supports other types of constraints. Please refer to the available command line options for details.


Building MG-FSM
---------------

> 1. Prerequisites for building MG-FSM
    - Java JDK 1.6 or 1.7
    - [Maven][3] 3.2 or higher
> 2. Check out the MG-FSM source code to some directory, we call it here as MGFSM_HOME
> 3. Compiling:
>
>           $ cd MGFSM_HOME
>           $ mvn clean install

> 4. Set the environment variables:
>
>           $ export JAVA_HOME=/location of Java
>           $ export HADOOP_HOME=/location of Hadoop


After following the above methods, you can run the MG-FSM algorithm with appropriate arguments. Also, note that you can run the algorithm over HDFS data (in distributed mode) or local file system data (in sequential mode).


Running MG-FSM
--------------

The input sequence file(s) should be of the following format:
>
>
>       s1  Central Park is the best place to be on a sunny day in New York
>       s2  Monday was a sunny day in New York
>       s3  It was a sunny and beautiful New York City afternoon
That is, each line represents a sequence, and has the format:
>
>
>       SequenceId item1 item2 item3 ... itemN
In this case, the first token of each line (whitespace delimited) becomes the *sequence id*, and all additional text of the line is interpreted as a sequence of items. For example, the above sequence database contains 3
sequences (s1, s2 and s3). 

### Running in sequential mode
The sequential version of the algorithm runs locally on a single machine. Following example illustrates how to execute in such a mode:
>
>
>       $ MGFSM_HOME/bin/mgfsm -i /path/to/input/dir/ -o /path/to/output/dir/ -s σ -g γ -l λ -m s
When executing the “-(s)equential” mode the input data file(s) should be in “.txt” format (i.e. “.txt”
extension should be present in the name) for proper execution.

### Running in distributed mode
To execute the algorithm on a Hadoop cluster, issue the “distributed” mode to the algorithm. Following
example illustrates:
>
>
>       $ MGFSM_HOME/bin/mgfsm -i /path/to/input/dir/ -o /path/to/output/dir/ -s σ -g γ -l λ -m d
Note: The current version of MG-FSM is tested with *Hadoop 2.5.0* and *Hadoop 2.6.0*.

### Output format
Frequent sequences are written to a file in the output directory, where each line in the file has the following format:
>
>
>       sequence <tab> #frequency

Example
-------
Assume the following input sequences are in a file(s) (.txt) in a directory called SAMPLE_INPUT/
>
>
>       s1 Central Park is the best place to be on a sunny day in New York
>       s2 Monday was a sunny day in New York
>       s3 It was a sunny and beautiful New York City afternoon

Run the following command to find frequent all subsequences with support (σ = 3), maximum length (λ = 3)
and maximum gap (γ = 2).
>
>
>       $ MGFSM_HOME/bin/mgfsm -i SAMPLE_INPUT/ -o SAMPLE_OUTPUT/ -s 3 -g 2 -l 3 -m s

Sample output:
>
>
>       sunny New York      3
>       a sunny New         3
>       New York            3
>       sunny New           3
>       a sunny             3

## Command line options

| Option            | Short Hand 	| Optional 	| Default Value 	| Description  |
|---------------	|------------	|----------	|---------------	|--------------------------------------------------------------------------------------	|
| support       	| s          	| Yes      	| 1             	| The minimum number of times the sequence to be mined must be present in the Database   	|
| gamma         	| g          	| Yes      	| 2             	| The maximum amount of gap that can be taken for a sequence to be mined by MG-FSM    |
| lambda        	| l          	| Yes      	| 5             	| The maximum length of the sequence to be mined is determined by the this parameter.    |
| execMode      	| m          	| Yes      	| s             	| Method of execution viz. (s)equential or (d)istributed |
| type          	| t          	| Yes      	| a             	| Specify the output type.  Expected values for type: 1. (a)ll 2. (m)aximal 3. (c)losed  |
| input         	| i          	| No      	| -             	| Path where the input transactions / database text file is located.                   	|
| output        	| o          	| No       	| -             	| Path where the output files are to written.   
| encodeOnly		| eo			| Yes		| -					| If this flag is given, only the encoding phase is executed. MG-FSM then stores the dictionary and the encoded sequences in the specified output folder.  			|
| mineOnly			| mo			| Yes		| -					| If this flag is given, only the mining phase is executed. MG-FSM then uses the dictionary and the encoded sequences at the specified input folder and mines the sequences with the given parameters. 		|	
| keepEncoded     	| k          	| Yes      	| -             	| Specify whether to store the intermediary files for later use or runs. If no parameter is passed, the intermediary files are stored in a sub directory of the output folder. If a path is passed as a parameter, the files will be stored at the passed path. The files stored are: 1. Dictionary 2. Encoded Sequences 	| 
| tempDir       	| tempDir    	| Yes      	| -             	| Specify the temporary directory to be used for the map– reduce jobs   	|
| numReducers   	| N          	| Yes      	| 90            	| Specify the number of reduce task.        	|
| partitionSize 	| p          	| Yes      	| 10000         	| Explicitly specify the partition size.      |
| indexing      	| id         	| Yes      	| full          	| Specify the indexing mode. Options are : 1. none 2. minmax 3. full        	|
| split         	| sp         	| Yes      	| false         	| Explicitly specify whether or not to allow split by setting this flag.   	|
| timestampInput   	| ti         	| Yes      	| false         	| Specify to use timestamp-encoded input format     	|
| temporalGap   	| tg         	| Yes      	| 0         		| Set a temporal gap for the timestamp-encoded input. Required when using -timestampInput.  	|

**Additional Notes**

+   -(k)eepEncoded and -(m)ine(O)nly options are mutually exclusive.
+   -(i)nput and -(r)esume are mutually exclusive.
+   -(o)utput and (i)nput are the only mandatory option.
+   -temporalGap (-tg) is mandatory when using -timestampInput. -(g)amma values will be ignored when using -timestampInput

## Examples
### Running MG-FSM on timestamp-encoded input

Timestamp-encoded is in the format 
>		sequenceID timestamp1 item1 timestamp2 item2 ... timestampN itemN

The timestamps can be given in arbitrary units, e.g. days, hours, weeks. You can use the **temporalGap** parameter to specify the interval in which you want to mine frequent sequences. MG-FSM then internally calculates the according gamma value. 



[1]: http://www.mpi-inf.mpg.de/departments/databases-and-information-systems/software/mg-fsm/      "Mgfsm"
[2]: http://resources.mpi-inf.mpg.de/d5/mg-fsm/mg-fsm-sigmod13.pdf "SIGMOD paper"
[3]: http://maven.apache.org/ "Maven"

[4]: http://irismili.wordpress.com/
[5]: http://people.mpi-inf.mpg.de/~kberberi/
[6]: http://dws.informatik.uni-mannheim.de/en/people/professors/prof-dr-rainer-gemulla/
[7]: http://people.mpi-inf.mpg.de/~kbeedkar/
[8]: mailto:dhgupta@mpi-inf.mpg.de
[9]: mailto:arenzwie@mail.uni-mannheim.de
