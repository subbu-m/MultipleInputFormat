MultipleInputFormat
===================

Usage of Multiple Input Format in Map Reduce
 
The main use of Multiple Input Format is to process two or more input files with different formats through Map Reduce Programming. 

Here in my example program given, I am doing word count on two different format files - space delimited (inputs) and comma delimited (inputc). Since input data of two files is different we are processing both in two map programs one for space delimited and another for comma delimited. The reducer program remains same since the output of Mapper will remain same for both.

Mapper for space delimited file is MulInpFormatsMap.java

Mapper for comma delimited file is MulInpFormatcMap.java

The below is the syntax to use Multiple Input Format in Driver Program.

		MultipleInputs.addInputPath(wcJob, new Path(args[0]), TextInputFormat.class, MulInpFormatcMap.class);
		MultipleInputs.addInputPath(wcJob, new Path(args[1]), TextInputFormat.class, MulInpFormatsMap.class);
