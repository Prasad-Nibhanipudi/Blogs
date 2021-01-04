# Blogs

# This implementation provide a single class utility 
Execute IDGenerator to get a file caseInsenstive.txt

# sort the file
sort --parallel=8 -uo caseInsensitiveSorted.txt caseInsensitive.txt 

# Pick non Unique IDs to identify duplicate ID's
uniq -c caseInsensitiveSorted.txt | grep -v '^ *1 '


# Run the IDGeneratorMultiThreadedTest to create and check id's how far they are