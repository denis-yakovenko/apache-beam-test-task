Task:

1) read user.avro and ageString.avro files
2) if a record contains age less then 20 you should save this record in error folder and add new "error" field to this record with the reason why it happened
3) If a record contains not "Gmail" email you should save this record in error folder and add new "error" field to this record with a reason why it happened
4) if record pass validation you need to cut substring before the @ sign from email field and give a name for this field like "shortUserName"
5) join two avro files: user records with ageString records using "sideInput" from Apache Beam. If the join fails, then indicate the ageString field as "unknown".
The names for the output files can be arbitrary

Input files user.avro and ageString.avro located at the "input" folder.  
Output files will be stored at the "output" folder.  
Error files will be stored at "error" folder.  

How to run:  
gradle clean execute -D mainClass=org.apache.beam.example.TestTask -P spark-runner
