# NifiRandomDataGenerator

This project is an implementation of Nifi's AbstractProcessor. I used GenerateFlowFile, 
AttributesToJson, and AttributesToCSV as models for this processor.

I designed this to help me generate data for Nifi flows. Nifi does contain
a GenerateFlowFile processor, but it does not provide the functionality that I generally look 
for when I want some quick data. 

This processor performs the following functionality:

- Generates the following fields:
    - metric - A random integer or double within a given range (inclusive)
    - metric name
    - min - minimum integer for the range
    - max - maximum integer for the range
    - identifier - An identifier between 1-100
    - timestamp - A timestamp for the event
    - lname - last name (optional and randomly generated from 100 names)
    - fname - first name (optional and randomly generated from 100 names)
- Provides ability to set the double's precision
- Provides ability to set output as json or csv

Sample json output:

<pre lang="json">
{
  "min" : "80",
  "max" : "90",
  "temp" : "83.5",
  "deviceID" : "47",
  "timestamp" : "2020-10-23 13:49:26.925",
  "lname" : "Zocchi",
  "fname" : "Michele"
}
</pre>
