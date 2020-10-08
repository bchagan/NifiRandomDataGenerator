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
- Provides ability to set the double's precision
- Provides ability to set output as json or csv

Sample json output:

<pre lang="json">
{
  "min" : "80",
  "max" : "90",
  "temp" : "82.2",
  "deviceID" : "91",
  "timestamp" : "2020-10-07 17:58:10.409"
}
</pre>
