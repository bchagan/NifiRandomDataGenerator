# NifiRandomDataGenerator

This project is an implementation of Nifi's AbstractProcessor.

I designed this to help me generate data for Nifi flows. Nifi does contain
a GenerateData processor, but it does not provide the functionality that I generally look 
for when I want some quick data. 

This processor performs the following functionality:

- Generates the following fields:
    - metric - A random integer or double within a given range (inclusive)
    - metric name
    - min - minimum integer for the range
    - max - maximum integer for the range
    - output format - json or csv
    - identifier - A configurable domain specific identifier
    - timestamp - A timestamp for the event
- Provides ability to set the double's precision
- Provides ability to set output as json or csv
