Avro Converter
==============

This is a converter to turn .csv data files into .avro files

### To use

```python
  python app.py -s SOURCE_FILE -d DESTINATION_FILE -x SCHEMA_FILE [-n]
```

### Options
-s: Relative source csv file
-d: File to save the avro data in
-x: Schema file to load the structure of the avro file.
-n: (OPTIONAL) parameter describing if the csv file has a header row.
