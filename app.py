#! /usr/big/env python

from avro import schema, datafile, io
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-s', '--source', dest='source',
        help='The source csv file')
parser.add_option('-d', '--dest', dest='dest',
        help='The destination avro file')
parser.add_option('-x', '--schema', dest='schema',
        help='The schema file for parsing the csv into the avro file')
parser.add_option('-n', '--header', dest='header',
        help='If set, skips the first row because it is a header row',
        action='store_false', default=False)

(opts, args) = parser.parse_args()

mandatory = ['source', 'dest', 'schema']
for m in mandatory:
    if not opts.__dict__[m]:
        print 'Mandatory option' + m + 'is missing\n'
        parser.print_help()
        exit(-1)

OUTFILE_NAME = opts.dest
INFILE_NAME = opts.source
SCHEMA_FILE = open(opts.schema, 'r')
START_LINE = [0,1][opts.header]

SCHEMA_STR = SCHEMA_FILE.read()

SCHEMA = schema.parse(SCHEMA_STR)

rec_writer = io.DatumWriter(SCHEMA)

df_writer = datafile.DataFileWriter(
    open(OUTFILE_NAME, 'wb'),
    rec_writer,
    writers_schema = SCHEMA
)

in_file = open(INFILE_NAME, 'r')

lines = in_file.read().splitlines()
for num,line in enumerate(lines[START_LINE:]):
    parts = line.split(',')

    avro_parts = dict()
    for num, field in enumerate(SCHEMA.fields):
        avro_parts[field.name] = parts[num]

    df_writer.append(avro_parts)
