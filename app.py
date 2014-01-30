#! /usr/big/env python

from avro import schema, datafile, io
import pprint

OUTFILE_NAME = 'data/regular_season_stats.avro'
INFILE_NAME = 'csv/regular_season_results.csv'

pp = pprint.PrettyPrinter()

SCHEMA_STR = """{
    "type": "record",
    "name": "Game",
    "fields": [
        { "name": "season", "type": "string"},
        { "name": "daynum", "type": "string"},
        { "name": "wteam_id", "type": "string"},
        { "name": "wscore", "type": "string"},
        { "name": "lteam_id", "type": "string"},
        { "name": "lscore", "type": "string"},
        { "name": "wloc", "type": "string"},
        { "name": "numot", "type": "string"}
    ]
}"""

SCHEMA = schema.parse(SCHEMA_STR)
rec_writer = io.DatumWriter(SCHEMA)

df_writer = datafile.DataFileWriter(
    open(OUTFILE_NAME, 'wb'),
    rec_writer,
    writers_schema = SCHEMA
)

in_file = open(INFILE_NAME, 'r')

lines = in_file.read().splitlines()
for num,line in enumerate(lines[1:]):
    parts = line.split(',')

    avro_parts = dict({
        'season': parts[0],
        'daynum': parts[1],
        'wteam_id': parts[2],
        'wscore': parts[3],
        'lteam_id': parts[4],
        'lscore': parts[5],
        'wloc': parts[6],
        'numot': parts[7]
    })
    df_writer.append(avro_parts)
