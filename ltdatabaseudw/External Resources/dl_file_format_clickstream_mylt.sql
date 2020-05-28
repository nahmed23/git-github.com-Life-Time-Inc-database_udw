CREATE EXTERNAL FILE FORMAT [dl_file_format_clickstream_mylt]
    WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (FIELD_TERMINATOR = N'\t', DATE_FORMAT = N'yyyy-MM-dd HH:mm:ss', FIRST_ROW = 2, ENCODING = N'UTF8'),
    DATA_COMPRESSION = N'org.apache.hadoop.io.compress.DefaultCodec'
    );

