CREATE EXTERNAL DATA SOURCE [Informatica_63c351bd_4aa7_4854_956d_7400bef321b9DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

