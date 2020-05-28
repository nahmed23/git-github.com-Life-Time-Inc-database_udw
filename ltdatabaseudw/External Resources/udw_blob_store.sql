CREATE EXTERNAL DATA SOURCE [udw_blob_store]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [UdwCred]
    );

