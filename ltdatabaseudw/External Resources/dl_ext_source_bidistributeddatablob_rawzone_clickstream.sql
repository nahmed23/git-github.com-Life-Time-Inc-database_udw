CREATE EXTERNAL DATA SOURCE [dl_ext_source_bidistributeddatablob_rawzone_clickstream]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://rawzone-clickstream@bidistributeddatablob.blob.core.windows.net',
    CREDENTIAL = [dl_storagecredential_blob_bidistributeddatablob]
    );

