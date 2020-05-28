CREATE EXTERNAL DATA SOURCE [Informatica_95c23ccb_bc27_4dfd_8144_33f6140c947fDS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

