CREATE EXTERNAL DATA SOURCE [Informatica_d0696a67_a3c7_4327_9ce5_539aac134b3bDS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

