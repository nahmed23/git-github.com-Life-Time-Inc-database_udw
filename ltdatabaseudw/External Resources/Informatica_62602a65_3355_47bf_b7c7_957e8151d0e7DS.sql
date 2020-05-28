CREATE EXTERNAL DATA SOURCE [Informatica_62602a65_3355_47bf_b7c7_957e8151d0e7DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

