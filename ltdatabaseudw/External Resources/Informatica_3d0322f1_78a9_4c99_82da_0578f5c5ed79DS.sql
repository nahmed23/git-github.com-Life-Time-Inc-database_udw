CREATE EXTERNAL DATA SOURCE [Informatica_3d0322f1_78a9_4c99_82da_0578f5c5ed79DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

