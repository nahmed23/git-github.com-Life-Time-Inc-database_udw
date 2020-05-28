CREATE EXTERNAL DATA SOURCE [Informatica_ab7ca80c_6849_4bee_9d57_090d9fa58d16DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

