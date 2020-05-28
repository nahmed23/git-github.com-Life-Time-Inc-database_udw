CREATE EXTERNAL DATA SOURCE [Informatica_36ff7f69_6a70_496e_996b_fe49034e6e49DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

