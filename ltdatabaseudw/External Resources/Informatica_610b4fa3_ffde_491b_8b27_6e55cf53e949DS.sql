CREATE EXTERNAL DATA SOURCE [Informatica_610b4fa3_ffde_491b_8b27_6e55cf53e949DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

