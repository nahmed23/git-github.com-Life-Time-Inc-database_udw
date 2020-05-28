CREATE EXTERNAL DATA SOURCE [Informatica_492fb18c_4585_4aa1_8cc6_e3fef84e4649DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

