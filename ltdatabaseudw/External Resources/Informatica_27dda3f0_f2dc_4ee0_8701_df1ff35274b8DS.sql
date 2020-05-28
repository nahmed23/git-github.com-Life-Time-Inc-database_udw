CREATE EXTERNAL DATA SOURCE [Informatica_27dda3f0_f2dc_4ee0_8701_df1ff35274b8DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

