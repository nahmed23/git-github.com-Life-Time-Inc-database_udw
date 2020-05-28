CREATE EXTERNAL DATA SOURCE [Informatica_eaae31a1_e9c7_4eb4_bf82_33e6eba3d6d4DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

