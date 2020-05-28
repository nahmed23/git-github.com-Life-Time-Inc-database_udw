CREATE EXTERNAL DATA SOURCE [Informatica_9b9b5e34_2842_475d_a2d8_56c45b8570fdDS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

