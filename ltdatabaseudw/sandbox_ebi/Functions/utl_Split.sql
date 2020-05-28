CREATE FUNCTION [sandbox_ebi].[utl_Split] (@str [VARCHAR](8000),@substr [VARCHAR](255) = ' ',@occurrence [INT]) RETURNS varchar(255)
AS
BEGIN

-- Declare variables and place-holders
DECLARE @found INT = @occurrence,
@word VARCHAR(8000),
@text VARCHAR(100),
@end int;

-- Start an infinite loop that will only end when the Nth word is found
WHILE 1=1
BEGIN
IF @found = 1
BEGIN
SET @end = CHARINDEX(@substr, @str)
IF @end IS NULL or @end = 0
BEGIN
SET @end = LEN(@str)
END
SET @text = LEFT(@str,@end)
BREAK
END;
-- If the selected word is beyond the number of words, NULL is returned
IF CHARINDEX(@substr, @str) IS NULL or CHARINDEX(@substr, @str) = 0
BEGIN
SET @text = NULL;
BREAK;
END

-- Each iteration of the loop will remove the first word fromt the left
SET @str = RIGHT(@str, LEN(@str)-CHARINDEX(@substr, @str));
SET @found = @found - 1
END

RETURN @text;
END