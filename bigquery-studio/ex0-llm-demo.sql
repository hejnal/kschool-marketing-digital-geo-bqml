--create new flash model
CREATE OR REPLACE MODEL
  `llm_models.gemini_flash_model` REMOTE
WITH CONNECTION `us.llm-conn` OPTIONS(ENDPOINT = 'gemini-2.0-flash-001');

-- basic joke in BQ
SELECT
  JSON_VALUE(ml_generate_text_result['candidates'][0]['content']['parts'][0]['text'], '$') AS result,
FROM
  ML.GENERATE_TEXT( MODEL `llm_models.gemini_flash_model`,
    (
    SELECT
      "tell me joke" AS prompt),
    STRUCT( 0.0 AS temperature,
      1000 AS max_output_tokens));

-- full transformation
WITH
  avg_birthdate AS (
  SELECT
    JSON_VALUE(ml_generate_text_result['candidates'][0]['content']['parts'][0]['text'], '$') AS avg_dob,
    CAST(prompt AS STRING) AS prompt,
    dob1,
    dob2,
    dob3
  FROM
    ML.GENERATE_TEXT( MODEL `llm_models.gemini_flash_model`,
      (
      SELECT
        FORMAT("You are a helpful assistant knowing well how to use calendar. Your job is to return an average birthday of 3 people Juan, Julia and Pedro. Hint: first convert the birthday to day of year and then get the average of all values. Assume non leap year. Example: Input: Juan: 05/03, Julia: 17/01, Pedro: 04/02. Response: 08/02. Skip the whole explanation and calculation description. Only output a date in the DD/MM format. Input Juan: %s, Julia: %s, Pedro: %s. Response: )", dob1, dob2, dob3) AS prompt,
        dob1,
        dob2,
        dob3
      FROM
        llm_demo.birthdays),
      STRUCT( 0.0 AS temperature,
        1000 AS max_output_tokens)) )
SELECT
  ab.dob1,
  ab.dob2,
  ab.dob3,
  ab.avg_dob,
  JSON_VALUE(ml_generate_text_result['candidates'][0]['content']['parts'][0]['text'], "$") AS artist,
FROM
  ML.GENERATE_TEXT( MODEL `llm_models.gemini_flash_model`,
    (
    SELECT
      FORMAT("Return famous musician name who's birthday is on %s. Return only name and lastname. For example for input: 25/01 return output: Etta James. Remove quotes from the response.", ab.avg_dob) AS prompt,
      ab.dob1,
      ab.dob2,
      ab.dob3,
      ab.avg_dob,
      STRUCT( 0.0 AS temperature,
        10 AS max_output_tokens,
      [STRUCT('HARM_CATEGORY_HATE_SPEECH' AS category,
        'BLOCK_ONLY_HIGH' AS threshold),
      STRUCT('HARM_CATEGORY_DANGEROUS_CONTENT' AS category,
        'BLOCK_ONLY_HIGH' AS threshold),
      STRUCT('HARM_CATEGORY_HARASSMENT' AS category,
        'BLOCK_ONLY_HIGH' AS threshold),
      STRUCT('HARM_CATEGORY_SEXUALLY_EXPLICIT' AS category,
        'BLOCK_ONLY_HIGH' AS threshold)] AS safety_settings)
    FROM
      avg_birthdate ab)) fm
JOIN
  avg_birthdate ab
ON
  fm.dob1 = ab.dob1
  AND fm.dob2 = ab.dob2
  AND fm.dob3 = ab.dob3;

-- new version (AI.GENERATE)
-- https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-ai-generate

SELECT
  AI.GENERATE(
    "tell me joke",
    connection_id => 'us.llm-conn',
    endpoint => 'gemini-2.0-flash').result;

-- full example
WITH 
avg_birthdate AS (
    SELECT
  dob1,
  dob2,
  dob3,
  AI.GENERATE(
    FORMAT("You are a helpful assistant knowing well how to use calendar. Your job is to return an average birthday of 3 people Juan, Julia and Pedro. Hint: first convert the birthday to day of year and then get the average of all values. Assume non leap year. Example: Input: Juan: 05/03, Julia: 17/01, Pedro: 04/02. Response: 08/02. Skip the whole explanation and calculation description. Only output a date in the DD/MM format. Input Juan: %s, Julia: %s, Pedro: %s. Response: )", dob1, dob2, dob3),
    connection_id => 'us.llm-conn',
    endpoint => 'gemini-2.0-flash').result AS avg_dob
FROM llm_demo.birthdays) 
SELECT 
dob1,
dob2,
dob3,
avg_dob,
AI.GENERATE(
    FORMAT("Return famous musician name who's birthday is on %s. Return only name and lastname. For example for input: 25/01 return output: Etta James. Remove quotes from the response.", avg_dob),
    connection_id => 'us.llm-conn',
    endpoint => 'gemini-2.0-flash').result AS artist
FROM avg_birthdate;