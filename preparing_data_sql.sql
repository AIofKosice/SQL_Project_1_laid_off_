-- Cleaning

SELECT *
FROM layoffs;

-- What we need
-- 1. Remove duplicates
-- 2. Standardize the data 
-- 3. Null values or blank values
-- 4. Remove not needed columns or rows(NULL || blank)









-- 1. Remove duplicates

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT  *
FROM layoffs;


Select *
from layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) as row_num
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = "Cazoo";

DELETE 
FROM duplicate_cte
WHERE row_num > 1;

DROP TABLE IF EXISTS `layoffs_staging_2`;
CREATE TABLE `layoffs_staging_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging_2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) as row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging_2
WHERE row_num > 1;

DELETE
FROM layoffs_staging_2
WHERE row_num > 1;














-- 2. Standardize the data 

SELECT company, TRIM(company)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET company = TRIM(company);

SELECT DISTINCT(industry), TRIM(industry)
FROM layoffs_staging_2
ORDER BY 1;

UPDATE layoffs_staging_2
SET industry = TRIM(industry);

SELECT location, TRIM(location)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET location = TRIM(location);

UPDATE layoffs_staging_2
SET location = TRIM(location);

SELECT *
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET stage = TRIM(stage);

UPDATE layoffs_staging_2
SET country = TRIM(country);

SELECT DISTINCT country
FROM layoffs_staging_2
order by 1;

SELECT `date`, 
CAST(`DATE` AS DATE)
FROM layoffs_staging_2;

SELECT `date`, 
STR_TO_DATE(`date`, "%Y-%m-%dT%H:%i:%s.%fZ")
FROM layoffs_staging_2;

/*
UPDATE layoffs_staging_2
SET `date` = CAST(REPLACE(`date`, 'Z', '') AS DATE);

UPDATE layoffs_staging_2
SET `date` = STR_TO_DATE(`date`, "%Y-%m-%dT%H:%i:%s.%fZ");
*/

SELECT *
FROM layoffs_staging_2
WHERE `date` = '0.2';

UPDATE layoffs_staging_2
SET `date` = null
WHERE `date` = '0.2' OR `date` = '0.05';


START TRANSACTION;
UPDATE layoffs_staging_2
SET `date` = NULL
WHERE `date` = '0.2'
OR `date` = '0.05'
OR `date` = '0.3';

COMMIT;
UPDATE layoffs_staging_2
SET `date` = CAST(REPLACE(`date`, 'Z', '') AS DATE) ;

SELECT `date`
FROM layoffs_staging_2
WHERE `date` = CAST(REPLACE(`date`, 'Z', '') AS DATE) IS NULL;

SELECT `date`, 
CAST(`DATE` AS DATE)
FROM layoffs_staging_2;
START TRANSACTION;
COMMIT;
START TRANSACTION;
ALTER TABLE layoffs_staging_2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging_2;

COMMIT;






-- 3. Null values or blank values

START TRANSACTION;
SELECT * 
FROM layoffs_staging_2;


SELECT * 
FROM layoffs_staging_2
WHERE `date` IS NULL;

SELECT * 
FROM layoffs_staging_2
WHERE location IS NULL
OR location = '';	

SELECT * 
FROM layoffs_staging_2
WHERE company  = 'Product Hunt';

UPDATE layoffs_staging_2
SET industry = NULL
WHERE industry = '';

-- we dont need this rows for our analysis
DELETE 
FROM layoffs_staging_2
WHERE (total_laid_off is NULL or total_laid_off = '')
AND (percentage_laid_off is NULL or percentage_laid_off = '');

SELECT * 
FROM layoffs_staging_2
WHERE (total_laid_off is NULL or total_laid_off = '')
OR (percentage_laid_off is NULL or percentage_laid_off = '');


SELECT *
FROM layoffs_staging_2 t1
JOIN layoffs_staging_2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL AND t2.industry != '';


SELECT *
FROM layoffs_staging_2
WHERE company = 'Appsmith';

SELECT *
FROM layoffs_staging_2
WHERE country = '';

SELECT *
FROM layoffs_staging_2
WHERE (company, location) IN(
SELECT company, location
FROM layoffs_staging_2
GROUP BY company, location
HAVING COUNT(*) > 1
);

ALTER TABLE layoffs_staging_2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging_2
WHERE company IS NULL OR company = ''
   OR location IS NULL OR location = ''
   OR industry IS NULL OR industry = ''
   /*
   OR total_laid_off IS NULL OR total_laid_off = ''
   OR percentage_laid_off IS NULL OR percentage_laid_off = ''
   */
   OR `date` IS NULL
   OR stage IS NULL OR stage = ''
   OR country IS NULL OR country = ''
   OR funds_raised IS NULL OR funds_raised = '';
   
UPDATE layoffs_staging_2
SET 
    company = CASE WHEN company = '' THEN NULL ELSE company END,
    location = CASE WHEN location = '' THEN NULL ELSE location END,
    industry = CASE WHEN industry = '' THEN NULL ELSE industry END,
    total_laid_off = CASE WHEN total_laid_off = '' THEN NULL ELSE total_laid_off END,
    percentage_laid_off = CASE WHEN percentage_laid_off = '' THEN NULL ELSE percentage_laid_off END,
    stage = CASE WHEN stage = '' THEN NULL ELSE stage END,
    country = CASE WHEN country = '' THEN NULL ELSE country END,
    funds_raised = CASE WHEN funds_raised = '' THEN NULL ELSE funds_raised END;



SELECT *
FROM layoffs_staging_2
WHERE company = 'Longi';

UPDATE layoffs_staging_2
SET country = 'China',
funds_raised = NULL
WHERE company = 'Longi';

SELECT *
FROM layoffs_staging_2
WHERE total_laid_off = 'other';


SELECT *
FROM layoffs_staging_2
WHERE company = 'Torii';

UPDATE layoffs_staging_2
SET total_laid_off = NULL
WHERE company = 'Torii';

SELECT *
FROM layoffs_staging_2
WHERE total_laid_off NOT REGEXP '^[0-9]+.[0-9]?$';

UPDATE layoffs_staging_2
SET total_laid_off = NULL
WHERE total_laid_off NOT REGEXP '^[0-9]+.[0-9]?$';

SELECT *
FROM layoffs_staging_2
WHERE funds_raised NOT REGEXP '^[0-9]+.[0-9]?$';

UPDATE layoffs_staging_2
SET funds_raised = NULL
WHERE funds_raised NOT REGEXP '^[0-9]+.[0-9]?$';

UPDATE layoffs_staging_2
SET percentage_laid_off = percentage_laid_off * 100;

ALTER TABLE layoffs_staging_2
MODIFY COLUMN total_laid_off DECIMAL(10, 2),
MODIFY COLUMN percentage_laid_off DECIMAL(10, 2),
MODIFY COLUMN funds_raised DECIMAL(10, 2);


SELECT *
FROM layoffs_staging_2;

COMMIT;