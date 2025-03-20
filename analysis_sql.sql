SELECT *
FROM layoffs_staging_2;

SELECT *
FROM layoffs_staging_2
WHERE total_laid_off = 
(
SELECT 
MAX(total_laid_off) FROM layoffs_staging_2
);

SELECT *
FROM layoffs_staging_2
WHERE funds_raised = 
(
SELECT 
MAX(funds_raised) FROM layoffs_staging_2
);

SELECT *
FROM layoffs_staging_2
WHERE percentage_laid_off > 80
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging_2
WHERE percentage_laid_off > 80
ORDER BY funds_raised DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging_2;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging_2
WHERE `date` BETWEEN '2022-01-1' AND '2022-12-31'
GROUP BY company
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY country
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`, 1, 7 ) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging_2
WHERE SUBSTRING(`date`, 1, 7 ) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 2 DESC;


SELECT SUM(total_laid_off)
FROM layoffs_staging_2;

WITH Total AS
(
SELECT SUBSTRING(`date`, 1, 7 ) AS `MONTH`, SUM(total_laid_off) AS total_laid
FROM layoffs_staging_2
WHERE SUBSTRING(`date`, 1, 7 ) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 2 DESC
)
SELECT `MONTH`, total_laid,  SUM(total_laid) OVER(ORDER BY `MONTH`) AS Cumullative_total_laid
FROM Total;
