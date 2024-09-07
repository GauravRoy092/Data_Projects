-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- deleting dupilcate things

SELECT * 
FROM layoffs;


CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;
SELECT * 
FROM layoffs_staging;

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
    
    
    
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
where row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
row_number() over(
partition by company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


select *
FROM layoffs_staging2
where row_num > 1;
DELETE FROM layoffs_staging2
WHERE row_num > 1;

select *
FROM layoffs_staging2
where row_num > 1;

-- deleting dupilcate things is DONE

-- NOW standardizing things

SELECT distinct(company)
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

update layoffs_staging2
set company = TRIM(company);

Select *
from layoffs_staging2
where industry Like 'Crypto%';

SELECT distinct(country)
FROM layoffs_staging2
where country like 'United States%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

SELECT DISTINCT (country), TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States %';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

select `date`,
STR_TO_DATE (`date`, '%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y');

select *
from layoffs_staging2;

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';


