-- checking the data in the query;

SELECT * FROM dataset;

--splitting ARTISTS column into separated columns

 -- 1. String-split() - split comma separated string into single column table
 -- 2. Pivot - transform row level data into column data
 -- 3. Cross APPLY returns only rows from the outer table that produce a result set from table_valued function 

 SELECT
	row_id, artists, ISNULL([artist1],' ') as artist_1, ISNULL([artist2],' ') as artist_2, ISNULL([artist3],' ') as artist_3,
	ISNULL([artist4],' ') as artist_4, ISNULL([artist5],' ') as artist_5, ISNULL([artist6],' ') as artist_6, 
	ISNULL([artist7],' ') as artist_7, ISNULL([artist8],' ') as artist_8, ISNULL([artist9],' ') as artist_9,
	ISNULL([artist10],' ') as artist_10
	FROM(
		SELECT 
			row_id, artists, 'artist' + CAST (ROW_NUMBER() OVER(PARTITION BY row_id ORDER BY row_id) AS VARCHAR) AS col, 
			Split.VALUE FROM dataset -- splitting row
		CROSS APPLY STRING_SPLIT(artists,';') AS Split -- applying cross apply
) AS tbl 

PIVOT (MAX(VALUE) FOR col in ([artist1],[artist2],[artist3],[artist4],[artist5], [artist6],[artist7],
		[artist8],[artist9],[artist10])) as PVT -- pivoting the table

-- creating and updating data into the main table
ALTER TABLE dataset
 ADD 
	artist_1 VARCHAR (255),
	artist_2 VARCHAR (255),
	artist_3 VARCHAR (255),
	artist_4 VARCHAR (255),
	artist_5 VARCHAR (255),
	artist_6 VARCHAR (255),
	artist_7 VARCHAR (255),
	artist_8 VARCHAR (255),
	artist_9 VARCHAR (255),
	artist_10 VARCHAR (255);

-- Using CTE as helper to be able to join retrieval data into main table
WITH CTE AS(
SELECT
	row_id, artists, ISNULL([artist1],' ') as artist_1, ISNULL([artist2],' ') as artist_2, ISNULL([artist3],' ') as artist_3,
	ISNULL([artist4],' ') as artist_4, ISNULL([artist5],' ') as artist_5, ISNULL([artist6],' ') as artist_6, 
	ISNULL([artist7],' ') as artist_7, ISNULL([artist8],' ') as artist_8, ISNULL([artist9],' ') as artist_9,
	ISNULL([artist10],' ') as artist_10
	FROM(
		SELECT 
			row_id, artists, 'artist' + CAST (ROW_NUMBER() OVER(PARTITION BY row_id ORDER BY row_id) AS VARCHAR) AS col, 
			Split.VALUE FROM dataset -- splitting row
		CROSS APPLY STRING_SPLIT(artists,';') AS Split -- applying cross apply
) AS tbl 

PIVOT (MAX(VALUE) FOR col in ([artist1],[artist2],[artist3],[artist4],[artist5], [artist6],[artist7],
		[artist8],[artist9],[artist10])) as PVT -- pivoting the table
	)

UPDATE dataset
SET 
	artist_1 = CTE.artist_1,
	artist_2 = CTE.artist_2,
	artist_3 = CTE.artist_3,
	artist_4 = CTE.artist_4,
	artist_5 = CTE.artist_5,
	artist_6 = CTE.artist_6,
	artist_7 = CTE.artist_7,
	artist_8 = CTE.artist_8,
	artist_9 = CTE.artist_9,
	artist_10 = CTE.artist_10
FROM dataset
JOIN CTE ON dataset.row_id = CTE.row_id;

-- converting DURATION (milisecond to minute) by dividing to 60000
-- adding tailing 0 to the column to form uniformity
SELECT 
	duration_ms,
	FORMAT(ROUND(duration_ms/60000,2),'0.00') AS duration_min
FROM dataset;

-- updating and converting data type to main dataset
ALTER TABLE dataset
ADD duration_min DECIMAL(5,2);

UPDATE dataset
SET duration_min = FORMAT(ROUND(duration_ms/60000,2),'0.00');



-- Appending trailing  0 to the DANCEABILITY column to achieve uniformity in the format
SELECT 
	danceability,
	CAST(FORMAT(danceability,'0.000')AS DECIMAL (4,3)) AS danceability_rv
FROM dataset;

-- updating and converting data type to main dataset
ALTER TABLE dataset
ADD danceability_rv DECIMAL (4,3);

UPDATE dataset
SET danceability_rv = CAST(FORMAT(danceability,'0.000')AS DECIMAL (4,3));



-- rounding, converting, and appending tailing zero to the ENERGY column to achieve uniformity in the format
-- converting data type from FLOAT to DECIMAL to get precise value
SELECT
	energy,
	CAST(FORMAT(ROUND(energy,3),'0.000')AS DECIMAL (4,3)) AS energy_rv
FROM dataset;

-- updating and converting data type to main dataset

ALTER TABLE dataset
ADD energy_rv DECIMAL (4,3);

UPDATE dataset
SET energy_rv = CAST(FORMAT(ROUND(energy,3),'0.000')AS DECIMAL (4,3));


--rounding and appending tailing 0 to LOUDNESS column to achieve the uniformity
-- converting data type from FLOAT to DECIMAL to get precise value

SELECT 
	loudness,
	 CAST(FORMAT(ROUND(loudness,3),'0.000') AS DECIMAL(6,3)) AS loudness_rv
FROM dataset;

-- updating and converting data type to main dataset
ALTER TABLE dataset
ADD loudness_rv DECIMAL (6,3);

UPDATE dataset
SET loudness_rv = CAST(FORMAT(ROUND(loudness,3),'0.000') AS DECIMAL(6,3));

--rounding and appending tailing 0 to  SPEECHINESS column to achieve the uniformity
-- converting data type from FLOAT to DECIMAL to get precise value
SELECT
	speechiness,
	FORMAT(ROUND(speechiness,3),'0.000') AS speechiness_rv
FROM dataset;

-- adding column and updating to main data table
ALTER TABLE dataset
ADD speechiness_rv  DECIMAL (6,3);

UPDATE dataset
SET speechiness_rv = CAST(FORMAT(ROUND(speechiness,3),'0.000') AS DECIMAL (6,3));


--rounding and appending tailing 0 to  ACOUSTICNESS column to achieve the uniformity
-- converting data type from FLOAT to DECIMAL to get precise value

SELECT 
	acousticness,
	CAST(FORMAT(ROUND(acousticness,5),'0.00000') AS DECIMAL (6,3)) AS acousticness_rv
FROM dataset;

-- adding column and updating to main data table
ALTER TABLE dataset
ADD acousticness_rv  DECIMAL (6,3);

UPDATE dataset
SET acousticness_rv = CAST(FORMAT(ROUND(acousticness,5),'0.00000') AS DECIMAL (6,3));


--rounding and appending tailing 0 to  INSTRUMENTALNESS column to achieve the uniformity
-- converting data type from FLOAT to DECIMAL to get precise value
SELECT
	instrumentalness,
	FORMAT(ROUND(instrumentalness,5),'0.00000') AS instrumentalness_rv
FROM dataset;

-- adding column and updating to main data table
ALTER TABLE dataset
ADD instrumentalness_rv DECIMAL (6,3);

UPDATE dataset
SET instrumentalness_rv = CAST(FORMAT(ROUND(instrumentalness,5),'0.00000') AS DECIMAL (6,3));

--rounding and appending tailing 0 to  LIVENESS column to achieve the uniformity
-- converting data type from FLOAT to DECIMAL to get precise value
SELECT
	liveness,
	FORMAT(ROUND(liveness,3),'0.000') AS liveness_rv
FROM dataset ;

-- adding column and updating to main data table
ALTER TABLE dataset
ADD liveness_rv DECIMAL (6,3);

UPDATE dataset
SET liveness_rv = CAST(FORMAT(ROUND(liveness,5),'0.00000') AS DECIMAL (6,3));

--rounding and appending tailing 0 to  VALENCE column to achieve the uniformity
-- converting data type from FLOAT to DECIMAL to get precise value
SELECT
	valence,
	FORMAT(ROUND(valence,5),'0.00000') AS valence_rv
FROM dataset;

-- adding column and updating to main data table
ALTER TABLE dataset
ADD valence_rv DECIMAL (6,3);

UPDATE dataset
SET valence_rv = CAST(FORMAT(ROUND(valence,5),'0.00000') AS DECIMAL (6,3));


--rounding and appending tailing 0 to  TEMPO column to achieve the uniformity
-- converting data type from FLOAT to DECIMAL to get precise value
SELECT
	tempo,
	FORMAT(ROUND(tempo,3),'0.000') AS tempo_rv
FROM dataset;

-- adding column and updating to main data table
ALTER TABLE dataset
ADD tempo_rv DECIMAL (6,3);

UPDATE dataset
SET tempo_rv = CAST(FORMAT(ROUND(tempo,5),'0.00000') AS DECIMAL (6,3));


-- Fetching selected columns after transformation 
SELECT 
	row_id,
	track_id,
	artists,
	album_name,
	track_name,
	popularity,
	duration_min,
	explicit,
	danceability_rv,
	energy_rv,
	loudness_rv,
	speechiness_rv,
	acousticness_rv,
	instrumentalness_rv,
	liveness_rv,
	valence_rv,
	tempo_rv,
	time_signature,
	track_genre
FROM dataset;

-- Creating temporary table from fetched columns
SELECT 
	row_id,
	artists,
	track_id,
	album_name,
	track_name,
	popularity,
	duration_min,
	explicit,
	danceability_rv,
	energy_rv,
	loudness_rv,
	speechiness_rv,
	acousticness_rv,
	instrumentalness_rv,
	liveness_rv,
	valence_rv,
	tempo_rv,
	time_signature,
	track_genre
INTO #dataset_formatted -- temporary table after formatting
FROM(
	SELECT 
	row_id,
	artists,
	track_id,
	album_name,
	track_name,
	popularity,
	duration_min,
	explicit,
	danceability_rv,
	energy_rv,
	loudness_rv,
	speechiness_rv,
	acousticness_rv,
	instrumentalness_rv,
	liveness_rv,
	valence_rv,
	tempo_rv,
	time_signature,
	track_genre
FROM dataset) AS data_helper;

-- Checking the Temporary table
SELECT * FROM #dataset_formatted

-- calculating z_score to help identifying outliers from dataset:
SELECT
	*,
	(popularity - AVG(popularity) OVER ())/ STDEV(popularity) OVER () AS popularity_zs,
	(duration_min - AVG(duration_min) OVER ())/ STDEV(duration_min) OVER () AS duration_zs,
	(danceability_rv - AVG(danceability_rv) OVER ())/ STDEV(danceability_rv) OVER () AS danceability_zs,
	(energy_rv - AVG(energy_rv) OVER ())/ STDEV(energy_rv) OVER () AS energy_zs,
	(loudness_rv - AVG(loudness_rv) OVER ())/ STDEV(loudness_rv) OVER () AS loudness_zs,
	(speechiness_rv - AVG(speechiness_rv) OVER ())/ STDEV(speechiness_rv) OVER () AS speechiness_zs,
	(acousticness_rv - AVG(acousticness_rv) OVER ())/ STDEV(acousticness_rv) OVER () AS acousticness_zs,
	(instrumentalness_rv - AVG(instrumentalness_rv) OVER ())/ STDEV(instrumentalness_rv) OVER () AS instrumentalness_zs,
	(liveness_rv - AVG(liveness_rv) OVER ())/ STDEV(liveness_rv) OVER () AS liveness_zs,
	(valence_rv - AVG(valence_rv) OVER ())/ STDEV(valence_rv) OVER () AS valence_zs,
	(tempo_rv - AVG(tempo_rv) OVER ())/ STDEV(tempo_rv) OVER () AS tempo_zs
FROM #dataset_formatted;

-- Identifying outliers from dataset
WITH z_score AS (
SELECT
	*,
	(popularity - AVG(popularity) OVER ())/ STDEV(popularity) OVER () AS popularity_zs,
	(duration_min - AVG(duration_min) OVER ())/ STDEV(duration_min) OVER () AS duration_zs,
	(danceability_rv - AVG(danceability_rv) OVER ())/ STDEV(danceability_rv) OVER () AS danceability_zs,
	(energy_rv - AVG(energy_rv) OVER ())/ STDEV(energy_rv) OVER () AS energy_zs,
	(loudness_rv - AVG(loudness_rv) OVER ())/ STDEV(loudness_rv) OVER () AS loudness_zs,
	(speechiness_rv - AVG(speechiness_rv) OVER ())/ STDEV(speechiness_rv) OVER () AS speechiness_zs,
	(acousticness_rv - AVG(acousticness_rv) OVER ())/ STDEV(acousticness_rv) OVER () AS acousticness_zs,
	(instrumentalness_rv - AVG(instrumentalness_rv) OVER ())/ STDEV(instrumentalness_rv) OVER () AS instrumentalness_zs,
	(liveness_rv - AVG(liveness_rv) OVER ())/ STDEV(liveness_rv) OVER () AS liveness_zs,
	(valence_rv - AVG(valence_rv) OVER ())/ STDEV(valence_rv) OVER () AS valence_zs,
	(tempo_rv - AVG(tempo_rv) OVER ())/ STDEV(tempo_rv) OVER () AS tempo_zs
FROM #dataset_formatted)

SELECT
	row_id,
	track_id,
	album_name,
	track_name,
	popularity,
	duration_min,
	explicit,
	danceability_rv,
	energy_rv,
	loudness_rv,
	speechiness_rv,
	acousticness_rv,
	instrumentalness_rv,
	liveness_rv,
	valence_rv,
	tempo_rv,
	time_signature,
	track_genre
FROM z_score
WHERE 
	popularity_zs <-3 OR popularity_zs >3
	OR duration_zs <-3 OR duration_zs > 3
	OR danceability_zs  <-3 OR danceability_zs > 3
	OR energy_zs  <-3 OR energy_zs >3
	OR loudness_zs <-3 OR loudness_zs >3
	OR speechiness_zs <-3 OR speechiness_zs >3
	OR acousticness_zs <-3 OR acousticness_zs >3
	OR instrumentalness_zs <-3 OR instrumentalness_zs >3
	OR liveness_zs <-3 OR liveness_zs >3
	OR valence_zs <-3 OR valence_zs >3
	OR tempo_zs <-3 OR tempo_zs >3
	;

-- Creating new temp table containing only #ouliers data
SELECT
	row_id,
	track_id,
	album_name,
	track_name,
	popularity,
	duration_min,
	explicit,
	danceability_rv,
	energy_rv,
	loudness_rv,
	speechiness_rv,
	acousticness_rv,
	instrumentalness_rv,
	liveness_rv,
	valence_rv,
	tempo_rv,
	time_signature,
	track_genre
INTO #outlier
FROM (
	SELECT
		*,
		(popularity - AVG(popularity) OVER ())/ STDEV(popularity) OVER () AS popularity_zs,
		(duration_min - AVG(duration_min) OVER ())/ STDEV(duration_min) OVER () AS duration_zs,
		(danceability_rv - AVG(danceability_rv) OVER ())/ STDEV(danceability_rv) OVER () AS danceability_zs,
		(energy_rv - AVG(energy_rv) OVER ())/ STDEV(energy_rv) OVER () AS energy_zs,
		(loudness_rv - AVG(loudness_rv) OVER ())/ STDEV(loudness_rv) OVER () AS loudness_zs,
		(speechiness_rv - AVG(speechiness_rv) OVER ())/ STDEV(speechiness_rv) OVER () AS speechiness_zs,
		(acousticness_rv - AVG(acousticness_rv) OVER ())/ STDEV(acousticness_rv) OVER () AS acousticness_zs,
		(instrumentalness_rv - AVG(instrumentalness_rv) OVER ())/ STDEV(instrumentalness_rv) OVER () AS instrumentalness_zs,
		(liveness_rv - AVG(liveness_rv) OVER ())/ STDEV(liveness_rv) OVER () AS liveness_zs,
		(valence_rv - AVG(valence_rv) OVER ())/ STDEV(valence_rv) OVER () AS valence_zs,
		(tempo_rv - AVG(tempo_rv) OVER ())/ STDEV(tempo_rv) OVER () AS tempo_zs
	FROM #dataset_formatted) AS helper
WHERE 
	popularity_zs <-3 OR popularity_zs >3
	OR duration_zs <-3 OR duration_zs > 3
	OR danceability_zs  <-3 OR danceability_zs > 3
	OR energy_zs  <-3 OR energy_zs >3
	OR loudness_zs <-3 OR loudness_zs >3
	OR speechiness_zs <-3 OR speechiness_zs >3
	OR acousticness_zs <-3 OR acousticness_zs >3
	OR instrumentalness_zs <-3 OR instrumentalness_zs >3
	OR liveness_zs <-3 OR liveness_zs >3
	OR valence_zs <-3 OR valence_zs >3
	OR tempo_zs <-3 OR tempo_zs >3
	;

-- Eliminating  the Outliers from #formatted_dataset
DELETE FROM #dataset_formatted
WHERE track_id IN (SELECT track_id FROM #outlier);

-- Checking the duplicates in the dataset
SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY (SELECT 0)) AS duplicate
FROM #dataset_formatted;


-- Eleminating duplicate rows
WITH duplication AS(
SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY (SELECT 0)) AS duplicate
FROM #dataset_formatted)

DELETE FROM  duplication
WHERE duplicate > 1;


-- creating a new table for subsequent analysis to simplify the process

SELECT
	track_id,
	artists,
	album_name,
	track_name,
	popularity,
	duration_min AS duration_minute,
	explicit,
	danceability_rv AS danceability,
	energy_rv AS energy,
	loudness_rv AS loudness,
	speechiness_rv AS speechiness,
	acousticness_rv AS acousticness,
	instrumentalness_rv AS instrumental,
	liveness_rv AS liveness,
	valence_rv AS valence,
	tempo_rv AS tempo, 
	time_signature,
	track_genre
INTO spotify_data
FROM #dataset_formatted;


-- AT THIS STAGE, THE DATA NICE AND CLEAN, READY TO BE USED FOR FURTHER ANALYSIS ---
SELECT * FROM spotify_data;

-- ###############################################################################--


--#################### EXPLORATORY SPOTIFY ANALYSIS ###############################--

-- 1. Which tracks have the highest popularity

SELECT TOP 10 
	track_name,
	popularity
FROM spotify_data
ORDER BY popularity DESC;

-- 2. Which albums have the most tracks in the dataset?
SELECT TOP 10
	album_name,
	COUNT(*) AS tracks
FROM spotify_data
GROUP BY album_name
ORDER BY tracks DESC;

-- 3. What are the most popular albums based on total popularity of their tracks?
SELECT TOP 10
	album_name,
	SUM(popularity) AS total_popularity
FROM spotify_data
GROUP BY album_name
ORDER BY total_popularity DESC;

-- 4. Is there a correlation between track popularity and other characteristics like energy, danceability, or valence?
SELECT 
	(AVG(popularity*energy)-AVG(popularity)*AVG(energy))/
	(STDEVP(popularity)*STDEVP(energy)) AS popularity_energy_correlation,
	(AVG(popularity*danceability)-AVG(popularity)*AVG(danceability))/
	(STDEVP(popularity)*STDEVP(danceability)) AS popularity_danceability_correlation,
	(AVG(popularity*valence)-AVG(popularity)*AVG(valence))/
	(STDEVP(popularity)*STDEVP(valence)) AS popularity_valence_correlation
FROM spotify_data;

-- 5. Are explicit tracks more or less popular than non-explicit tracks?

SELECT
	CASE WHEN explicit = 1 THEN 'True'
		WHEN explicit = 0 THEN 'False' END  AS explicit,
	COUNT(popularity) AS tracks,
	COUNT(popularity)*100.0 / SUM(COUNT(popularity)) OVER ()  AS pct
FROM spotify_data
GROUP BY CASE WHEN explicit = 1 THEN 'True'
		WHEN explicit = 0 THEN 'False' END;

-- 6. How do different audio features like danceability, energy, or loudness vary across tracks?
SELECT 
	'danceabilty' AS feature,
	MIN(danceability) AS min,
	FORMAT(ROUND(AVG(danceability),3),'0.000') AS avg,
	MAX(danceability) AS max,
	FORMAT(ROUND(STDEV(danceability),3),'0.000') AS stdev
FROM spotify_data
UNION ALL
SELECT 
	'energy' AS feature,
	MIN(energy) AS min,
	FORMAT(ROUND(AVG(energy),3),'0.000') AS avg,
	MAX(energy) AS max,
	FORMAT(ROUND(STDEV(energy),3),'0.000') AS stdev
FROM spotify_data
UNION ALL
SELECT
	'loudness' AS feature,
	MIN(loudness) AS min,
	FORMAT(ROUND(AVG(loudness),3),'0.000') AS avg,
	MAX(loudness) AS max,
	FORMAT(ROUND(STDEV(loudness),3),'0.000') AS stdev
FROM spotify_data;

-- 7. What are the most common genres in the dataset?
SELECT TOP 10
	track_genre,
	COUNT(*) AS total_tracks
FROM spotify_data
GROUP BY track_genre
ORDER BY 2 DESC;

-- 8. Which genre has the highest average popularity or energy?
SELECT TOP 10
	track_genre,
	ROUND(AVG(popularity),2) AS avg_popularity
FROM spotify_data
GROUP BY track_genre
ORDER BY 2 DESC;

SELECT TOP 10
	track_genre,
	FORMAT(AVG(energy),'0.00') AS avg_energy
FROM spotify_data
GROUP BY track_genre
ORDER BY 2 DESC;

-- 9. How does the distribution of different genres compare in terms of popularity or other characteristics?
SELECT
	track_genre,
	MIN(popularity) AS min_popularity,
	FORMAT(AVG(popularity),'0.00') AS avg_popularity,
	MAX(popularity) AS max_popularity,
	FORMAT(STDEV(popularity),'0.00') AS stdev_popularity
FROM spotify_data
GROUP BY track_genre
ORDER BY avg_popularity DESC;

-- 10. Is there any relationship between album duration and album popularity?
WITH dur AS ( -- TDM = total duration in minutes
SELECT 
	album_name,
	SUM(duration_minute) AS total_duration_minutes
FROM spotify_data
GROUP BY album_name
),

pop AS( -- popularity_sum
SELECT
	album_name,
	SUM(popularity) AS popularity_sum
FROM spotify_data
GROUP BY album_name
)

SELECT
(AVG(total_duration_minutes*popularity_sum)-AVG(total_duration_minutes)*AVG(popularity_sum))/
	(STDEVP(total_duration_minutes)*STDEVP(popularity_sum)) AS album_duration_popularity_corr
FROM dur
	JOIN pop
	ON dur.album_name = pop.album_name;

-- 11. Do longer albums tend to have more tracks?
SELECT 
	album_name,
	SUM(duration_minute) AS total_duration_minutes,
	COUNT(*) AS number_of_tracks
FROM spotify_data
GROUP BY album_name
ORDER BY total_duration_minutes DESC;
	
-- 12. What are the most common time signatures in the dataset?
SELECT
	time_signature,
	COUNT(*) AS time_signature_count,
	FORMAT(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),'0.00') + '%' AS pct
FROM spotify_data
GROUP BY time_signature
ORDER BY time_signature_count DESC;

--13. What is the distribution of loudness, tempo, danceability, and other features in the dataset?
SELECT 
	'danceabilty' AS feature,
	MIN(danceability) AS min,
	FORMAT(ROUND(AVG(danceability),3),'0.000') AS avg,
	MAX(danceability) AS max,
	FORMAT(ROUND(STDEV(danceability),3),'0.000') AS stdev
FROM spotify_data
UNION ALL
SELECT 
	'energy' AS feature,
	MIN(energy) AS min,
	FORMAT(ROUND(AVG(energy),3),'0.000') AS avg,
	MAX(energy) AS max,
	FORMAT(ROUND(STDEV(energy),3),'0.000') AS stdev
FROM spotify_data
UNION ALL
SELECT 
	'speechinness' AS feature,
	MIN(speechiness) AS min,
	FORMAT(ROUND(AVG(speechiness),3),'0.000') AS avg,
	MAX(speechiness) AS max,
	FORMAT(ROUND(STDEV(speechiness),3),'0.000') AS stdev
FROM spotify_data
UNION ALL
SELECT 
	'acousticness' AS feature,
	MIN(acousticness) AS min,
	FORMAT(ROUND(AVG(acousticness),3),'0.000') AS avg,
	MAX(acousticness) AS max,
	FORMAT(ROUND(STDEV(acousticness),3),'0.000') AS STDEV
FROM spotify_data
UNION ALL
SELECT 
	'instrumentalness' AS feature,
	MIN(instrumentalness) AS min,
	FORMAT(ROUND(AVG(instrumentalness),3),'0.000') AS avg,
	MAX(instrumentalness) AS max,
	FORMAT(ROUND(STDEV(instrumentalness),3),'0.000') AS max
FROM spotify_data
UNION ALL
SELECT 
	'liveness' AS feature,
	MIN(liveness) AS min,
	FORMAT(ROUND(AVG(liveness),3),'0.000') AS avg,
	MAX(liveness) AS max,
	FORMAT(ROUND(STDEV(liveness),3),'0.000') AS stdev
FROM spotify_data
UNION ALL
SELECT 
	'valence' AS feature,
	MIN(valence) AS min,
	FORMAT(ROUND(AVG(valence),3),'0.000') AS avg,
	MAX(valence) AS max,
	FORMAT(ROUND(STDEV(valence),3),'0.000') AS STDEV
FROM spotify_data;


-- 14. How does the presence of explicit content affect track popularity or other audio features?
	-- calculating summary statistic
	-- determining average value of each feature and pivot it into table

SELECT 
	feature,
	[0] AS explicit_false,
	[1] AS explicit_true
FROM(
	SELECT 
		'danceability' AS feature,
		explicit,
		FORMAT(ROUND(AVG(danceability),2),'0.00') AS avg_value
	FROM spotify_data
	GROUP BY explicit
	UNION ALL
	SELECT 
		'energy' AS feature,
		explicit,
		FORMAT(ROUND(AVG(energy),2),'0.00') AS avg_value
	FROM spotify_data
	GROUP BY explicit
	UNION ALL
	SELECT 
		'speechiness' AS feature,
		explicit,
		FORMAT(ROUND(AVG(speechiness),2),'0.00') AS avg_value
	FROM spotify_data
	GROUP BY explicit
	UNION ALL
	SELECT 
		'acousticness' AS feature,
		explicit,
		FORMAT(ROUND(AVG(acousticness),2),'0.00') AS avg_value
	FROM spotify_data
	GROUP BY explicit
	UNION ALL
	SELECT 
		'instrumentalness' AS feature,
		explicit,
		FORMAT(ROUND(AVG(instrumentalness),2),'0.00') AS avg_value
	FROM spotify_data
	GROUP BY explicit
	UNION ALL
	SELECT 
		'liveness' AS feature,
		explicit,
		FORMAT(ROUND(AVG(liveness),2),'0.00') AS avg_value
	FROM spotify_data
	GROUP BY explicit
	UNION ALL
	SELECT 
		'valence' AS feature,
		explicit,
		FORMAT(ROUND(AVG(valence),2),'0.00') AS avg_value
	FROM spotify_data
	GROUP BY explicit
	) AS tableSource
PIVOT (
	MAX(avg_value) 
	FOR explicit IN ([0] , [1] )
	) AS pivottable
	;

-- 15. Do tracks of certain lengths tend to be more popular or have specific audio features?
SELECT *
FROM (
	SELECT
		'popularity' AS feature,
		ROUND(AVG(popularity),2) AS avg_value, 
		CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END AS duration_type
	FROM spotify_data
	GROUP BY CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END
	UNION ALL
	SELECT
		'danceability' AS feature,
		ROUND(AVG(danceability),2) AS avg_valueg, 
		CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END AS duration_type
	FROM spotify_data
	GROUP BY CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END
	UNION ALL
	SELECT
		'energy' AS feature,
		ROUND(AVG(energy),2) AS avg_value, 
		CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END AS duration_type
	FROM spotify_data
	GROUP BY CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END
	UNION ALL
	SELECT
		'speechiness' AS feature,
		ROUND(AVG(speechiness),2) AS avg_value, 
		CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END AS duration_type
	FROM spotify_data
	GROUP BY CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END
	UNION ALL
	SELECT
		'acousticness' AS feature,
		ROUND(AVG(acousticness),2) AS avg_value, 
		CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END AS duration_type
	FROM spotify_data
	GROUP BY CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END
	UNION ALL
	SELECT
		'instrumentalness' AS feature,
		ROUND(AVG(instrumentalness),2) AS avg_value, 
		CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END AS duration_type
	FROM spotify_data
	GROUP BY CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END
	UNION ALL
	SELECT
		'liveness' AS feature,
		ROUND(AVG(liveness),2) AS avg_value, 
		CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END AS duration_type
	FROM spotify_data
	GROUP BY CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END
	UNION ALL
	SELECT
		'valence' AS feature,
		ROUND(AVG(valence),2) AS avg_value, 
		CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END AS duration_type
	FROM spotify_data
	GROUP BY CASE WHEN duration_minute BETWEEN 0.00 AND 3.00 THEN 'Short'
			WHEN duration_minute BETWEEN 3.01 AND 6.00 THEN 'Medium'
			ELSE 'Long' END ) AS table_helper
PIVOT (
	MAX(avg_value) 
	FOR duration_type IN ([short], [medium],[long] )
	) AS pivottable;

-- 16. Are tracks with higher valence values generally more popular?
SELECT 
	CASE WHEN valence < 0.3 THEN 'valence_low'
		WHEN valence >= 0.3 AND valence <0.7 THEN 'valence_medium'
		ELSE 'valence_high' END AS valence_level,
	ROUND(AVG(popularity),2) AS popularity_avg
FROM spotify_data
GROUP BY 
	CASE WHEN valence < 0.3 THEN 'valence_low'
		WHEN valence >= 0.3 AND valence <0.7 THEN 'valence_medium'
		ELSE 'valence_high' END  
ORDER BY 
	popularity_avg DESC
;