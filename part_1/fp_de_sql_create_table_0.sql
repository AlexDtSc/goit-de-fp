CREATE TABLE IF NOT EXISTS olympic_dataset.alexdtsc_athlete_enriched_agg (
    sport VARCHAR(255),
    medal VARCHAR(50),
    sex VARCHAR(50),
    country_noc VARCHAR(10),
    avg_height DOUBLE,
    avg_weight DOUBLE,
    timestamp TIMESTAMP
);

SELECT * FROM olympic_dataset.alexdtsc_athlete_enriched_agg;

SELECT COUNT(*) FROM olympic_dataset.alexdtsc_athlete_enriched_agg;

TRUNCATE TABLE olympic_dataset.alexdtsc_athlete_enriched_agg;