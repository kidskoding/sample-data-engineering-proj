DROP TABLE IF EXISTS pokemon_stats;

CREATE TABLE pokemon_stats (
    pokemon_id INT PRIMARY KEY,
    pokemon_name VARCHAR(50) NOT NULL,
    primary_type VARCHAR(50),
    base_hp INT,
    base_attack INT,
    base_defense INT,
    height_dm INT,
    weight_hg INT,
    first_ability VARCHAR(100),
    ingest_timestamp TIMESTAMP
);

CREATE INDEX idx_pokemon_name ON pokemon_stats (pokemon_name);