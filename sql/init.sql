-- ============================
-- STAR WARS DATA WAREHOUSE
-- ============================

-- Таблица персонажей
CREATE TABLE IF NOT EXISTS people (
    id SERIAL PRIMARY KEY,
    swapi_id INTEGER UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    height VARCHAR(20),
    mass VARCHAR(20),
    hair_color VARCHAR(50),
    skin_color VARCHAR(50),
    eye_color VARCHAR(50),
    birth_year VARCHAR(20),
    gender VARCHAR(20),
    homeworld_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица планет
CREATE TABLE IF NOT EXISTS planets (
    id SERIAL PRIMARY KEY,
    swapi_id INTEGER UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    rotation_period VARCHAR(20),
    orbital_period VARCHAR(20),
    diameter VARCHAR(20),
    climate VARCHAR(100),
    gravity VARCHAR(100),
    terrain VARCHAR(255),
    population VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для сырых данных (Data Lake → staging)
CREATE TABLE IF NOT EXISTS raw_swapi_data (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    swapi_id INTEGER NOT NULL,
    raw_json JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_people_name ON people(name);
CREATE INDEX IF NOT EXISTS idx_planets_name ON planets(name);
CREATE INDEX IF NOT EXISTS idx_raw_entity ON raw_swapi_data(entity_type, swapi_id);

-- ============================
-- DATA MARTS (Витрины)
-- ============================

-- Витрина: статистика по полу
CREATE TABLE IF NOT EXISTS dm_gender_stats (
    gender VARCHAR(20) PRIMARY KEY,
    total_count INTEGER,
    avg_height NUMERIC(10,2),
    avg_mass NUMERIC(10,2),
    min_height NUMERIC(10,2),
    max_height NUMERIC(10,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Витрина: топ персонажей по характеристикам
CREATE TABLE IF NOT EXISTS dm_top_characters (
    category VARCHAR(50),
    rank INTEGER,
    name VARCHAR(255),
    value NUMERIC(10,2),
    gender VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, rank)
);

-- Витрина: распределение по цвету глаз
CREATE TABLE IF NOT EXISTS dm_eye_color_stats (
    eye_color VARCHAR(50) PRIMARY KEY,
    total_count INTEGER,
    percentage NUMERIC(5,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
