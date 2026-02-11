import pandas as pd
import psycopg2 as pg

conn = pg.connect("""
    dbname=sakuga_db 
    user=postgres
    password=password
    host=localhost
    port=5432""")


cur = conn.cursor()

# Creating tables
CREATE_TABLE_RAW = """
    CREATE TABLE IF NOT EXISTS anime_info_raw(
    anime_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    score FLOAT,
    genres TEXT,
    english_name TEXT,
    japanese_name TEXT,
    synopsis TEXT,
    type VARCHAR(30),
    episodes INT,
    aired TEXT,
    premiered TEXT,
    producers TEXT,
    licensors TEXT,
    studios TEXT,
    source TEXT,
    duration TEXT,
    rating TEXT,
    ranked FLOAT,
    popularity INT,
    members INT,
    favorites INT,
    watching INT,
    completed INT,
    on_hold INT,
    dropped INT
    );"""

COPY_INTO_TABLE_RAW = """
    COPY anime_info_raw 
    FROM STDIN
    WITH CSV HEADER"""
# with open('data/anime-filtered.csv', 'r') as f:
    # cur.copy_expert(COPY_INTO_TABLE_RAW, f)
CREATE_GENRES_TABLE = """
    CREATE TABLE genres(
    genre_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL)
    ;"""

CREATE_ANIME_TYPES = """
    INSERT INTO anime_types(name)
    SELECT DISTINCT type
    FROM anime_info_raw
    WHERE type is NOT NULL;
    """

CREATE_TABLE_SOURCES = """
    CREATE TABLE sources(
    source_id SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL);"""

ALTER_TABLE_TYPE_ID ="""
    ALTER TABLE anime_info
    ADD COLUMN type_id INT;"""

ALTER_ANIME_INFO_AND_CREATE_SOURCE_ID = """ALTER TABLE anime_info ADD COLUMN source_id INT;"""

ALTER_TABLE_ANIME_INFO_TO_ADD_FK_RELATION_FOR_SOURCES = """
    ALTER TABLE anime_info
    ADD CONSTRAINT fk_anime_source
    FOREIGN KEY (source_id)
    REFERENCES sources(source_id);
    """

MAPPING_TYPE_ID_TO_ANIME_INFO = """
    update anime_info a 
    SET type_id = t.type_id
    FROM anime_types t
    WHERE a.type = t.name;"""

# Populating tables
POPULATE_ANIME_INFO = """
    INSERT INTO anime_info
    SELECT
    anime_id::INTEGER,
    name,
    score::REAL,
    english_name,
    japanese_name,
    synopsis ,
    type,
    CASE WHEN episodes ~ '^[0-9]+' THEN episodes::INTEGER ELSE NULL END,
    aired,
    premiered,
    studios,
    source,
    duration,
    rating,
    ranked,
    popularity,
    members,
    favorites
    FROM anime_info_raw"""

POPULATE_TABLE_SOURCES = """
    INSERT INTO sources(source_name)
    SELECT DISTINCT source
    FROM anime_info_raw
    WHERE source is NOT NULL;"""

POPULATE_TABLE_GENRES = """
    INSERT INTO genres(name)
    SELECT DISTINCT trim(unnest(string_to_array(genres,',')))
    FROM anime_info_raw
    WHERE genres IS NOT NULL;"""

POPULATE_ANIME_GENRES = """
    INSERT INTO anime_genres(anime_id, genre_id)
    SELECT
    a.anime_id,
    g.genre_id
    FROM anime_info_raw a
    CROSS JOIN LATERAL unnest(string_to_array(a.genres,',')) AS genre
    JOIN genres g on trim(genre) = g.name"""

# Mapping
MAP_GENRES_TO_ANIME_INFO="""
    CREATE TABLE anime_genres(
    anime_id INT REFERENCES anime_info(anime_id),
    genre_id INT REFERENCES genres(genre_id),
    PRIMARY KEY(anime_id, genre_id));"""

MAP_SOURCE_ID_TO_ANIME_INFO = """
    UPDATE anime_info a
    SET source_id = s.source_id
    FROM sources s 
    WHERE trim(a.source) = trim(s.source_name); """

GENRE_BY_ANIME_ID = """SELECT g.name
    FROM anime_genres ag
    JOIN genres g ON ag.genre_id = g.genre_id
    WHERE ag.anime_id = 1;
    """
NAMES_BY_GENRE = """
    SELECT a.name
    FROM anime_info a
    JOIN anime_genres ag ON a.anime_id = ag.anime_id
    JOIN genres g ON ag.genre_id = g.genre_id
    WHERE g.name = 'Action';
    """
ALL_NAMES_AND_GENRES = """
    SELECT a.name, array_agg(g.name)
    FROM anime_info a
    JOIN anime_genres ag ON a.anime_id = ag.anime_id
    JOIN genres g ON ag.genre_id = g.genre_id
    GROUP BY a.name;
    """

DELETE_RAW_TABLE = """
    DROP TABLE anime_info_raw;"""

ALTER_TABLE_ANIMEINFO_ADD_CREATEANDUPDATED_AT = """
    ALTER TABLE anime_info
    ADD COLUMN created_at TIMESTAMP DEFAULT now(),
    ADD COLUMN updated_at TIMESTAMP DEFAULT now();"""

ALTER_TABLE_ANIMEGENRES_ADD_CREATEANDUPDATED_AT = """
    ALTER TABLE anime_genres
    ADD COLUMN created_at TIMESTAMP DEFAULT now(),
    ADD COLUMN updated_at TIMESTAMP DEFAULT now();"""

ALTER_TABLE_SOURCES_ADD_CREATEANDUPDATED_AT = """
    ALTER TABLE sources
    ADD COLUMN created_at TIMESTAMP DEFAULT now(),
    ADD COLUMN updated_at TIMESTAMP DEFAULT now();"""

ALTER_TABLE_ANIMETYPES_ADD_CREATEANDUPDATED_AT = """
    ALTER TABLE anime_types
    ADD COLUMN created_at TIMESTAMP DEFAULT now(),
    ADD COLUMN updated_at TIMESTAMP DEFAULT now();"""

ALTER_TABLE_GENRES_ADD_CREATEANDUPDATED_AT = """
    ALTER TABLE genres
    ADD COLUMN created_at TIMESTAMP DEFAULT now(),
    ADD COLUMN updated_at TIMESTAMP DEFAULT now();"""

MAKE_UPDATEDAT_WORK = """
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
LINK_TRIGGER_TO_ANIMEINFO = """
CREATE TRIGGER anime_info_updated_at
BEFORE UPDATE ON anime_info
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
"""
LINK_TRIGGER_TO_ANIMEGENRES = """
CREATE TRIGGER anime_genres_updated_at
BEFORE UPDATE ON anime_genres
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
"""
LINK_TRIGGER_TO_SOURCES = """
CREATE TRIGGER sources_updated_at
BEFORE UPDATE ON sources
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
"""
LINK_TRIGGER_TO_ANIMETYPES = """
CREATE TRIGGER anime_types_updated_at
BEFORE UPDATE ON anime_types
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
"""

CREATE_USERS = """
    CREATE TABLE users(
    user_id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    email TEXT NOT NULL UNIQUE,
    CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
    password_has TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now());
    """
ADDING_FK_TO_ANIMEINFO = """
ALTER TABLE anime_info
ADD COLUMN user_id INTEGER REFERENCES users(user_id);
"""
ADDKING_FK_INDEX_TO_ANIMEINFO = """CREATE INDEX idx_anime_user_id ON anime_info(user_id);"""


cur.execute(ADDING_FK_TO_ANIMEINFO)
cur.execute(ADDKING_FK_INDEX_TO_ANIMEINFO)
conn.commit()
cur.close()
conn.close()

print("DONE ALL")
