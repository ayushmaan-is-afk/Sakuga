import pandas as pd
import psycopg2 as pg

conn = pg.connect("""
    dbname=sakuga_db 
    user=postgres
    password=password
    host=localhost
    port=5432""")


cur = conn.cursor()


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
ALTER_TABLE_TYPE_ID ="""
    ALTER TABLE anime_info
    ADD COLUMN type_id INT;"""
MAPPING_TYPE_ID_TO_ANIME_INFO = """
    update anime_info a 
    SET type_id = t.type_id
    FROM anime_types t
    WHERE a.type = t.name;"""
CREATE_TABLE_SOURCES = """
    CREATE TABLE sources(
    source_id SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL);"""
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
LINKING_GENRES_WITH_ANIME_INFO="""
    CREATE TABLE anime_genres(
    anime_id INT REFERENCES anime_info(anime_id),
    genre_id INT REFERENCES genres(genre_id),
    PRIMARY KEY(anime_id, genre_id));"""
ALTER_ANIME_INFO_AND_CREATE_SOURCE_ID = """ALTER TABLE anime_info ADD COLUMN source_id INT;"""
MAP_SOURCE_ID_TO_ANIME_INFO = """
    UPDATE anime_info a
    SET source_id = s.source_id
    FROM sources s 
    WHERE trim(a.source) = trim(s.source_name); """
ALTER_TABLE_ANIME_INFO_TO_ADD_FK_RELATION_FOR_SOURCES = """
    ALTER TABLE anime_info
    ADD CONSTRAINT fk_anime_source
    FOREIGN KEY (source_id)
    REFERENCES sources(source_id);
    """
POPULATE_ANIME_GENRES = """
    INSERT INTO anime_genres(anime_id, genre_id)
    SELECT
    a.anime_id,
    g.genre_id
    FROM anime_info_raw a
    CROSS JOIN LATERAL unnest(string_to_array(a.genres,',')) AS genre
    JOIN genres g on trim(genre) = g.name"""




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
QUERYS = [

]
cur.execute(ALTER_TABLE_ANIME_INFO_TO_ADD_FK_RELATION_FOR_SOURCES)

conn.commit()
cur.close()
conn.close()

print("DONE")
