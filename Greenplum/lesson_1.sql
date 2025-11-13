-- ================================================
-- PRACTICE 1: Работа со схемами, таблицами, сжатием и партиционированием
-- Сауыт Қасым
-- ================================================



-- 1 Подключаемся к ней:
--  pg_learn

-- 2. Создаём собственную схему и делаем её дефолтной
CREATE SCHEMA IF NOT EXISTS kassym_s AUTHORIZATION current_user;
SET search_path TO kassym_s;



-- 3. Таблица со сжатием (ZSTD уровень 2)

CREATE TABLE kassym_s.table_zstd
(
    id BIGINT NOT NULL,
    val TEXT
)
WITH (
    appendonly = true,
    orientation = column,
    compresstype = zstd,
    compresslevel = 2
)
DISTRIBUTED BY (id);


-- 4. Аналогичная таблица без сжатия

CREATE TABLE kassym_s.table_nocomp
(
    id BIGINT NOT NULL,
    val TEXT
)
WITH (
    appendonly = true,
    orientation = column,
    compresstype = none
)
DISTRIBUTED BY (id);


-- 5. Заполняем обе таблицы одинаковыми данными (100 000 строк)

INSERT INTO kassym_s.table_zstd (id, val)
SELECT gs, md5(gs::text || 'seed')::text
FROM generate_series(1, 100000) AS gs;

INSERT INTO kassym_s.table_nocomp (id, val)
SELECT gs, md5(gs::text || 'seed')::text
FROM generate_series(1, 100000) AS gs;


-- 6. Сравниваем размеры таблиц

SELECT 'table_zstd' AS table_name, pg_size_pretty(pg_total_relation_size('kassym_s.table_zstd')) AS total_size;
SELECT 'table_nocomp' AS table_name, pg_size_pretty(pg_total_relation_size('kassym_s.table_nocomp')) AS total_size;



-- 7. Распределение по сегментам

SELECT gp_segment_id, count(*) AS rows_per_segment
FROM kassym_s.table_zstd
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

SELECT gp_segment_id, count(*) AS rows_per_segment
FROM kassym_s.table_nocomp
GROUP BY gp_segment_id
ORDER BY gp_segment_id;



-- 8. Таблица с плохим распределением (все строки на одном сегменте)

CREATE TABLE kassym_s.table_bad_distribution
(
    id INT,
    val TEXT
)
WITH (
    appendonly = true,
    orientation = column,
    compresstype = zstd,
    compresslevel = 2
)
DISTRIBUTED BY (id);

INSERT INTO kassym_s.table_bad_distribution (id, val)
SELECT 1, md5(gs::text || 'seed')::text
FROM generate_series(1, 100000) AS gs;

SELECT gp_segment_id, count(*) AS rows_per_segment
FROM kassym_s.table_bad_distribution
GROUP BY gp_segment_id
ORDER BY gp_segment_id;


-- 9. Таблица с партиционированием по дате (понедельно, 2025 год)

CREATE TABLE kassym_s.events
(
    id SERIAL,
    date_begin TIMESTAMP NOT NULL,
    payload TEXT,
    PRIMARY KEY (id, date_begin)
)
DISTRIBUTED BY (id)
PARTITION BY RANGE (date_begin)
(
    START (DATE '2025-01-01') INCLUSIVE
    END (DATE '2026-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION extra_data
);


-- 10. Проверяем, что таблица и партиции создались

SELECT * FROM pg_partitions
WHERE schemaname = 'kassym_s' AND tablename = 'events'
ORDER BY partitionname;
