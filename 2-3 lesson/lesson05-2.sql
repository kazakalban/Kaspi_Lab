-- создаем таблицы
drop table if exists books;
CREATE TABLE books (
    id bigint NOT NULL,
    info jsonb NOT NULL
);

drop table if exists categories;
CREATE TABLE categories (
    id bigint NOT NULL,
    title character(50) NOT NULL
);

-- заполянем данными
insert into books (id, info)
values
(1, '[{"title": "Курс общей физики", "authors": ["Игорь Владимирович Савельев"], "volumes": [{"year": 1986, "pages": 432, "title": "Механика. Молекулярная физика.", "number": 1}, {"year": 1988, "pages": 496, "title": "Электричество и магнетизм. Волны. Оптика.", "number": 2}, {"year": 1987, "pages": 320, "title": "Квантовая оптика. Атомная физика. Физика твердого тела. Физика атомного ядра и элементарных частиц.", "number": 3}], "publishing": "Наука", "category_id": 2, "count_volumes": 3}]'::jsonb),
(4,	'[{"title": "Мир Лиспа", "authors": ["Эро Хювянен", "Йоко Сеппянен"], "volumes": [{"year": 1990, "pages": 447, "title": "Введение в язык Лисп и функциональное программирование", "number": 1}, {"year": 1990, "pages": 319, "title": "Методы и системы программирования", "number": 2}], "publishing": "Мир", "category_id": 2, "count_volumes": 2}]'::jsonb),
(3,	'[{"title": "Крейсера", "authors": ["Валентин Саввич Пикуль"], "volumes": [{"year": 1990, "pages": 511}], "subtitle": "Из жизни юного мичмана", "publishing": "Современник", "category_id": 1, "count_volumes": 1}, {"title": "Фаворит", "authors": ["Валентин Саввич Пикуль"], "volumes": [{"year": 1992, "pages": 599, "title": "Его императрица", "number": 1}, {"year": 1992, "pages": 540, "title": "Его Таврида", "number": 2}], "publishing": "Современник", "category_id": 1, "count_volumes": 2}]'::jsonb),
(2,	'[{"title": "Как стать гроссмейстером", "authors": ["Котов Александр Александрович"], "volumes": [{"year": 1985, "pages": 240}], "publishing": "Физкультура и спорт", "category_id": 3, "count_volumes": 1}]'::jsonb);

insert into categories (id, title)
values
(1,	'Художественная литература'),                         
(2,	'Наука'),                      
(3,	'Спорт');                                          


-- смотрим что сидит в таблице
select * from books;

-- пример json объекта
-- [{"a":1,"b":"foo"},{"a":"2","c":"bar"}]


-- json в набор записей
-- здесь каждое значение это json-объект
select * from json_array_elements_text('["foo", "bar"]');

-- можем вывести определенные поля
SELECT  
    id, info_data->'title' as title  
FROM  
    books,  
    jsonb_array_elements(info) as info_data;

-- можем вывести поля в текстовом виде
SELECT  
    id, info_data->>'title' as title  
FROM  
    books,  
    jsonb_array_elements(info) as info_data;


-- json в набор записей
-- только здесь мы определяем структуру json-структуры
select *
from json_to_recordset('[{"a":1,"b":"foo"},{"a":"2","c":"bar"}]') as x(a int, b text);

select *
from json_to_recordset('[{"a":1,"b":"foo"},{"a":"2","c":"bar"}]') as x(a int);


-- идентичный запрос с заданной структурой
SELECT
    id, title  
FROM  
    books,  
    jsonb_to_recordset(info) as info_common(title text);


-- более сложный пример
-- но вначале рассмотрим еще раз базовый запрос
SELECT
	id,
	authors
FROM
	books,
	jsonb_to_recordset(info) as info_common(authors jsonb);

-- теперь усложняем
SELECT
	id,
	author,
	row_number() over() as author_number
FROM
	books,
	jsonb_to_recordset(info) as info_common(authors jsonb),
	jsonb_array_elements_text(authors) as author;

-- и последний пример
SELECT
	id, title as title_book, sum(pages) as pages_in_all_volumes
FROM
	books,
	jsonb_to_recordset(info) as info_common(volumes jsonb, title varchar),
	jsonb_to_recordset(volumes) as info_volumes(pages int)
GROUP BY id, title;
	
