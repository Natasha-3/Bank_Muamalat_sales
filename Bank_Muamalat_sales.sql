--РАЗВЕДОЧНЫЙ АНАЛИЗ ПРОДАЖ БАНКА MUAMALAT

--ОГЛАВЛЕНИЕ  

--1. Введение  
--2. Предобработка  
--3. Выявление аномалий  
--    3.1 Функции обнаружения пропусков и дубликатов  
--    3.2 Таблица ord (заказы)  
--    3.3 Таблица cust (клиенты)  
--    3.4 Таблица cat (категории продуктов)  
--    3.5 Таблица prod (продукты)  
--4. Поиск закономерностей  
--    4.1 Географическое расположение  
--        4.1-1 Как города, в которых проживают клиенты, влияют на заказы  
--        4.1-2 Как штаты, в которых проживают клиенты, влияют на заказы  
--    4.2 Финансовые показатели  
--        4.2-1 Выручка  
--            4.2-1-1 Выручка с течением времени  
--            4.2-1-2 Выручка по городам  
--        4.2-2 Средний чек  
--            4.2-2-1 Средний чек с течением времени  
--            4.2-2-2 Средний чек по категориям с течением времени  
--        4.2-3 ARPU и ARPPU  
--    4.3 Время  
--5. Выводы  



--1. ВВЕДЕНИЕ

--Цель анализа  
--Цель проекта - провести разведочный анализ данных продаж банка Muamalat, 
--выявить аномалии и общие закономерности.  
--
--Описание данных   
--Данные взяты из набора 
--href="https://www.kaggle.com/datasets/anggundwilestari/bank-muamalat?resource=download" 
--(Bank Muamalat Sales Performance). 
--Они находятся в 4-х таблицах:  customers (клиенты), products (продукты), orders (заказы) 
--и productcategory (категории продуктов).  
--   
--Customers
--* customerid - id клиента;
--* firstname - имя;
--* lastname - фамилия;
--* customeremail - электронная почта;
--* customerphone - телефон;
--* customeraddress - адрес;
--* customercity - город;
--* customerstate - штат;
--* customerzip - почтовый индекс.
--
--Orders
--* orderid - id заказа;
--* date - дата заказа;
--* customerid - id клиента;
--* prodnumber - номер продукта;
--* quantity - количество.
--
--Product category
--* categoryid - id категории;
--* categoryname - название категории;
--* categoryabbreviation - аббревиатура категории.
--
--Products
--* prodnumber - номер продукта;
--* prodname - название продукта;
--* category - категория;
--* price - цена.



--2. ПРЕДОБРАБОТКА

--Таблицы выглядят так:  
--Customers
SELECT * FROM customers limit 7;
--Orders
SELECT * FROM orders limit 5;
--Product category
SELECT * FROM productcategory;
--Products
SELECT * FROM products limit 5;

--В таблице Customers для удобства сделаем следующее:
--1. customeremail преобразуем так, чтобы там оставалась только та почта, на 
--которую необходимо писать.
--2. В customerphone уберем лишние знаки.
--3. Из customercity уберем отдельные слова City, чтобы там были только сами 
--названия городов.
--Сохраним результаты в новую таблицу cust, чтобы обезопасить исходные данные.

-- Создаем новую таблицу:
DROP TABLE IF EXISTS cust;
CREATE table IF NOT EXISTS cust as
SELECT * 
FROM customers;

SELECT count (*) FROM cust;

-- Проверяем, что в каждой строке customeremail есть mailto и она заканчивается #:
SELECT count(*)
FROM cust
WHERE customerEmail ~ 'mailto:' and right(customeremail, 1) = '#';

-- Выполняем преобразования в столбцах:
-- Выполняем преобразования в столбцах:
UPDATE cust
SET 
customeremail = split_part(
    left(customeremail, length(customeremail) - 1),
    'mailto:', 2),
customerphone =
        left(regexp_replace(customerphone, '[^0-9]', '', 'g'), 3) || '-' ||
        substring(regexp_replace(customerphone, '[^0-9]', '', 'g'), 4, 3) || '-' ||
        right(regexp_replace(customerphone, '[^0-9]', '', 'g'), 4),
customercity = regexp_replace(customercity, ' City', '')
;

SELECT * 
FROM cust
limit 5;

--Остальные таблицы нас устраивают. Для безопасной работы с ними также создадим 
--дополнительные таблицы:
DROP TABLE IF EXISTS ord;
CREATE table ord as
SELECT * 
FROM orders;

DROP TABLE IF EXISTS cat;
CREATE table cat as
SELECT * 
FROM productcategory;

DROP TABLE IF EXISTS prod;
CREATE table prod as
SELECT * 
FROM products;
--Далее будем использовать таблицы cust, ord, prod и cat.

--Проверим наличие полных дубликатов в таблицах:
SELECT *
FROM 
(
    SELECT *,
    row_number() over (partition by firstname, lastname order by customerid) as num
    FROM cust
)
WHERE num > 1
;

SELECT *
FROM 
(
    SELECT *,
    row_number() over (partition by date, customerid, prodnumber, quantity order by orderid) as num
    FROM ord
)
WHERE num > 1
;

SELECT *
FROM 
(
    SELECT *,
    row_number() over (partition by categoryname, categoryabbreviation order by categoryid) as num
    FROM cat
)
WHERE num > 1
;

SELECT *
FROM 
(
    SELECT *,
    row_number() over (partition by prodname, category, price order by prodnumber) as num
    FROM prod
)
WHERE num > 1
;
--Полные дубликаты отсутствуют.

-- Удаляем вспомогательные столбцы:
ALTER TABLE cust DROP COLUMN IF EXISTS num;
ALTER TABLE ord DROP COLUMN IF EXISTS num;
ALTER TABLE prod DROP COLUMN IF EXISTS num;
ALTER TABLE cat DROP COLUMN IF EXISTS num;



--3. ВЫЯВЛЕНИЕ АНОМАЛИЙ

--Выбросы и аномалии бывают разными, например:  
--* отсутствующие значения там, где они должны быть, или наличие значений там, 
--где их быть не должно;
--* слишком большие, слишком маленькие или несоответствующие формату значения;
--* заметно часто или наоборот редко встречающиеся значения.  
--
--Проверим наши данные на наличие этих признаков.  



--3.1 ФУНКЦИИ ОБНАРУЖЕНИЯ ПРОПУСКОВ И ДУБЛИКАТОВ
 
--Для начала напишем несколько функций. Они позволят нам находить пропуски, 
--а также проверять наличие повторяющихся значений в столбцах, сразу по всей 
--таблице.  

--Первая функция будет принимать название таблицы и показывать количество 
--null-значений в каждом столбце:
CREATE OR REPLACE FUNCTION get_null_counts_all_columns(
    p_table_name TEXT
)
RETURNS TABLE (
    column_name TEXT,
    null_count BIGINT
)
AS $$
DECLARE
    v_sql_parts TEXT[] := '{}';
    v_col_name TEXT; 
    v_full_table_name TEXT;
    v_final_sql TEXT;
BEGIN
    SELECT FORMAT('%I.%I', table_schema, table_name)
    INTO v_full_table_name
    FROM information_schema.tables
    WHERE table_name = p_table_name
    AND table_schema = current_schema();

    IF v_full_table_name IS NULL THEN
        RAISE EXCEPTION 'Таблица "%" не найдена в текущей схеме.', p_table_name;
    END IF;

    FOR v_col_name IN
        SELECT c.column_name 
        FROM information_schema.columns AS c 
        WHERE c.table_schema = current_schema()
          AND c.table_name = p_table_name
        ORDER BY c.ordinal_position
    LOOP
        v_sql_parts := array_append(v_sql_parts,
                                    FORMAT('SELECT %L AS column_name, COUNT(*) AS null_count FROM %s WHERE %I IS NULL',
                                           v_col_name, v_full_table_name, v_col_name));
    END LOOP;

    v_final_sql := array_to_string(v_sql_parts, ' UNION ALL ');

    RETURN QUERY EXECUTE v_final_sql;

END;
$$ LANGUAGE plpgsql;

--Вторая функция принимает название таблицы и показывает, есть ли дубликаты в 
--каждом из столбцов:
DROP FUNCTION get_duplicates_all_columns(text);
CREATE OR REPLACE FUNCTION get_duplicates_all_columns(
    p_table_name TEXT
)
RETURNS TABLE (
    column_name TEXT,
    is_duplicates BOOLEAN
)
AS $$
DECLARE
    v_sql_parts TEXT[] := '{}';
    v_col_name TEXT; 
    v_full_table_name TEXT;
    v_final_sql TEXT;
BEGIN
    SELECT FORMAT('%I.%I', table_schema, table_name)
    INTO v_full_table_name
    FROM information_schema.tables
    WHERE table_name = p_table_name
    AND table_schema = current_schema();

    IF v_full_table_name IS NULL THEN
        RAISE EXCEPTION 'Таблица "%" не найдена в текущей схеме.', p_table_name;
    END IF;

    FOR v_col_name IN
        SELECT c.column_name 
        FROM information_schema.columns AS c 
        WHERE c.table_schema = current_schema()
          AND c.table_name = p_table_name
        ORDER BY c.ordinal_position
    LOOP
        v_sql_parts := array_append(v_sql_parts,
                                    FORMAT('
                                    select %L as column_name, max(count) > 1 as is_duplicates
                                    from(
                                    SELECT %I as column_name, count(*) as count
                                    FROM %s 
                                    GROUP BY 1 
                                    )
                                    ',
                                    v_col_name, v_col_name, v_full_table_name));
    END LOOP;

    v_final_sql := array_to_string(v_sql_parts, ' UNION ALL ');

    RETURN QUERY EXECUTE v_final_sql;

END;
$$ LANGUAGE plpgsql;



--3.2 ТАБЛИЦА ORD (ЗАКАЗЫ)  

--Проверим наличие пропущенных значений в таблице:
SELECT * FROM get_null_counts_all_columns('ord');
--Пропуски отсутствуют.  

--Проверим наличие дубликатов:
SELECT * FROM get_duplicates_all_columns('ord');
--Видно, что id заказа уникален. В остальных столбцах есть повторы, но они 
--допустимы.

--СТОЛБЕЦ DATE  

--Посмотрим, какое время охватывают наши данные по заказам:
WITH min_nax_dates AS
(
    SELECT
        min(date) AS min_date,
        max(date) AS max_date
    FROM ord
),
month_order_counts AS
(
    SELECT
        extract(YEAR from date) AS order_year,
        extract(MONTH from date) AS order_month,
        count(*) AS order_count
    FROM ord
    GROUP BY 1, 2
)
SELECT
    CASE
        WHEN (SELECT moc.order_count FROM month_order_counts moc 
        WHERE moc.order_year = extract(YEAR from mmd.min_date) 
        AND moc.order_month = extract(MONTH from mmd.min_date)) > 1
        THEN mmd.min_date
        ELSE null
    END AS min_date,
    CASE
        WHEN (SELECT moc.order_count FROM month_order_counts moc 
        WHERE moc.order_year = extract(YEAR from mmd.max_date) 
        AND moc.order_month = extract(MONTH from mmd.max_date)) > 1
        THEN mmd.max_date
        ELSE null
    END AS max_date
FROM
    min_nax_dates as mmd;
--Данные целиком охватывают 2 года: 2020 и 2021 год.  
   
--Посмотрим, как распределены заказы по дням:
SELECT date, count(*) as orders FROM ord GROUP BY 1 ORDER BY 1;
--Замечаем, что у нас обработаны 728 строк. С учетом того, что 2020 - високосный 
--год (т.е. в нем 366 дней), а 2021 - обычный, мы должны получить 731 день. Это 
--означает, что в данных есть пропущенные даты. 

-- Создадим размерную таблицу дат:
DROP table if exists date_dim;
CREATE table date_dim
as
SELECT date::date
,date_part('day',date)::int as day_of_month
,date_part('doy',date)::int as day_of_year
,trim(to_char(date, 'Dy')) as day_of_week
,date_part('month',date)::int as month_number
,trim(to_char(date, 'Month')) as month_name
,trim(to_char(date, 'Mon')) as month_short_name
,date_part('quarter',date)::int as quarter_number
,date_part('year',date)::int as year 
FROM generate_series('2020-01-01'::date, '2021-12-31'::date, '1 day') as date;
SELECT * FROM date_dim;

--Выясним, за какие дни отсутствуют данные:
SELECT dd.date
FROM
(
    SELECT date
    FROM ord
) o
RIGHT JOIN date_dim dd 
on o.date = dd.date 
WHERE o.date is null;
--Дни, когда не было заказов: 2020-02-23, 2020-11-12 и 2020-11-14. 
--Связи между этими датами не прослеживается.  

--Построим график распределения заказов по месяцам:
SELECT CONCAT_WS('-', RIGHT(year::text, 2), LPAD(month::text, 2, '0')) as date, 
orders 
FROM 
( 
SELECT 
    date_part('year', dd.date) as year, 
    date_part('month', dd.date) as month, 
    dd.month_short_name as month_name, 
    count(*) as orders  
    FROM  
    (  
        SELECT *  
        FROM ord  
    ) o  
    RIGHT JOIN date_dim dd  
    on o.date = dd.date  
    GROUP BY 1,2,3 ORDER BY 1,2 
) ym 
;
--График находится в файле Bank_Muamalat_sales.ipynb.
--Из графика видно, что в январе количество заказов всегда высокое, 
--а к марту-апрелю снижается. В другие периоды в одни и те же месяцы 
--в 2020 году был рост количества заказов, а в 2021 - спад, и наоборот. 
--Также видно, что в сентябре 2021г. заказы упали, а дальнейший подъем 
--был незначительным. Этот временной период следует изучить подробнее.  
--Никаких явных аномалий в датах нет.  
 
--СТОЛБЕЦ ORDERID  

--Рассмотрим столбец номер заказа (orderid):
SELECT 
min(orderid), max(orderid), count(*) as num_of_orders
FROM ord;
--У нас всего 3339 строк в таблице заказов, однако последнему заказу 
--присвоен номер 3340. Выясним, где ошибка:
SELECT *
FROM
(
SELECT orderid, 
row_number() over () as row_number
FROM ord
)
WHERE orderid != row_number
;
--Видно, что некорректно указано лишь последнее значение. 
--Поскольку orderid фигурирует лишь в этой таблице, скорректируем его:
UPDATE ord
SET orderid = 3339
WHERE orderid = 3340;
SELECT * from ord ORDER BY orderid desc limit 1;
--В столбце orderid каждое из значений встречается лишь 1 раз, 
--поэтому проверять его на слишком часто или редко встречающиеся 
--значения бессмысленно. 

--СТОЛБЕЦ CUSTOMERID  

--Чтобы убедиться в корректности его заполнения, проверим, все ли 
--значения customerid из таблицы ord есть в таблице cust:

SELECT *
FROM
(
    SELECT customerid
    FROM ord 
) o
LEFT JOIN cust c on o.customerid = c.customerid
WHERE c.customerid is null;
--Все значения id клиента в таблице заказов есть также в таблице клиентов. 
 
--Проверим, есть ли клиенты, которые совершили слишком большое количество 
--заказов (т е customerid, встречающиеся слишком часто):
SELECT customerid, count(*) as num_of_orders
FROM ord
GROUP BY 1
ORDER BY 2 desc
limit 5;
--Аномально больших количеств заказов нет. Самое больше количество заказов 
--от одного клиента за 2 года - 7.

--Проверим, совершал ли кто-то необычно большое количество заказов в течение 
--какого-то определенного месяца:
SELECT extract (YEAR from date) as year,
extract (MONTH from date) as month,
customerid,
count(*) as num_of_orders
FROM ord
GROUP BY 1,2,3
ORDER BY 4 desc
limit 5;
--Никто из клиентов не совершал аномально много заказов в течение какого-то 
--определенного месяца. Максимальное количество заказов от клиента за месяц - 3.   
  
--СТОЛБЕЦ PRODNUMBER

--Проверим все ли значения prodnumber соответствуют таблице prod:
SELECT distinct prodnumber
FROM ord
EXCEPT
SELECT prodnumber
FROM prod;
--Все значения prodnumber есть в таблице prod.  

--Найдем продукты по следующим критериям:  
--* больше всего заказов;
--* заказывают больше всего по количеству;
--* меньше всего заказов;
--* заказывают меньше всего по количеству.
with prod_statistics as (
SELECT b.prodnumber, b.prodname,
coalesce(num_of_orders, 0) as num_of_orders,
coalesce(sum_quantity, 0) as sum_quantity
FROM
(
    SELECT
    prodnumber,
    count(*) as num_of_orders,
    sum(quantity) as sum_quantity
    FROM ord
    GROUP BY 1 
) a
FULL JOIN prod b on a.prodnumber = b.prodnumber
)
SELECT b.prodnumber, b.prodname, b.num_of_orders, b.sum_quantity
FROM
(
    SELECT 
    max(num_of_orders) as max_orders,
    max(sum_quantity) as max_quantity,
    min(num_of_orders) as min_orders,
    min(sum_quantity) as min_quantity
    FROM prod_statistics
) a
JOIN prod_statistics b on 1=1 
WHERE b.num_of_orders = max_orders or b.num_of_orders = min_orders	
or 	b.sum_quantity = max_quantity or b.sum_quantity = min_quantity
;
--Самым популярным продуктом является Sleepy Eye Blueprint - у него больше всех 
--заказов, а также он лидирует по количеству проданных единиц.  
--Наименее популярным является Upside Down Robot Blueprint - за два года этот 
--товар ни разу не заказали.  
--Аномальных значений в столбце prodnumber не найдено.
  
--СТОЛБЕЦ QUANTITY 

--Посмотрим, как распределено количество заказанных товаров по месяцам:
SELECT CONCAT_WS('-', RIGHT(year::text, 2), LPAD(month::text, 2, '0')) as date, 
quantity 
FROM 
( 
SELECT 
    date_part('year', dd.date) as year, 
    date_part('month', dd.date) as month, 
    dd.month_short_name as month_name, 
    sum (quantity) as quantity  
    FROM  
    (  
        SELECT *  
        FROM ord  
    ) o  
    RIGHT JOIN date_dim dd  
    on o.date = dd.date  
    GROUP BY 1,2,3 ORDER BY 1,2 
) ym 
;
--График находится в файле Bank_Muamalat_sales.ipynb.
--График количества заказанных товаров в целом соответствует графику количества 
--заказов по месяцам. В январе количество заказанных товаров высокое, но падает
--к марту. Начиная с июня, в те месяцы, когда в 2020 году был рост заказанных 
--товаров, в 20201 году был спад. Никаких аномалий здесь нет, однако на обоих 
--графиках можно увидеть падение продаж в сентябре и октябре 2021 года.  

--Изучим, в каком количестве чаще всего покупают товары:
SELECT quantity, count(*)
FROM ord 
GROUP BY 1 
ORDER BY 2 desc;
--Обычно покупают товары в количествое от 1 до 6 штук. Чаще всего покупают 3-4 
--штуки, реже - по одному.  Аномалий здесь нет.

--Для удобства расчетов в таблицу ord следует добавить еще один столбец cost, 
--который покажет стоимость каждого заказа:
ALTER TABLE ord ADD COLUMN cost NUMERIC;
UPDATE ord o
SET cost = p.price * o.quantity
FROM prod p
WHERE o.prodnumber = p.prodnumber; 

SELECT *
FROM ord
limit 3;



--3.3 ТАБЛИЦА CUST (КЛИЕНТЫ)

--Проверим наличие пропусков в таблице:
SELECT * FROM get_null_counts_all_columns('cust');
--Пропусков нет.  

--Проверим наличие дубликатов:
SELECT * FROM get_duplicates_all_columns('cust');
--Видно, что id клиента, почта, телефон и адрес уникальны. Имя, фамилия, город, 
--штат и почтовый индекс имеют повторы.

--СТОЛБЕЦ CUSTOMERID 

--Проверим максимальное значение customerid:
SELECT count(*) as num_of_rows,
max(customerid) as max_customer_id
FROM cust;
--Значения customerid соответсвуют номеру строки в таблице cust. Каждое из 
--значений в столбце встречается лишь 1 раз, поэтому проверять его на слишком 
--часто или редко встречающиеся значения бессмысленно. 

--СТОЛБЦЫ FIRSTNAME И LASTNAME

--Посмотрим, есть повторяющиеся сочетания имени и фамилии среди клиентов. Это 
--исключит возможность попадания одного клиента в таблицу дважды.
SELECT firstname, lastname, count(*) as count
FROM cust
GROUP BY 1,2
HAVING count(*) > 1;
--Все сочетания имени и фамилии уникальны.  
  
--Столбцы с телефоном, почтой и адресом уникальны и не имеют пропусков. 
--Аномалий по описанным выше признакам в них быть не может.
   
--СТОЛБЕЦ CUSTOMERCITY  

--Посмотрим, в каких городах находится больше всего клиентов: 
SELECT customercity, count(*) as num_of_customers
FROM cust
GROUP BY 1
ORDER BY 2 desc;
--Среди клиентов банка Muamalat жители 381 города. Наибольшее количество 
--клиентов из Вашингтона и Хьюстона.
SELECT customercity, count(*) as num_of_customers
FROM cust
GROUP BY 1
HAVING count(*) <2
ORDER BY 2 desc;
--Также есть 87 городов, в которых живут лишь по одному клиенту. Аномалий нет.

--СТОЛБЕЦ CUSTOMERSTATE 

--Посмотрим, в каких штатах больше всего клиентов:
SELECT customerstate, count(*) as num_of_customers
FROM cust
GROUP BY 1
ORDER BY 2 desc;
--Клиентами банка Muamalat являются люди 48 штатов. Больше всего клиентов в 
--штатах Калифорния, Техас и Флорида, а меньше всего - в Монтане и Вайоминге.  
--Аномальные значения отсутствуют.

--СТОЛБЕЦ CUSTOMERZIP

--Для понимания более узкого территориального распределения сделаем группировку 
--по почтовому индексу и посмотрим, сколько клиентов проживают на каждой 
--географической территории:
SELECT customerzip, count(*) as num_of_customers
FROM cust
GROUP BY 1
ORDER BY 2 desc;
--Мы получили 1250 географических территорий. На каждой из них проживает от 1 
--до 6 клиентов банка Muamalat. Аномалии отсутствуют.



--3.4 ТАБЛИЦА CAT (КАТЕГОРИИ ПРОДУКТОВ)

SELECT * FROM cat;
--Из таблицы видно, что пропуски отсутствуют, также нет аномальных значений и частот.



--3.5 ТАБЛИЦА PROD (ПРОДУКТЫ)

--Проверим наличие пропусков и дубликатов:
SELECT * FROM get_null_counts_all_columns('prod');
SELECT * FROM get_duplicates_all_columns('prod');
--Пропуски отсутствуют. Столбцы с номером продукта и его названием уникальны, 
--в то время как столбцы категория и цена имеют повторы.  
  
--В столбцах prodnumber и prodname аномалии по рассматриваемым признакам отсутствуют.

--СТОЛБЕЦ CATEGORY 

--Проверим, что все категории есть в таблице cat:
SELECT p.category as prod_category, c.categoryid as cat_category
FROM
(
SELECT category 
FROM prod
) p
RIGHT JOIN cat c on p.category = c.categoryid
GROUP BY 1,2;
--Все категории проедставлены в обоих таблицах. 

--Посмотрим, сколько продуктов представлено в каждой категории:
SELECT p.category, c.categoryname, p.products_count
FROM
(
SELECT category, count(prodnumber) as products_count
FROM prod 
GROUP BY 1
) p
RIGHT JOIN cat c on p.category = c.categoryid
ORDER BY 1;
--Больше всего продуктов представлено в категории eBooks, а меньше - в 
--категориях Robot Kits и Robots.  
--Аномалии отсутствуют.

--СТОЛБЕЦ PRICE  

--Посмотрим, какая наибольшая и наименьшая цена продукта в каждой категории
--и среди продуктов в целом:
SELECT coalesce(p.category::text,'All') as category, 
coalesce(c.categoryname,'All') as categoryname , min_price, max_price,
avg_price, median_price
FROM
(
    SELECT category, min(price) as min_price, max(price) as max_price, 
    round(avg(price)::numeric,2) as avg_price, 
    percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price
    FROM prod
    GROUP BY cube (category)
    ORDER BY 1
) p
LEFT JOIN cat c on p.category = c.categoryid;
--Видно, что наименьшая цена составляет 499.0, а наибольшая 89900.0. 
--Самая бюджетная категория - Blueprints, а самая дорогая - Robots.  

--Разобьем продукты на ценовые категории и посмотрим, сколько продуктов будет 
--в каждой:
with price_cohorts as(
    SELECT 
    coalesce(sum(case when price < 1000 then 1 else 0 end), 0) 
    as price_less_than_1000,
    coalesce(sum(case when price >= 1000 and price < 5000 then 1 else 0 end), 0) 
    as price_less_than_5000,
    coalesce(sum(case when price >= 5000 and price < 10000 then 1 else 0 end), 0) 
    as price_less_than_10000,
    coalesce(sum(case when price >= 10000 and price < 30000 then 1 else 0 end), 0) 
    as price_less_than_30000,
    coalesce(sum(case when price >= 30000 and price < 50000 then 1 else 0 end), 0) 
    as price_less_than_50000,
    coalesce(sum(case when price >= 50000 then 1 else 0 end), 0) 
    as price_over_50000
    FROM prod
)
SELECT 'price_less_than_1000' as price_category, price_less_than_1000 
as products_count
FROM price_cohorts
UNION ALL
SELECT 'price_less_than_5000' as price_category, price_less_than_5000 
as products_count
FROM price_cohorts
UNION ALL
SELECT 'price_less_than_10000' as price_category, price_less_than_10000 
as products_count
FROM price_cohorts
UNION ALL
SELECT 'price_less_than_30000' as price_category, price_less_than_30000 
as products_count
FROM price_cohorts
UNION ALL
SELECT 'price_less_than_50000' as price_category, price_less_than_50000 
as products_count
FROM price_cohorts
UNION ALL
SELECT 'price_over_50000' as price_category, price_over_50000 as products_count
FROM price_cohorts;
--Большая часть продуктов стоит от 1000 до 5000.  

--Определим, какой продукт является самым дорогим и самым дешевым:
WITH prod_with_min_max as (
    SELECT prodnumber, prodname, price,
    min(price) over () as min_price,
    max(price) over () as max_price
    FROM prod
)
SELECT prodnumber, prodname, price
FROM prod_with_min_max
WHERE price = min_price or price = max_price;
--Самая высокая цена у товара MICR-23K Robot, а самая низкая - у Cat Robot 
--Blueprint.  
--Аномалии в столбце price не выявлены.



--4. ПОИСК ЗАКОНОМЕРНОСТЕЙ

--В этом разделе исследуем наши данные на наличие устойчивых повторяющихся 
--взаимосвязей. Будем рассматривать географическое расположение, финансовые 
--показатели и временные периоды.


--4.1 ГЕОГРАФИЧЕСКОЕ РАСПОЛОЖЕНИЕ

--4.1-1 КАК ГОРОДА, В КОТОРЫХ ПРОЖИВАЮТ КЛИЕНТЫ, ВЛИЯЮТ НА ЗАКАЗЫ

--Посмотрим, из каких городов приходит больше всего заказов, а из каких меньше:
SELECT c.customercity, count(o.orderid) as num_of_orders
FROM
(
    SELECT customerid, customercity
    FROM cust
) c
FULL JOIN ord o on c.customerid = o.customerid
GROUP BY 1
ORDER BY 2 desc;
--Видно, что больше всего заказов приходит из Вашингтона и Хьюстона.

SELECT c.customercity, count(o.orderid) as num_of_orders
FROM
(
    SELECT customerid, customercity
    FROM cust
) c
FULL JOIN ord o on c.customerid = o.customerid
GROUP BY 1
HAVING count(orderid) = 0
ORDER BY 2 desc;
--Также есть 20 городов, из которых заказов не поступало.  

--Выясним, сколько заказов приходится на каждого клиента в каждом городе:
SELECT c.customercity, 
count(o.orderid) as num_of_orders, 
count(distinct c.customerid) as num_of_customers,
round(count(o.orderid) * 1.0/ count(distinct c.customerid),1) 
as orders_per_customers
FROM
(
    SELECT customerid, customercity
    FROM cust
) c
FULL JOIN ord o on c.customerid = o.customerid
GROUP BY 1
ORDER BY 2 desc;
--Из результатов видно, что хотя Вашингтон и Хьюстон лидируют по количеству 
--заказов, но на каждого клиента в этих городах приходится в среднем по 1,5 заказа.

SELECT c.customercity, 
count(o.orderid) as num_of_orders, 
count(distinct c.customerid) as num_of_customers,
round(count(o.orderid) * 1.0/ count(distinct c.customerid),1) as orders_per_customers
FROM
(
    SELECT customerid, customercity
    FROM cust
) c
FULL JOIN ord o on c.customerid = o.customerid
GROUP BY 1
ORDER BY 4 desc
limit 10;
--Есть также города, где проживают 1-2 клиента, но за 2 года они сделали по 4 заказа.

--Постороим график зависимости количества городов от числа заказов на человека:
SELECT nums.num as orders_per_customers, coalesce(num_of_cities,0) as num_of_cities 
FROM 
( 
    SELECT orders_per_customers, count(distinct customercity) as num_of_cities 
    FROM 
    ( 
        SELECT c.customercity, 
        count(o.orderid) as num_of_orders, 
        count(distinct c.customerid) as num_of_customers, 
        round(count(o.orderid) * 1.0 / count(distinct c.customerid),1) 
        as orders_per_customers 
        FROM 
        ( 
            SELECT customerid, customercity 
            FROM cust 
        ) c 
        FULL JOIN ord o on c.customerid = o.customerid 
        GROUP BY 1 
        ORDER BY 4 desc 
    ) a 
    GROUP BY 1 
) b 
RIGHT JOIN GENERATE_SERIES(0.0, 4.0, 0.1) as nums(num) 
on b.orders_per_customers = nums.num;
--График находится в файле Bank_Muamalat_sales.ipynb.
--Видно, что клиенты из большинства городов за 2 года сделали 1 или 2 заказа.  



--4.1-2 КАК ШТАТЫ, В КОТОРЫХ ПРОЖИВАЮТ КЛИЕНТЫ, ВЛИЯЮТ НА ЗАКАЗЫ

--Посмотрим, из каких штатов приходит больше всего заказов, а из каких меньше:
(
    SELECT c.customerstate, count(o.orderid) as num_of_orders
    FROM cust c
    FULL JOIN ord o on c.customerid = o.customerid
    GROUP BY 1
    ORDER BY 2 desc
    limit 5
)
UNION ALL 
(
    SELECT c.customerstate, count(o.orderid) as num_of_orders
    FROM cust c
    FULL JOIN ord o on c.customerid = o.customerid
    GROUP BY 1
    ORDER BY 2 desc
    limit 5 offset 43
);
--Больше всего заказов приходит из штатов Калифорния, Техас и Флорида, а меньше 
--всего - из Вайоминга. Это связано с количеством клиентов в каждом штате: 
--больше всего клиентов наблюдалось в Калифорнии, Техасе и Флориде, а меньше 
--всего в Вайоминге (всего 1 клиент), Монтане, Северной Дакоте и Гавайях.  
  
--Выясним, сколько заказов приходится на каждого клиента в каждом штате:
SELECT c.customerstate, 
count(o.orderid) as num_of_orders, 
count(distinct c.customerid) as num_of_customers,
round(count(o.orderid) * 1.0/ count(distinct c.customerid),1) as orders_per_customers
FROM cust c
FULL JOIN ord o on c.customerid = o.customerid
GROUP BY 1
ORDER BY 2 desc;
--Больше всего заказов на человека приходится в штате Небраска (2.5), а меньше 
--всего - в штате Вайоминг (0). У штатов-лидеров по заказам количество заказов 
--на человека является средним показателем, т е само количество заказов велико 
--только из-за большого количества клиентов в этих штатах.  

--Сильная связь количества клиентов и количества заказов также подтверждается 
--высоким коэффициентом корреляции:
SELECT CORR(num_of_customers, num_of_orders) AS correlation
FROM
(
    SELECT c.customerstate, 
    count(o.orderid) as num_of_orders, 
    count(distinct c.customerid) as num_of_customers,
    round(count(o.orderid) * 1.0/ count(distinct c.customerid),1) 
    as orders_per_customers
    FROM cust c
    FULL JOIN ord o on c.customerid = o.customerid
    GROUP BY 1
    ORDER BY 2 desc
) a;



--4.2 ФИНАНСОВЫЕ ПОКАЗАТЕЛИ

--4.2-1 ВЫРУЧКА

--4.2-1-1 ВЫРУЧКА С ТЕЧЕНИЕМ ВРЕМЕНИ

--Посчитаем выручку по месяцам, за каждый год и общую:
SELECT
CASE WHEN grouping(extract(YEAR from date)) = 1 THEN 'All'
ELSE extract(YEAR from date)::text
END as year,
CASE WHEN grouping(extract(MONTH from date)) = 1 THEN 'All'
ELSE extract(MONTH from date)::text
END as month,
sum(cost) as month_revenue
FROM ord
GROUP BY ROLLUP (extract(YEAR from date), extract(MONTH from date)) 
ORDER BY 1,2;
--Общая выручка составила 175475057. Выручка за 2020 год - 91321009, а за 2021 
--2021 - 84154048.  

--Построим график изменения выручки по месяцам:
SELECT extract(YEAR from date) as year, extract(MONTH from date) as month, 
sum(cost)/1000000 as month_revenue 
FROM ord 
GROUP BY 1,2 
ORDER BY 1,2;
--График находится в файле Bank_Muamalat_sales.ipynb.
--Пунктирной красной линией обозначена линия тренда, а розовая область - это 
--95%-ный доверительный интервал.  
--Из графика видно, что выручка идет на спад: за 2 года она упала на 1 млн. 
--В то же время из-за доверительного интервала нельзя сказать, что это 
--наблюдение значимо.  

--Чтобы убедиться в этом, проведем T-тест.  
--Нулевая гипотеза: угол наклона кривой тренда равен 0, график тренда 
--горизонтален, и зависимая переменная (выручка) со временем не меняется.
--Результаты T-теста находится в файле Bank_Muamalat_sales.ipynb.
--p-уровень значимости 0.113 > 0.05 (порог значимости). Принимаем нулевую 
--гипотезу - выручка со временем не меняется. 



--4.2-1-2 ВЫРУЧКА ПО ГОРОДАМ

--Посмотрим, какой из городов вносит наибольший вклад в выручку:  
SELECT customercity, 
count(distinct orderid) as num_of_orders, 
sum(quantity) as sum_quantity,
avg(price) as avg_price,
sum(cost) as city_revenue
FROM
(
    SELECT o.orderid, customerid, customercity, o.prodnumber, quantity, cost, price
    FROM
    (
        SELECT orderid, customerid, prodnumber, quantity, cost
        FROM ord
    ) o
    JOIN cust c using(customerid)
    JOIN prod p using(prodnumber)
) a
GROUP BY 1
ORDER BY 5 desc
limit 10;
--Видно, что большую прибыль приносят Вашингтон и Хьюстон (как было выяснено 
--ранее, у них больше всего заказов). Следом идут такие города, как Сакраметно 
--и Сан-Диего. Жители этих городов также делают много заказов: в Сакраметно 
--покупают более дорогие продукты, но в меньшем количестве, а в Сан-Диего - 
--наоборот.



--4.2-2 СРЕДНИЙ ЧЕК

--4.2-2-1 СРЕДНИЙ ЧЕК С ТЕЧЕНИЕМ ВРЕМЕНИ

--Посчитаем средний чек по месяцам, за каждый год и за все время:
SELECT
CASE WHEN grouping(extract(YEAR from date)) = 1 THEN 'All'
ELSE extract(year from date)::text
END as year,
CASE WHEN grouping(extract(MONTH from date)) = 1 THEN 'All'
ELSE extract(MONTH from date)::text
END as month,
round(sum(cost)/count(*),2) as average_bill
FROM ord
GROUP BY ROLLUP (extract(YEAR from date), extract(MONTH from date)) 
ORDER BY 1,2;
--В 2020 году средний чек составил 53940.35, в 2021 - он упал до 51126.40. 
--Суммарный средний чек - 52553.18.  

--Построим график изменения среднего чека по месяцам:
SELECT extract(YEAR from date) as year, extract(MONTH from date) as month, 
round(sum(cost)/count(*),2) as average_bill 
FROM ord 
GROUP BY 1,2 
ORDER BY 1,2;
--График находится в файле Bank_Muamalat_sales.ipynb.
--Есть тенденция к уменьшению среднего чека со временем, но она довольно 
--слабая. В целом средний чек не изменяется.



--4.2-2-2 Средний чек по категориям с течением времени
--Посмотрим, как с течением времени изменялся средний чек каждой из категорий 
--товаров:
SELECT year, month,
sum(CASE WHEN category = 1 THEN average_bill ELSE 0 END) as category_1,
sum(CASE WHEN category = 2 THEN average_bill ELSE 0 END) as category_2,
sum(CASE WHEN category = 3 THEN average_bill ELSE 0 END) as category_3,
sum(CASE WHEN category = 4 THEN average_bill ELSE 0 END) as category_4,
sum(CASE WHEN category = 5 THEN average_bill ELSE 0 END) as category_5,
sum(CASE WHEN category = 6 THEN average_bill ELSE 0 END) as category_6,
sum(CASE WHEN category = 7 THEN average_bill ELSE 0 END) as category_7
FROM
(
    SELECT extract(YEAR from date) as year,
    extract(MONTH from date) as month,
    category, sum(cost) as month_category_cost, count(*) as num_of_orders,
    round(sum(cost)/count(*),2) as average_bill
    FROM
    (
        SELECT category, prodnumber
        FROM prod
    ) p
    RIGHT JOIN ord o using(prodnumber)
    GROUP BY 1,2,3
    ORDER BY 1,2,3
) a
GROUP BY 1,2;
--График находится в файле Bank_Muamalat_sales.ipynb.
--Тренд среднего чека с течением времени остается неизменным. Чем дороже 
--категория, тем больше средний чек и тем больше волатильность среднего чека.



--4.2-3 ARPU И ARPPU  

--Прежде чем приступить к расчетам, ознакомимся с понятиями:   
--ARPU (Average Revenue Per User — средний доход на одного пользователя) — 
--метрика, которая показывает, сколько в среднем приносит один пользователь 
--за определённый период.  
--ARPU = Доход от пользователей / Количество пользователей  
--ARPPU (Average Revenue Per Paying User — средний доход на одного платящего 
--пользователя) — метрика, которая показывает, сколько в среднем приносит один 
--пользователь, совершавший покупки, за определенный период.  
--ARPPU = Доход от пользователей / Количество платящих пользователей  

--ARPU мы можем посчитать лишь за все время наших данных (2 года), поскольку у 
--нас нет информации о количестве пользователей, взаимодействовавших с банком 
--Muamalat каждый месяц:
SELECT count(distinct c.customerid) as num_of_customers, sum(o.cost) as revenue,
round(sum(o.cost) / count(distinct c.customerid),2) as ARPU
FROM cust c
LEFT JOIN ord o on c.customerid = o.customerid;
--ARPU составило 82654.29.  

--Рассчитаем ARPPU за весь период, за каждый год и месяц:
SELECT 
CASE WHEN grouping(extract(YEAR from date)) = 1 THEN 'All'
ELSE extract(YEAR from date)::text
END as year,
CASE WHEN grouping(extract(MONTH from date)) = 1 THEN 'All'
ELSE extract(MONTH from date)::text
END as month, 
count(orderid) as num_of_orders, sum(cost) as revenue,
round(sum(cost) / count(orderid),2) as ARPPU
FROM ord
GROUP BY ROLLUP (extract(YEAR from date), extract(MONTH from date)) 
ORDER BY 1,2;
--ARPPU за все время составило 52553.18, за 2020 год - 53940.35, а за 2021 - 
--51126.40.  
--Наибольшие значения ARPPU были в октябре 2020 года и в сентябре 2021. 
--Наименьшие - в марте и июле 2021 года.



--4.3 Время

--Рассчитаем, сколько времени прошло с последней покупки клиентов в месяцах и 
--полугодиях:
SELECT months_since_latest, count(customerid) as num_of_customers
FROM
(
    SELECT c.customerid, max(o.date) as latest_date,
    (extract(YEAR from age('2021-12-31'::date, max(o.date))) * 12 +
    extract(MONTH from age('2021-12-31'::date, max(o.date)))) as months_since_latest
    FROM cust c
    JOIN ord o using(customerid)
    GROUP BY 1
) a
GROUP BY months_since_latest
ORDER BY months_since_latest asc;

SELECT seasons_since_latest, num_of_customers,
round(num_of_customers * 100.0 / sum(num_of_customers) over(),2) as pct_of_customers
FROM
(
    SELECT 
    CASE 
    WHEN seasons_since_latest <= 1 THEN 1
    WHEN seasons_since_latest <= 2 THEN 2
    WHEN seasons_since_latest <= 3 THEN 3
    WHEN seasons_since_latest <= 4 THEN 4
    END as seasons_since_latest, 
    count(customerid) as num_of_customers
    FROM
    (
        SELECT c.customerid, max(o.date) as latest_date,
        (extract(YEAR from age('2021-12-31'::date, max(o.date))) * 2 +
        extract(MONTH from age('2021-12-31'::date, max(o.date)))/6) as seasons_since_latest
        FROM cust c
        JOIN ord o using(customerid)
        GROUP BY 1
    ) a
    GROUP BY 1
    ORDER BY seasons_since_latest asc
) a;
--Видно, что 10,5% клиентов не возвращались за покупками в течение 1,5 лет, 
--17,5% - в течение года, а 26% - в течение полугода.  
--За последние пол года покупки совершили 46% клиентов.

--Рассмотрим проценты продаж по категориям от их совокупного объема по месяцам:
with ord_and_cat as(
SELECT o.date, o.prodnumber, o.cost, p.category, c.categoryname
FROM ord o
JOIN prod p using(prodnumber)
JOIN cat c on p.category = c.categoryid
)
SELECT date_trunc('month', date)::date as month,
category, categoryname, sum(cost) as cost,
sum(sum(cost)) over (partition by date_trunc('month', date)::date) as month_cost,
round(sum(cost) * 100.0 / 
sum(sum(cost)) over (partition by date_trunc('month', date)::date),2) as pct
FROM ord_and_cat
GROUP BY 1,2,3
ORDER BY 1,2;

--Построим график по этим данным для наглядности:
with ord_and_cat as(
SELECT o.date, o.prodnumber, o.cost, p.category, c.categoryname
FROM ord o
JOIN prod p using(prodnumber)
JOIN cat c on p.category = c.categoryid
)
SELECT month,
sum(CASE WHEN categoryname = 'Blueprints' THEN pct ELSE 0 END) as Blueprints,
sum(CASE WHEN categoryname = 'Drone Kits' THEN pct ELSE 0 END) as Drone_Kits,
sum(CASE WHEN categoryname = 'Drones' THEN pct ELSE 0 END) as Drones,
sum(CASE WHEN categoryname = 'eBooks' THEN pct ELSE 0 END) as eBooks,
sum(CASE WHEN categoryname = 'Robot Kits' THEN pct ELSE 0 END) as Robot_Kits,
sum(CASE WHEN categoryname = 'Robots' THEN pct ELSE 0 END) as Robots,
sum(CASE WHEN categoryname = 'Training Videos' THEN pct ELSE 0 END) as Training_Videos
FROM
(
    SELECT date_trunc('month', date)::date as month,
    category, categoryname, sum(cost) as cost,
    sum(sum(cost)) over (partition by date_trunc('month', date)::date) as month_cost,
    round(sum(cost) * 100.0 / 
    sum(sum(cost)) over (partition by date_trunc('month', date)::date),2) as pct
    FROM ord_and_cat
    GROUP BY 1,2,3
    ORDER BY 1,2
) a
GROUP BY 1;
--График находится в файле Bank_Muamalat_sales.ipynb.
--Больший вклад в выручку вносит категория robots (№6) - около 43% в среднем, 
--а наименьший - blueprints (№1) - около 1%.   
--Со временем доля продаж категории robots в общем объеме продаж падает, а 
--категории drones (№3) - растет. При этом доверительные интервалы указывают на 
--то, что эти изменения могут быть незначимыми.  

--Далее по категориям:
--* blueprints (№1) сезонных закономерностей не имеет.
--* drone kits (№2). Продажи составляют в среднем 9-10% всех продаж. Сезонных 
--закономерностей нет.  
--* drones (№3). В августе и сентябре доля продаж наоборот падает. Также можно 
--заметить, что и в 2020, и в 2021 году наблюдался рост в январе и падение в 
--феврале. Доля drones в общих продажах составляет около 27%.  
--* ebooks (№4) вности в выручку около 4%. Сезонных закономерностей нет.  
--* robot kits (№5). Вклад категории составляет окло 12-13%. С сентября по 
--октябрь доля этой категории в продажах как правило растет, а к декабрю - падает.  
--* robots (№6) имеет сезонную закономерность: рост доли продаж всегда 
--наблюдается в декабре и феврале, а также значительные пики - в августе и сентябре.  
--* training Videos (№7) составляет около 5%. Пики продаж этой категории часто 
--совпадают с категорией drones. Вероятно, их часто покупают вместе (например, 
--когда требуются обучающие видео по запуску дронов). 

--Изучим процент продаж по каждой категории от годового объема:
with ord_and_cut as (
SELECT date_trunc('month', o.date)::date as month, 
extract (YEAR from o.date) as year,
p.category, c.categoryname, sum(o.cost) as cost
FROM ord o
JOIN prod p using(prodnumber)
JOIN cat c on p.category = c.categoryid
GROUP BY 2,1,3,4
)
SELECT month,
sum(CASE WHEN categoryname = 'Blueprints' THEN pct_yearly ELSE 0 END) as Blueprints,
sum(CASE WHEN categoryname = 'Drone Kits' THEN pct_yearly ELSE 0 END) as Drone_Kits,
sum(CASE WHEN categoryname = 'Drones' THEN pct_yearly ELSE 0 END) as Drones,
sum(CASE WHEN categoryname = 'eBooks' THEN pct_yearly ELSE 0 END) as eBooks,
sum(CASE WHEN categoryname = 'Robot Kits' THEN pct_yearly ELSE 0 END) as Robot_Kits,
sum(CASE WHEN categoryname = 'Robots' THEN pct_yearly ELSE 0 END) as Robots,
sum(CASE WHEN categoryname = 'Training Videos' THEN pct_yearly ELSE 0 END) 
as Training_Videos
FROM
(
    SELECT month, category, categoryname, cost,
    sum(cost) over (partition by year, category) as yearly_sales,
    round(cost * 100.0 / sum(cost) over (partition by year, category),2)
    as pct_yearly
    FROM ord_and_cut
    ORDER BY 1
) a
GROUP BY 1;
--Графики находится в файле Bank_Muamalat_sales.ipynb.
--На графиках видно следующее:
--* Ebooks самая стабильная категория - она имеет наименьшую волатильность.  
--* Для robots % от годовых продаж от месяца к месяцу в среднем не меняется, 
--для training videos и robot kits наблюдается рост, а для blueprints, drone 
--kits, drones и ebooks наоборот падение.  
--* Графики drone и drone kits схожи. Это объясняется тем, что в самих 
--категориях (дроны и наборы дронов) находятся подобные товары. 
--* Также есть схожесть в графиках ebooks и training videos. Вероятно, люди 
--предпочитают покупать электронные книги и обучающие видео в одно время.



--5. ВЫВОДЫ

--1. Аномалии:   
--В указанные дни совсем не было заказов: 2020-02-23, 2020-11-12 и 2020-11-14.  

--2. Сводка по заказам:  
--    * Как заказы распределяются по месяцам:  
--        * В январе количество заказов всегда высокое, а к марту-апрелю 
--          снижается.  
--        * В другие месяцы зависимость обратная: когда в 2020 рост, в 2021 -
--          спад, и наоборот.  
--        * В сентябре 2021г. заказы упали, а дальнейший подъем был 
--          незначительным. Этот период следует изучить подробнее.    
--    * Сколько совершаеся заказов:  
--        * За месяц клиенты делают не более 3х заказов, за 2 года - не более 7.    
--        * Чаще всего покупают в количестве 3-4 штук.   
--    * Количество заказов сильно коррелируеет с колличеством клиентов в штатах: 
--      чем больше клиентов в штате, тем больше заказов поступает из этого штата. 
--      При этом количество заказов на человека у штатов-лидеров по заказам среднее. 
--    * Для городов картина аналогична. Больше всего заказов приходит из Вашингтона 
--      и Хьюстона, они вносят основной клад в выручку.
 
--3. Сводка по клиентам:   
--10,5% клиентов не возвращались за покупками в течение 1,5 лет, 17,5% - в 
--течение года, а 26% - в течение полугода.  
--За последние пол года покупки совершили 46% клиентов.  

--4. Сводка по продуктам:
--Большая часть продуктов стоит от 1000 до 5000. Самая высокая цена у товара 
--MICR-23K Robot(89900.0), а самая низкая - у Cat Robot Blueprint(499.0).  
--Самым популярным продуктом является Sleepy Eye Blueprint. Наименее популярным 
--является Upside Down Robot Blueprint - за два года этот товар ни разу не заказали.  

--5. Сводка по категориям продуктов:
--    * Больше всего продуктов представлено в категории eBooks, а меньше - в 
--      категориях Robot Kits и Robots.     
--    * Самая бюджетная категория - Blueprints, а самая дорогая - Robots.  
--    * Больший вклад в выручку вносит категория robots (43%), а наименьший - 
--      blueprints (1%).  
--    * Drone и drone kits - подобные категории. У них схожи проценты продаж от 
--      годового объема. 
--    * Товары категорий ebooks и training videos часто востребованы в одни и 
--      те же временные периоды.

--6. Сезонные закономерности в доле продаж от общих продаж:
--    * Drones. В августе и сентябре доля продаж падает. Также наблюдается 
--      рост в январе и падение в феврале. 
--    * Robot kits. С сентября по октябрь доля этой категории в продажах как 
--      правило растет, а к декабрю - падает. 
--    * Robots. Рост наблюдается в декабре и феврале, а также в августе и 
--      сентябре. Пики продаж этой категории часто совпадают с категорией 
--      training Videos. 

--7. Финансовые показатели:
--    * Общая выручка составила 175475057. Выручка за 2020 год - 91321009, а за 
--      2021 - 84154048. Выручка со временем значительно не меняется.  
--    * Суммарный средний чек - 52553.18. В 2020 году средний чек составил 
--      53940.35, в 2021 - 51126.40.  Средний чек со временем значительно не 
--      изменяется.
--    * Чем дороже категория, тем больше ее средний чек и тем больше 
--      волатильность.
--    * ARPU составило 82654.29.    
--    * ARPPU за все время составило 52553.18, за 2020 год - 53940.35, а за 
--      2021 - 51126.40.  


--Гипотезы:  
--1. Если сделать рекламу, ориентированную на клиентов, которые не возвращались 
--   более 1,5 лет, то это увеличит количество новых заказов.
--2. Если сделать рекламу, направленую на те города, где сейчас живут по одному 
--   клиенту и большая численность населения, то это увеличит количество клиентов 
--   в этих городах.
--4. Если вместе с товарами категории drones предлагать товары категории training 
--   Videos или ebooks, то это увеличит средний чек.
--5. Если убрать Upside Down Robot Blueprint с продаж, то это сократит время 
--   работающих над ним людей и позволит сэкономить на хранении товара. 

--Дополнительно:
--1. Следует изучить влияние географического расположения штатов и размера 
--   населения на количество клиентов.
--2. Следует обратить внимание на финансовые показатели. Видно, что размеры 
--   выручки, среднего чека и ARPPU со временем падают, хотя результаты и не 
--   были значимыми.

