/* 2 варианта: в первом используется временная таблица. Такой запрос потребует больше ресурсов, но он более безопасный и контролируемый
 Во втором варианте используется CTE. Такой запрос будет более эффективным, тк удаляет дубликаты сразу в основной таблице, но может быть рискованным на больших таблицах*/

-- Вариант 1
-- Создаем временную таблицу
create temporary table temp_client as
select * from dm.client;

-- Удаляем из временной таблицы дубликаты
delete from temp_client
where (client_rk, effective_from_date, ctid) not in (
	select client_rk, effective_from_date, min(ctid)
	from temp_client
	group by client_rk, effective_from_date
	);

select count(*) from dm.client; -- количество записей в основной таблице
select count(*) from temp_client; -- количество записей во временной таблице

begin;
-- удаляем записи из таблицы
delete from dm.client;
-- вставляем записи из временной таблицы
insert into dm.client 
select * from temp_client;
commit;

-- удаляем временну таблицу
drop table temp_client;




-- Вариант 2
-- разбиваем на группы и присваиваем строкам порядковые номера
with duplicates as (
	select ctid, row_number() over (partition by client_rk, effective_from_date order by ctid) as rn 
	from dm.client
	)
-- удаляем дубли из таблицы
delete from dm.client
where ctid in (
	select ctid
	from duplicates
	where rn > 1
	);