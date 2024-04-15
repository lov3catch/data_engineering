/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT c.name, COUNT(fc.film_id) AS films_count
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
GROUP BY c.category_id
ORDER BY films_count DESC;



/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
select t1.total_amount, category.name
from (select sum(payment.amount) as total_amount, film_category.category_id
      from rental
               left join payment on payment.rental_id = rental.rental_id
               left join inventory on rental.inventory_id = inventory.inventory_id
               left join film_category on inventory.film_id = film_category.film_id
      group by film_category.category_id) as t1
         left join category on t1.category_id = category.category_id
order by total_amount desc;





/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
select distinct film.title
from film
         left outer join inventory on film.film_id = inventory.film_id
where inventory is null;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...