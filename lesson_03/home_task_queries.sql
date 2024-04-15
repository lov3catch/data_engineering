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
SELECT t1.total_amount, category.name
FROM (SELECT SUM(payment.amount) AS total_amount, film_category.category_id
      FROM rental
               LEFT JOIN payment ON payment.rental_id = rental.rental_id
               LEFT JOIN inventory ON rental.inventory_id = inventory.inventory_id
               LEFT JOIN film_category ON inventory.film_id = film_category.film_id
      GROUP BY film_category.category_id) AS t1
         LEFT JOIN category ON t1.category_id = category.category_id
ORDER BY total_amount DESC;






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