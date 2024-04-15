/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- Films in categories
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
-- Most popular actors
WITH TopActors AS (SELECT film_actor.actor_id,
                          COUNT(rental.rental_id) AS rental_count
                   FROM rental
                            JOIN inventory ON inventory.inventory_id = rental.inventory_id
                            JOIN film_actor ON film_actor.film_id = inventory.film_id
                   GROUP BY film_actor.actor_id
                   ORDER BY rental_count DESC
                   LIMIT 10)
SELECT TopActors.actor_id,
       actor.first_name,
       actor.last_name,
       TopActors.rental_count
FROM TopActors
         JOIN actor ON actor.actor_id = TopActors.actor_id
ORDER BY rental_count desc;




/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
-- Top category by spend amount
SELECT t1.total_amount, category.name
FROM (SELECT SUM(payment.amount) AS total_amount, film_category.category_id
      FROM rental
               LEFT JOIN payment ON payment.rental_id = rental.rental_id
               LEFT JOIN inventory ON rental.inventory_id = inventory.inventory_id
               LEFT JOIN film_category ON inventory.film_id = film_category.film_id
      GROUP BY film_category.category_id) AS t1
         LEFT JOIN category ON t1.category_id = category.category_id
ORDER BY total_amount DESC;

-- Top category by spend amount (v2)
SELECT DISTINCT SUM(p.amount) OVER (PARTITION BY c.category_id) AS total_amount,
                c.name
FROM rental r
         JOIN payment p ON r.rental_id = p.rental_id
         JOIN inventory i ON r.inventory_id = i.inventory_id
         JOIN film_category fc ON i.film_id = fc.film_id
         JOIN category c ON fc.category_id = c.category_id
ORDER BY total_amount DESC;




/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- Selects titles of films that are not available in the inventory
SELECT DISTINCT f.title
FROM film f
         LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.inventory_id IS NULL;



/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- Selects top 3 actors who appeared most frequently in films categorized as 'Children'
SELECT
    a.first_name,
    a.last_name,
    COUNT(fa.actor_id) AS appearance_count
FROM
    film_actor fa
    LEFT JOIN actor a ON fa.actor_id = a.actor_id
    JOIN film_category fc ON fa.film_id = fc.film_id
    JOIN category c ON c.category_id = fc.category_id
WHERE
    c.name = 'Children'
GROUP BY
    fa.actor_id,
    a.first_name,
    a.last_name
ORDER BY
    appearance_count DESC
LIMIT 3;
