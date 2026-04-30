/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Лисаконова Валентина
 * Дата: 28.03.26
*/


/* Часть 1. Разработка витрины данных
 * Напишите ниже запрос для создания витрины данных
*/
WITH only_delivered_or_canceled_orders AS (
    SELECT *
    FROM ds_ecom.orders
    WHERE order_status IN ('Доставлено', 'Отменено')
),
only_top3_regions AS (
    SELECT *
    FROM ds_ecom.users
    WHERE region IN (
        SELECT region
        FROM ds_ecom.orders
        LEFT JOIN ds_ecom.users USING (buyer_id)
        WHERE order_status IN ('Доставлено', 'Отменено')
        GROUP BY region
        ORDER BY COUNT(*) DESC
        LIMIT 3
    )
),
orders_id_with_installment AS (
    SELECT DISTINCT order_id, 1 AS installment
    FROM ds_ecom.order_payments
    WHERE payment_installments > 1
),
payments_with_promokod AS (
    SELECT DISTINCT order_id, 1 AS having_promo
    FROM ds_ecom.order_payments
    WHERE payment_type = 'промокод'
),
orders_with_money_transfer_payment AS (
    SELECT DISTINCT order_id, 1 AS money_transfer
    FROM ds_ecom.order_payments
    WHERE payment_sequential = (SELECT MIN(payment_sequential)
        FROM ds_ecom.order_payments AS p2
        WHERE p2.order_id = order_payments.order_id)
    AND payment_type = 'денежный перевод'
),
table_with_conditions AS (
    SELECT *
    FROM only_delivered_or_canceled_orders
    JOIN only_top3_regions USING (buyer_id)
    LEFT JOIN (
        SELECT DISTINCT order_id,
            AVG(
                CASE
                    WHEN review_score > 5 THEN review_score / 10.0
                    ELSE review_score
                END
            ) AS order_rate
        FROM ds_ecom.order_reviews
        GROUP BY order_id
    ) AS order_reviews_agg USING (order_id)
    LEFT JOIN (
        SELECT order_id, (SUM(price) + SUM(delivery_cost)) AS total_cost
        FROM ds_ecom.order_items
        GROUP BY order_id
    ) AS order_costs USING (order_id)
    LEFT JOIN orders_id_with_installment USING (order_id)
    LEFT JOIN payments_with_promokod USING (order_id)
    LEFT JOIN orders_with_money_transfer_payment USING (order_id)
)
SELECT DISTINCT
    user_id,
    region,
    MIN(order_purchase_ts) OVER (PARTITION BY user_id, region) AS first_order_ts,
    MAX(order_purchase_ts) OVER (PARTITION BY user_id, region) AS last_order_ts,
    LAST_VALUE(order_purchase_ts) OVER (PARTITION BY user_id, region ORDER BY order_purchase_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) -
    FIRST_VALUE(order_purchase_ts) OVER (PARTITION BY user_id, region ORDER BY order_purchase_ts) AS lifetime,
    COUNT(order_id) OVER (PARTITION BY user_id, region) AS total_orders,
    AVG(order_rate) OVER (PARTITION BY user_id, region) AS avg_order_rating,
    COUNT(order_rate) OVER (PARTITION BY user_id, region) AS num_orders_with_rating,
    COUNT(CASE WHEN order_status = 'Отменено' THEN 1 END) OVER (PARTITION BY user_id, region) AS num_canceled_orders,
    (COUNT(CASE WHEN order_status = 'Отменено' THEN 1 END) OVER (PARTITION BY user_id, region)) /
    (COUNT(order_id) OVER (PARTITION BY user_id, region))::NUMERIC AS canceled_orders_ratio,
    SUM(CASE WHEN order_status = 'Доставлено' THEN total_cost ELSE 0 END) OVER (PARTITION BY user_id, region) AS total_order_costs,
    AVG(CASE WHEN order_status = 'Доставлено' THEN total_cost END) OVER (PARTITION BY user_id, region) AS avg_order_cost,
    COALESCE(SUM(installment) OVER (PARTITION BY user_id, region), 0) AS num_installment_orders,
    COALESCE(SUM(having_promo) OVER (PARTITION BY user_id, region), 0) AS num_orders_with_promo,
    CASE WHEN (SUM(money_transfer) OVER (PARTITION BY user_id, region)) >= 1 THEN 1 ELSE 0 END AS used_money_transfer,
    CASE WHEN (SUM(installment) OVER (PARTITION BY user_id, region) >= 1) THEN 1 ELSE 0 END AS used_installments,
    CASE WHEN (COUNT(CASE WHEN order_status = 'Отменено' THEN 1 END) OVER (PARTITION BY user_id, region) >= 1) THEN 1 ELSE 0 END AS used_cancel
FROM table_with_conditions;



/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/

/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/

WITH user_stats AS (
    SELECT
        user_id,
        SUM(total_orders) AS sum_total_orders,
        SUM(total_order_costs) AS sum_total_orders_costs
    FROM ds_ecom.product_user_features
    GROUP BY user_id
),
segmentation AS (
    SELECT user_id, sum_total_orders, sum_total_orders_costs,
        CASE
            WHEN sum_total_orders = 1 THEN '1 заказ'
            WHEN sum_total_orders BETWEEN 2 AND 5 THEN '2—5 заказов'
            WHEN sum_total_orders BETWEEN 6 AND 10 THEN '6–10 заказов'
            WHEN sum_total_orders >= 11 THEN '11 и более заказов'
        END AS user_segment,
        sum_total_orders_costs::NUMERIC / sum_total_orders AS one_order_middle_price
    FROM user_stats
)
SELECT
    user_segment,
    COUNT(user_id) AS total_users_segment,
    ROUND(AVG(sum_total_orders), 2) AS avg_total_segment_orders,
    ROUND(AVG(one_order_middle_price), 2) AS avg_segment_order_cost
FROM segmentation
GROUP BY user_segment
ORDER BY user_segment;
/* Напишите краткий комментарий с выводами по результатам задачи 1.
 * 
*/

-- За указанный период зарегистрированные пользователи чаще всего совершают только один заказ (60452 пользователя).
-- 1942 пользователя совершают от 2 до 5 заказов, 5 пользователей совершают от 6 до 10 заказов,
-- и только один пользователь совершает 11 и более заказов.
-- Большая часть зарегистрированных пользователей совершает единичную покупку,
-- и только 1948 пользователей возвращались за повторными покупками.
-- Пока что недостаточно данных для того, чтобы выявить причины того, что большая часть пользователей совершила только 1 заказ.
-- Средняя стоимость заказа уменьшается с увеличением количества заказов. Пользователи, совершившие один заказ,
-- на один заказ тратят больше денег (3,324.36), чем пользователи, которые совершают заказы более регулярно.
-- Возможно, пользователи, совершившие один заказ, закупаются в большем объёме и с расчётом на долгий срок, а пользователи,
-- совершающие заказы на меньшую сумму, закупаются в меньшем объёме, но регулярно. (но это ГИПОТЕЗА)

/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/

WITH user_stats AS (
    SELECT
        user_id,
        SUM(total_orders) AS sum_total_orders,
        SUM(total_order_costs) AS sum_total_orders_costs
    FROM ds_ecom.product_user_features
    GROUP BY user_id
),
segmentation AS (
    SELECT *,
        CASE
            WHEN sum_total_orders = 1 THEN '1 заказ'
            WHEN sum_total_orders BETWEEN 2 AND 5 THEN '2—5 заказов'
            WHEN sum_total_orders BETWEEN 6 AND 10 THEN '6–10 заказов'
            WHEN sum_total_orders >= 11 THEN '11 и более заказов'
        END AS user_segment,
        sum_total_orders_costs::NUMERIC / sum_total_orders AS one_order_middle_price
    FROM user_stats
)
SELECT
    user_id,
    sum_total_orders,
    one_order_middle_price,
    DENSE_RANK() OVER (ORDER BY one_order_middle_price DESC) AS user_rank
FROM segmentation
WHERE sum_total_orders >= 3
ORDER BY one_order_middle_price DESC
LIMIT 15;

/* Напишите краткий комментарий с выводами по результатам задачи 2.
 * 
*/
-- Наибольший средний чек покупки ~14716, наименьший – ~5526.
-- Самый большой разрыв в средних чеках топ-2 покупателей – ~2300.
-- Разница остальных чеков (отсортированных) не превышает 1000 рублей (более однородна).
-- Большинство пользователей из топ-15 (12 из 15) сделали ровно 3 заказа.


/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/

SELECT
    region,
    COUNT(user_id) AS total_region_users,
    SUM(total_orders) AS total_region_orders,
    ROUND(SUM(total_order_costs)::NUMERIC / SUM(total_orders), 2) AS middle_price_region_order,
    ROUND(SUM(num_installment_orders)::NUMERIC / SUM(total_orders) * 100, 2) || '%' AS installment_region_ratio,
    ROUND(SUM(num_orders_with_promo)::NUMERIC / SUM(total_orders) * 100, 2) || '%' AS promo_region_ratio,
    ROUND(AVG(used_cancel) * 100, 2) || '%' AS users_used_cancel_ratio
FROM ds_ecom.product_user_features
GROUP BY region;

/* Напишите краткий комментарий с выводами по результатам задачи 3.
 * 
*/

-- Наибольшее количество пользователей, совершивших заказы, в Москве (39,386),
-- в Новосибирской области и Санкт-Петербурге примерно
-- одинаковое количество покупателей (Новосибирская область – 11,044, Санкт-Петербург – 11,978).
-- Из-за преобладающего количества покупателей в Москве наблюдается
-- наибольшее количество заказов, также существенно превышающих заказы
-- Новосибирской области и Санкт-Петербурга (Москва – 40,747, Санкт-Петербург – 12,414, Новосибирская область – 11,401).
-- Количество заказов в регионах не сильно превышает количество пользователей, следовательно,
-- большая часть покупателей сделала по одному заказу.
-- Средняя стоимость одного заказа в Санкт-Петербурге и Новосибирской области отличается на ~102
-- (наибольшая средняя стоимость одного заказа наблюдается в Санкт-Петербурге – 3,593.46,
-- в Новосибирской области средняя стоимость одного заказа – 3,491.79),
-- наибольший отрыв в стоимости с Москвой (средняя стоимость одного заказа – 3,140.14).
-- Во всех регионах примерно половина заказов была оплачена в рассрочку
-- (Москва – 47.73%, Новосибирская область – 54.14%, Санкт-Петербург – 54.66%).
-- Примерно 4% покупок во всех регионах были оплачены с использованием промокода
-- (Новосибирская область – 3.68%, Москва – 3.74%, Санкт-Петербург – 4.16%).
-- Доля пользователей, отменивших заказ, во всех регионах составляет менее 1%
-- (в Москве доля наибольшая – 0.63%, наименьшая доля в Новосибирской области – 0.43%,
-- в Санкт-Петербурге доля 0.53%).

/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/

WITH first_user_order AS (
    SELECT
        user_id,
        MIN(first_order_ts) AS min_first_order_ts,
        SUM(total_orders) AS user_total_orders,
        SUM(total_order_costs) AS sum_total_order_costs,
        SUM(avg_order_rating * num_orders_with_rating) AS weighted_rating,
        SUM(num_orders_with_rating) AS total_orders_with_rating,
        AVG(lifetime) AS avg_user_session_duration,
        CASE
            WHEN SUM(used_money_transfer) >= 1 THEN 1
            ELSE 0
        END AS user_used_money_transfer
    FROM ds_ecom.product_user_features
    WHERE date_trunc('month', first_order_ts) BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY user_id
)
SELECT
    TO_CHAR(min_first_order_ts, 'TMMon YYYY') AS month_first,
    COUNT(user_id) AS count_users,
    SUM(user_total_orders) AS total_orders_user_month,
    SUM(sum_total_order_costs) / SUM(user_total_orders) AS one_order_middle_cost,
    SUM(weighted_rating) / SUM(total_orders_with_rating) AS avg_month_orders_rating,
    SUM(user_used_money_transfer)::NUMERIC / COUNT(user_id) AS share_money_transfer,
    AVG(avg_user_session_duration)::INTERVAL(0) AS avg_session_duration
FROM first_user_order
GROUP BY TO_CHAR(min_first_order_ts, 'TMMon YYYY');

-- Чаще всего пользователи совершали первый заказ в ноябре (4703 пользователя),
-- наименьшее количество пользователей в январе (465).
-- С февраля по июнь количество пользователей варьируется от 1063 до 2197,
-- с июля по октябрь – от 2463 до 2831, декабрь – 3587.
-- Количество заказов соответствует количеству пользователей: чем больше пользователей, тем больше заказов.
-- Средняя цена одного заказа варьируется от ~2556 (февраль) до ~3263 (сентябрь).
-- Средняя оценка заказов в каждой группе составляет ~4.1-4.3
-- (наибольшая — сентябрь (~4.27), наименьшая — ноябрь (~4.01)).
-- Доля пользователей, использующих для оплаты "денежный перевод",
-- наибольшая в феврале (~0.22), наименьшая в ноябре (~0.19).
-- Средняя продолжительность активности наибольшая у пользователей,
-- совершивших первый заказ в январе (12 days 19:37:10),
-- наименьшая — у пользователей, совершивших первый заказ в декабре (2 days 05:56:09).