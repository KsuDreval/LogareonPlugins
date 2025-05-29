using System;
using Npgsql;

class sqlQuery
{
    public static void Main ()
    {
        //строка для подключения
        string connectionStrig = "Host=192.168.200.13;Port=5432;Database=DatamartDocker2;Username=postgres;Password=postgres;";
        //запрос в бд
        string queryString = $@"WITH 
        -- Все SKU из таблицы товаров с нужными полями
        all_skus AS (
            SELECT 
                s.""uuid"" AS sku_uuid,
                s.""Name"" AS sku_name,
                s.""uuid"" AS ox_uuid,
                s.""domain"" AS sku_domain
            FROM 
                sku s
        ),

        -- Определяем дату начала периода (последние 7 дней)
        date_range AS (
            SELECT 
                CURRENT_DATE - INTERVAL '200 days' AS start_date,
                CURRENT_DATE AS end_date
        ),

        -- Товары, которые были отгружены (Shipped) за последнюю неделю
        shipped_skus AS (
            SELECT DISTINCT
                c.""SKU"" AS sku_uuid
            FROM 
                shippingorder so
                JOIN shippingorder_lines sol ON so.""uuid"" = sol.""parentuuid""
                JOIN Commodity c ON sol.""Commodity"" = c.""uuid""
            WHERE 
                so.""State"" = 'Shipped'
                AND so.""ShippingDate"" >= (SELECT start_date FROM date_range)
                AND so.""ShippingDate"" <= (SELECT end_date FROM date_range)
        ),

        -- Суммируем количество проданных товаров (только отгруженные за последнюю неделю)
        sales_data AS (
            SELECT 
                c.""SKU_name"" AS CommodityName,
                c.""SKU"" AS sku_uuid,
                SUM(sol.""QuantityPackage"") AS total_quantity
            FROM 
                shippingorder so
                JOIN shippingorder_lines sol ON so.""uuid"" = sol.""parentuuid""
                JOIN Commodity c ON sol.""Commodity"" = c.""uuid""
            WHERE 
                so.""State"" = 'Shipped'
                AND so.""ShippingDate"" >= (SELECT start_date FROM date_range)
                AND so.""ShippingDate"" <= (SELECT end_date FROM date_range)
            GROUP BY 
                c.""SKU_name"", c.""SKU""
        ),

        -- Объединяем все SKU с отметкой были ли они отгружены
        combined_data AS (
            SELECT 
                a.sku_name AS CommodityName,
                a.ox_uuid AS ox_uuid,
                a.sku_domain AS sku_domain,
                COALESCE(sd.total_quantity, 0) AS total_quantity,
                CASE WHEN ss.sku_uuid IS NOT NULL THEN 1 ELSE 0 END AS was_shipped
            FROM 
                all_skus a
            LEFT JOIN 
                shipped_skus ss ON a.sku_uuid = ss.sku_uuid
            LEFT JOIN 
                sales_data sd ON a.sku_uuid = sd.sku_uuid
        ),

        -- Вычисляем общую сумму для отгруженных товаров за период
        total_sales AS (
            SELECT SUM(total_quantity) AS total
            FROM combined_data
            WHERE was_shipped = 1
        ),

        -- Рассчитываем доли и кумулятивную долю только для отгруженных товаров
        quantity_analysis AS (
            SELECT 
                cd.CommodityName,
                cd.ox_uuid,
                cd.sku_domain,
                cd.total_quantity,
                cd.was_shipped,
                CASE 
                    WHEN cd.was_shipped = 1 AND ts.total > 0 THEN 
                        cd.total_quantity / ts.total
                    ELSE 0
                END AS quantity_share,
                CASE 
                    WHEN cd.was_shipped = 1 THEN 
                        SUM(cd.total_quantity) OVER (ORDER BY CASE WHEN cd.was_shipped = 1 THEN cd.total_quantity ELSE 0 END DESC) / ts.total
                    ELSE 0
                END AS cumulative_quantity_share
            FROM 
                combined_data cd
            CROSS JOIN 
                total_sales ts
        )

        -- Классифицируем товары и выводим период анализа
        SELECT 
            CommodityName,
            ox_uuid AS ""OX_UUID"",
            sku_domain AS ""SKU_Domain"",
            total_quantity,
            CASE 
                WHEN was_shipped = 1 THEN quantity_share * 100 
                ELSE 0 
            END AS quantity_percentage,
            CASE 
                WHEN was_shipped = 1 THEN cumulative_quantity_share * 100 
                ELSE 0 
            END AS cumulative_percentage,
            CASE 
                WHEN was_shipped = 0 THEN 'D' -- Товары, которые не были отгружены
                WHEN cumulative_quantity_share <= 0.8 THEN 'A' -- 80% объема
                WHEN cumulative_quantity_share <= 0.95 THEN 'B' -- 15% объема
                ELSE 'C' -- 5% объема
            END AS abc_category,
            (SELECT start_date FROM date_range) AS ""Period_Start"",
            (SELECT end_date FROM date_range) AS ""Period_End""
        FROM 
            quantity_analysis
        ORDER BY 
            CASE 
                WHEN was_shipped = 0 THEN 4 -- Категория D в конце
                WHEN cumulative_quantity_share <= 0.8 THEN 1
                WHEN cumulative_quantity_share <= 0.95 THEN 2
                ELSE 3
            END,
            total_quantity DESC;";

        NpgsqlConnection conn = new NpgsqlConnection(connectionStrig);
        conn.Open ();

        NpgsqlCommand cmd = new NpgsqlCommand (queryString, conn);

        NpgsqlDataReader reader = cmd.ExecuteReader();

        for (int i = 0; i < 10; i++)
        {
            if (reader.Read())
            {
                foreach (var item in reader)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            else break;
        }
        reader.Close ();
        conn.Close ();
    }
}