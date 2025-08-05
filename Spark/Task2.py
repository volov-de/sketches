from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when, lower

# Создаем SparkSession


# Читаем данные
users_df = spark.read.csv(
    "s3a://datasets/users2.csv",
    header=True,
    inferSchema=True
)

transactions_df = spark.read.csv(
    "s3a://datasets/transactions.csv",
    header=True,
    inferSchema=True
)

# выбираем нужные столбцы
users_clean = users_df.select(
    "user_id",
    "email",
    "registration_date"
)

# выбираем нужные столбцы
transactions_clean = transactions_df.select(
    "transaction_id",
    "user_id",
    "transaction_date",
    "amount",
    "status",
    "payment_method",
    "currency"
)

# Объединяем таблицы и считаем метрики
user_transactions = users_clean.join(
    transactions_clean,
    "user_id",
    "left"
)

# обратите внимание на поля amount, payment_method и currency
user_metrics = user_transactions.groupBy("user_id").agg(
    count("transaction_id").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    count(when(col("payment_method").isin(["card"]), 1)).alias("card_payments_count"),
    count(when(col("payment_method").isin(["paypal"]), 1)).alias("paypal_payments_count"),
    count(when(col("currency").isin(["usd"]), 1)).alias("usd_payments_count"),
    count(when(col("currency").isin(["eur"]), 1)).alias("eur_payments_count")
)

# Добавляем дополнительную информацию о пользователях
final_df = user_metrics.join(
    users_clean,
    "user_id"
).select(
    "user_id",
    "email",
    "registration_date",
    "transaction_count",
    "total_amount",
    "card_payments_count",
    "paypal_payments_count",
    "usd_payments_count",
    "eur_payments_count"
)


# Проверки качества данных
duplicates = final_df.groupBy("user_id").count().filter(col("count") > 1).count()
null_amounts = final_df.filter(col("total_amount").isNull()).count()

print(f"Found {duplicates} duplicate user_ids")
print(f"Found {null_amounts} users with null total_amount")

# Пример результата
final_df.show(5)
