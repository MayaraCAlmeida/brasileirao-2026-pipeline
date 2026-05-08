# -*- coding: utf-8 -*-
"""
spark/transform_spark.py
Versao PySpark das transformacoes do pipeline do Brasileirao.

Execucao local:
    pip install pyspark
    python spark/transform_spark.py
"""

import os
import sys

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# inicializa SparkSession local
spark = (
    SparkSession.builder.appName("BrasileiraoTransform")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("[OK] SparkSession iniciada")

# faz a leitura dos CSVs
BASE = "dados_processados"

classificacao = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("encoding", "UTF-8")
    .csv(os.path.join(BASE, "tabela.csv"))
)

artilharia = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("encoding", "UTF-8")
    .csv(os.path.join(BASE, "artilharia.csv"))
)

print(f"[OK] Classificacao: {classificacao.count()} times")
print(f"[OK] Artilharia: {artilharia.count()} jogadores")

# gera as metricas por jogo
team_stats = (
    classificacao.withColumn("saldo_gols", F.col("gols_pro") - F.col("gols_contra"))
    .withColumn("media_gols_pro", F.round(F.col("gols_pro") / F.col("jogos"), 2))
    .withColumn("media_gols_contra", F.round(F.col("gols_contra") / F.col("jogos"), 2))
    .withColumn("media_saldo_gols", F.round(F.col("saldo_gols") / F.col("jogos"), 2))
)

# performance Score
# Score = (aproveitamento x 0.5) + (saldo/jogo x 0.3) + (gols_pro/jogo x 0.2)
team_stats = team_stats.withColumn(
    "performance_score",
    F.round(
        (F.col("aproveitamento") * 0.5)
        + (F.col("saldo_gols") / F.col("jogos") * 0.3)
        + (F.col("gols_pro") / F.col("jogos") * 0.2),
        2,
    ),
)

# zona da tabela
team_stats = team_stats.withColumn(
    "zona",
    F.when(F.col("posicao") <= 4, "Libertadores (G4)")
    .when(F.col("posicao") <= 6, "Sul-Americana (SulAmericana)")
    .when(F.col("posicao") >= 17, "Rebaixamento (Z4)")
    .otherwise("Meio da tabela"),
)

# gera o ranking de artilharia com window function
window_rank = Window.orderBy(F.col("gols").desc())
artilharia_ranked = artilharia.withColumn("ranking", F.rank().over(window_rank))

# resultados
print("\n--- Top 5 times por Performance Score ---")
team_stats.select("posicao", "time", "pontos", "performance_score", "zona").orderBy(
    F.col("performance_score").desc()
).show(5, truncate=False)

print("--- Top 5 artilheiros ---")
artilharia_ranked.select("ranking", "jogador", "clube", "gols").orderBy("ranking").show(
    5, truncate=False
)

print("--- Estatisticas por zona ---")
team_stats.groupBy("zona").agg(
    F.count("*").alias("times"),
    F.round(F.avg("aproveitamento"), 1).alias("aproveitamento_medio"),
    F.round(F.avg("performance_score"), 1).alias("score_medio"),
).orderBy("zona").show(truncate=False)

# salva em Parquet
# out = os.path.join(BASE, "spark")
# team_stats.write.mode("overwrite").csv(os.path.join(out, "team_stats"), header=True)
# artilharia_ranked.write.mode("overwrite").parquet(os.path.join(out, "artilharia"))

# print(f"\n[OK] Transformacoes PySpark concluidas. Arquivos salvos em {out}/")

print("\n[OK] Transformacoes PySpark concluidas.")

spark.stop()
