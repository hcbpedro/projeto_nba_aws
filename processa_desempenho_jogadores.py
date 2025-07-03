

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# Funções Spark usadas:
from pyspark.sql.functions import col, sum, countDistinct, avg, when, lit, to_date, split, expr, substring, concat



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ==================================================================================
# INÍCIO DO CÓDIGO DE PROCESSAMENTO DE DADOS PARA O TEMA 1
# ==================================================================================

print("Iniciando Job: Processa_Desempenho_Jogadores_NBA (Processando Todas as Temporadas)")

# --- 1. Definindo Caminhos e Carregando Dados ---
s3_bucket_path = "s3://meu-projeto-nba-dados-pchb/dados-brutos/"

path_box_scores = f"{s3_bucket_path}NBA Player Box Score Stats(1950 - 2022).csv"
# O nome do arquivo 'NBA Player Stats(1950 - 2022).csv' foi usado no upload.
path_player_stats_agg_upload = f"{s3_bucket_path}NBA Player Stats(1950 - 2022).csv"
path_salaries_upload = f"{s3_bucket_path}NBA Salaries(1990-2023).csv"

print(f"Carregando dados de box score de forma robusta de: {path_box_scores}")

# Ler o CSV sem usar a primeira linha como cabeçalho diretamente e sem inferir esquema inicialmente
raw_df_box_scores_with_header_row = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "false") \
    .option("sep", ",") \
    .load(path_box_scores)

# Remover a primeira linha (que é o cabeçalho problemático)
# Usando RDDs para pular a primeira linha de forma confiável
schema_com_nomes_genericos = raw_df_box_scores_with_header_row.schema # Schema com _c0, _c1,...
rdd_sem_header = raw_df_box_scores_with_header_row.rdd.zipWithIndex().filter(lambda r_idx: r_idx[1] > 0).map(lambda r_idx: r_idx[0])

# Lista dos nomes corretos das colunas, conforme o cabeçalho
# (começando com um nome para a primeira coluna "vazia")
column_names = [
    "unnamed_index", "Season", "Game_ID", "PLAYER_NAME", "Team", "GAME_DATE", "MATCHUP", "WL",
    "MIN", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA", "FT_PCT",
    "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "PF", "PTS", "PLUS_MINUS",
    "VIDEO_AVAILABLE"
]

# Importar tipos para criar schema vazio
from pyspark.sql.types import StructType, StructField, StringType # Adicione mais tipos conforme necessário

if rdd_sem_header.isEmpty():
    print(f"Alerta CRÍTICO: RDD para df_box_scores ficou vazio após tentativa de remover o cabeçalho. Verifique o arquivo CSV em {path_box_scores} e o delimitador.")
    # Criar DataFrame vazio com o schema esperado para evitar falhas posteriores, mas o job não produzirá dados úteis de box_scores.
    empty_schema_fields = [StructField(name, StringType(), True) for name in column_names]
    df_box_scores = spark.createDataFrame([], StructType(empty_schema_fields))
    print("Criado DataFrame df_box_scores VAZIO com schema esperado.")
else:
    df_box_scores = spark.createDataFrame(rdd_sem_header, schema_com_nomes_genericos)
    print("Schema de df_box_scores após remover a linha de cabeçalho (ainda com _c0, _c1...):")
    df_box_scores.printSchema()
    df_box_scores.show(3, truncate=False)

    # Renomear as colunas _c0, _c1, ... para os nomes definidos em column_names
    if len(df_box_scores.columns) == len(column_names):
        df_box_scores = df_box_scores.toDF(*column_names)
        print("Schema de df_box_scores APÓS renomear as colunas:")
        df_box_scores.printSchema()
        df_box_scores.show(3, truncate=False)
    else:
        print(f"Alerta CRÍTICO: O número de colunas lidas ({len(df_box_scores.columns)}) não corresponde ao número de nomes de coluna definidos ({len(column_names)}).")
        print("As colunas lidas são:", df_box_scores.columns)
        print("Os nomes definidos são:", column_names)
        print("O processamento de df_box_scores provavelmente falhará ou produzirá resultados incorretos.")
        # Para segurança, criando um df_box_scores vazio com o schema correto se houver essa discrepância.
        empty_schema_fields = [StructField(name, StringType(), True) for name in column_names]
        df_box_scores = spark.createDataFrame([], StructType(empty_schema_fields))
        print("Criado DataFrame df_box_scores VAZIO devido à incompatibilidade de contagem de colunas.")



# Carregando o arquivo de salários, pois ele será usado em um join opcional de salários
print(f"Carregando dados de salários de: {path_salaries_upload}")
dynamic_frame_salaries = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [path_salaries_upload]},
    format="csv",
    format_options={"withHeader": True, "inferSchema": "true", "separator": ","}
)
df_salaries = dynamic_frame_salaries.toDF()
print("Schema df_salaries:")
df_salaries.printSchema()
df_salaries.show(3, truncate=False)


print("Dados brutos necessários foram carregados.")

# --- 2. Limpeza de Dados (em df_box_scores) ---
print("Limpando df_box_scores (conversão de tipos)...")

# Lista de colunas de estatísticas que devem ser numéricas (double para cálculos)
numeric_cols_stats = ["MIN", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA", "FT_PCT",
                      "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "PF", "PTS", "PLUS_MINUS"]

for stat_col in numeric_cols_stats:
    if stat_col in df_box_scores.columns:
        # Tentar converter para double. Se a coluna não puder ser convertida
        # o valor se tornará null. Isso é geralmente o comportamento desejado.
        df_box_scores = df_box_scores.withColumn(stat_col, col(stat_col).cast("double"))
    else:
        print(f"Aviso na Limpeza: Coluna {stat_col} não encontrada em df_box_scores para conversão numérica.")

# Converter GAME_DATE
if "GAME_DATE" in df_box_scores.columns:
    df_box_scores = df_box_scores.withColumn("GAME_DATE_CONV", to_date(col("GAME_DATE"))) # Ajustando o formato se necessário
else:
    print("Aviso na Limpeza: Coluna GAME_DATE não encontrada para conversão de data.")

# Recalcular percentuais caso os originais não sejam confiáveis ou para garantir tipo double
if all(c in df_box_scores.columns for c in ["FGA", "FGM"]):
    df_box_scores = df_box_scores.withColumn(
        "FG_PCT_CALC",
        when(col("FGA") > 0, col("FGM") / col("FGA")).otherwise(lit(0.0))
    )



print("Schema df_box_scores após limpeza e conversão de tipos:")
df_box_scores.printSchema()
df_box_scores.show(5, truncate=False)


# --- 3. Agregar Estatísticas por Jogador e por Temporada ---
print("Agregando estatísticas por jogador e temporada...")

# Colunas para somar - verificando se todas existem em df_box_scores
sum_cols_mapping = {
    "MIN": "TotalMinutes", "PTS": "TotalPoints", "REB": "TotalRebounds", "AST": "TotalAssists",
    "STL": "TotalSteals", "BLK": "TotalBlocks", "TOV": "TotalTurnovers", "FGM": "TotalFGM",
    "FGA": "TotalFGA", "FG3M": "TotalFG3M", "FG3A": "TotalFG3A", "FTM": "TotalFTM",
    "FTA": "TotalFTA", "PF": "TotalPersonalFouls"
}
agg_expressions = [countDistinct("Game_ID").alias("GamesPlayed")]
for source_col, alias_col in sum_cols_mapping.items():
    if source_col in df_box_scores.columns:
        agg_expressions.append(sum(col(source_col)).alias(alias_col))
    else:
        print(f"Aviso: Coluna {source_col} não encontrada para agregação SUM.")

# Verifica se PLAYER_NAME e Season existem para o groupBy
if "PLAYER_NAME" in df_box_scores.columns and "Season" in df_box_scores.columns and "Game_ID" in df_box_scores.columns:
    player_season_stats = df_box_scores.groupBy("PLAYER_NAME", "Season").agg(*agg_expressions)

    print("Alerta: Colunas chave PLAYER_NAME, Season ou Game_ID não encontradas em df_box_scores para o groupBy principal. Tentando criar um DataFrame vazio para player_season_stats.")

    # Importando os tipos necessários do PySpark SQL
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

    # Definindo a função auxiliar col_ primeiro:
    def col_(name, type_str): # Renomeei o parâmetro para type_str para evitar conflito com a função type() do Python
        if type_str == "string": return StructField(name, StringType(), True)
        if type_str == "long": return StructField(name, LongType(), True)
        if type_str == "double": return StructField(name, DoubleType(), True)
        return StructField(name, StringType(), True) # Default para string

    # Usar col_ para definir os campos do esquema esperado
    # (sum_cols_mapping deve estar definido antes deste bloco else, no escopo anterior)
    expected_schema_fields = [
        col_("PLAYER_NAME", "string"),
        col_("Season", "string"),
        col_("GamesPlayed", "long")
    ]
    # Adiciona os campos para as colunas de soma agregadas
    for source_col_original, alias_name in sum_cols_mapping.items(): # sum_cols_mapping da etapa de agregação
        expected_schema_fields.append(col_(alias_name, "double")) # Assumindo que todas as somas são double

    final_expected_schema = StructType(expected_schema_fields)

    player_season_stats = spark.createDataFrame([], schema=final_expected_schema)
    print("Criado DataFrame player_season_stats vazio devido à falta de colunas chave no input df_box_scores.")



# --- 4. Calcular Médias e Métricas de Eficiência (por jogador, por temporada) ---
print("Calculando médias e métricas de eficiência...")
# Verificando se as colunas 'GamesPlayed', 'TotalMinutes', etc., existem antes de usá-las.
# Foram criadas no passo anterior (player_season_stats).

# Lista de métricas por jogo (TotalStat / GamesPlayed)
# (Stat, PerGameAlias)
per_game_metrics = [
    ("TotalMinutes", "MPG"), ("TotalPoints", "PPG"), ("TotalRebounds", "RPG"),
    ("TotalAssists", "APG"), ("TotalSteals", "SPG"), ("TotalBlocks", "BPG"),
    ("TotalTurnovers", "TPG")
]
player_season_analysis = player_season_stats # Começa com o DF agregado
for total_col, per_game_alias in per_game_metrics:
    if total_col in player_season_analysis.columns and "GamesPlayed" in player_season_analysis.columns:
        player_season_analysis = player_season_analysis.withColumn(
            per_game_alias,
            when(col("GamesPlayed") > 0, col(total_col) / col("GamesPlayed")).otherwise(lit(0.0))
        )
    else:
         print(f"Aviso: Coluna {total_col} ou GamesPlayed não encontrada para calcular {per_game_alias}.")

# Percentuais
if "TotalFGA" in player_season_analysis.columns and "TotalFGM" in player_season_analysis.columns:
    player_season_analysis = player_season_analysis.withColumn(
        "FG_PCT", when(col("TotalFGA") > 0, col("TotalFGM") / col("TotalFGA")).otherwise(lit(0.0))
    )
if "TotalFG3A" in player_season_analysis.columns and "TotalFG3M" in player_season_analysis.columns:
    player_season_analysis = player_season_analysis.withColumn(
        "FG3_PCT", when(col("TotalFG3A") > 0, col("TotalFG3M") / col("TotalFG3A")).otherwise(lit(0.0))
    )
if "TotalFTA" in player_season_analysis.columns and "TotalFTM" in player_season_analysis.columns:
    player_season_analysis = player_season_analysis.withColumn(
        "FT_PCT", when(col("TotalFTA") > 0, col("TotalFTM") / col("TotalFTA")).otherwise(lit(0.0))
    )

# True Shooting Percentage (TS_PCT)
if "TotalPoints" in player_season_analysis.columns and \
   "TotalFGA" in player_season_analysis.columns and \
   "TotalFTA" in player_season_analysis.columns:
    player_season_analysis = player_season_analysis.withColumn(
        "TS_PCT",
        when((col("TotalFGA") + 0.44 * col("TotalFTA")) > 0,
             col("TotalPoints") / (2 * (col("TotalFGA") + 0.44 * col("TotalFTA")))
        ).otherwise(lit(0.0))
    )
else:
    print(f"Aviso: Colunas necessárias para TS_PCT não encontradas.")


print("Schema do player_season_analysis:")
player_season_analysis.printSchema()
print("Amostra do player_season_analysis (10 primeiras linhas):")
player_season_analysis.show(10, truncate=False)


player_season_final_analysis = player_season_analysis # Atribuição direta

print("Schema final antes de salvar:")
player_season_final_analysis.printSchema()
print("Amostra final antes de salvar (10 primeiras linhas):")
player_season_final_analysis.show(10, truncate=False)


# --- 6. Salvar o Resultado Processado ---
s3_output_path_tema1 = "s3://meu-projeto-nba-dados-pchb/dados-processados/desempenho_jogadores_por_temporada/"

print(f"Salvando resultado do Tema 1 em formato Parquet em: {s3_output_path_tema1}")

# Usando o método de escrita do DataFrame Spark diretamente
# O modo "overwrite" substituirá os dados se a pasta já existir
player_season_final_analysis.write.mode("overwrite").format("parquet").save(s3_output_path_tema1)

print("Processamento do Tema 1 (Todas as Temporadas) concluído e dados salvos.")

# ==================================================================================
# FIM DO CÓDIGO DE PROCESSAMENTO DE DADOS PARA O TEMA 1
# ==================================================================================

job.commit()
