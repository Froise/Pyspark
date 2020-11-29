from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, FloatType,StructType, StructField, ArrayType, StringType
from pyspark.sql.dataframe import DataFrame as DFrame

NOME_JOB = 'LerDadosGoverno'

def obtem_dados_csv(path, delimiter=";", header="false", inferSchema="false"):
    df = spark.read.format("com.databricks.spark.csv")\
        .option("header", header)\
        .option("delimiter",delimiter)\
        .option("inferSchema",inferSchema)\
        .load(path)
    return df

def obtem_dados_parquet():
    pass


if __name__ == "__main__":

    configuracao = (SparkConf()
    .set("spark.driver.maxResultSize", "2g"))

    spark = (SparkSession
        .builder
        .config(conf=configuracao)
        .appName(NOME_JOB)
        .enableHiveSupport()
        .getOrCreate())

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    to_value = lambda v: float(v.replace(",","."))
    udf_to_value = F.udf(to_value, FloatType())

    caminho_csv = "C:/Users/francoise.moreira/OneDrive - JM Confitec Sistemas de Computação LTDA/ESTUDO/SPARK_JUPITER_PANDA_PYTHON/lerParquet e tratar dados/dadosCSV/2021_Pagamento.csv"
    caminho_parquet = "C:/Users/francoise.moreira/OneDrive - JM Confitec Sistemas de Computação LTDA/ESTUDO/SPARK_JUPITER_PANDA_PYTHON/lerParquet e tratar dados/dadosParquet/"

    df = obtem_dados_csv(caminho_csv,";","true","true")

    # NOTE transformar os dados recebidos em string
    DadosGoverno = (df
    .transform(lambda df: df.withColumn("id_processo", F.col("Identificador do processo de viagem").cast(StringType())))
    .transform(lambda df: df.withColumn("proposta", F.col("N�mero da Proposta (PCDP)").cast(StringType())))
    .transform(lambda df: df.withColumn("cod_orgao_sup", F.col("C�digo do �rg�o superior").cast(StringType())))
    .transform(lambda df: df.withColumn("nome_orgao_sup", F.col("Nome do �rg�o superior").cast(StringType())))
    .transform(lambda df: df.withColumn("cod_orgao_pg", F.col("Codigo do �rg�o pagador").cast(StringType())))
    .transform(lambda df: df.withColumn("nome_orgao_pg", F.col("Nome do �rgao pagador").cast(StringType())))
    .transform(lambda df: df.withColumn("cod_und_pg", F.col("C�digo da unidade gestora pagadora").cast(StringType())))
    .transform(lambda df: df.withColumn("nome_und_pg", F.col("Nome da unidade gestora pagadora").cast(StringType())))
    .transform(lambda df: df.withColumn("tp_pgto", F.col("Tipo de pagamento").cast(StringType())))
    )
    df = DadosGoverno.withColumn("valor", udf_to_value(df["Valor"]))
    DadosGoverno = df.select('id_processo','proposta','cod_orgao_sup','nome_orgao_sup','cod_orgao_pg','nome_orgao_pg','cod_und_pg','nome_und_pg','tp_pgto','valor')
    print("quantidade de dados: ",DadosGoverno.count())
    print("media dos gastos: ",DadosGoverno.agg(F.avg("valor")).show())
    print("valor minimo gasto: ",DadosGoverno.agg(F.min("valor")).show())
    print("valor máximo gasto: ",DadosGoverno.agg(F.max("valor")).show())
