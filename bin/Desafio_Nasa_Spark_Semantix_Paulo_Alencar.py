# -*- coding: utf-8 -*-
"""
Criado em 18/12/2019
Autor: Paulo Alencar
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext

#Criando SparkContext e SQLContext
sc = SparkContext("local", "nasa app")
sqlContext = SQLContext(sc)

#Carregando arquivos em PySpark Dataframe
df_jul95 = sqlContext.read.csv(r'D:\\Pessoais\\Semantix\\datasets\\access_log_Jul95',sep=' ',timestampFormat='[dd-MMM-yyyy:hh:mm:ss -0400]').cache()

df_aug95 = sqlContext.read.csv(r'D:\\Pessoais\\Semantix\\datasets\\access_log_Aug95',sep=' ',timestampFormat='[dd-MMM-yyyy:hh:mm:ss -0400]').cache()

#Confirmando o tipo de estrutura é PySpark Dataframe
#type(df_jul95)
#type(df_aug95)

#Mostrando os 20 primeiros registros de cada PySparkDataframe
#df_jul95.show()
#df_aug95.show()

#Mostrando a estrutura de colunas que foi criada
#df_jul95.printSchema()
#df_aug95.printSchema()

#Criando tabelas para poder escrever comandos Spark SQL nelas
sqlContext.registerDataFrameAsTable(df_jul95, "access_log_Jul95")

sqlContext.registerDataFrameAsTable(df_aug95, "access_log_Aug95")

##===================================================================================
#Respondendo a Questao 01: 1. Número de hosts únicos.
#===================================================================================

#selecionando os conteudos das duas tabelas
df_hosts_jul95 = sqlContext.sql("""SELECT _c0 AS host \
                                   FROM access_log_Jul95""")

df_hosts_aug95 = sqlContext.sql("""SELECT _c0 AS host \
                                   FROM access_log_Aug95""")


#usando o método unionALL do Spark DataFrame para unir os dois conteudos das tabelas
df_merged = df_hosts_jul95.unionAll(df_hosts_aug95)

#contagem individuais de cada tabela
#df_hosts_jul95.count()
#df_hosts_aug95.count()

#contagem da tabela feita o Union
#df_merged.count()

#Usando o metodo distinct() do Spark DataFrame para remover duplicados
df_merged_distinct = df_merged.distinct()

#contando o resultado para confirmar que a remoção de duplicadas foi feita com sucesso
#df_merged_distinct.count()

#convertendo para Pandas Dataframe para permitir export em .csv de forma mais fácil
df_distinct_hosts = df_merged_distinct.toPandas()

#exportando o Pandas DataFrame para arquivo .csv
df_distinct_hosts.to_csv("D:\\Pessoais\\Semantix\\results\\Questao_01_distinct_hosts.csv",index=False)

#===================================================================================
#Respondendo a Questao 02. O total de erros 404.
#===================================================================================

#selecionando os conteudos das duas tabelas
df2_merged = sqlContext.sql("""SELECT count(*) as qtde \
                                        FROM access_log_Aug95 \
                                       WHERE _c6 = '404' \
                                     UNION \
                                      SELECT count(*) as qtde \
                                        FROM access_log_Jul95 \
                                       WHERE _c6 = '404' \
                            """)

#importando a função sql de spark
from pyspark.sql import functions as F

#agregando o total por quantidade, pela funcao soma
df2_qtde_error_404 = df2_merged.agg(F.sum("qtde"))

#convertento Spark DataFrame para Pandas DataFrame
df_total_error_404 = df2_qtde_error_404.toPandas()

#exportando o conteúdo retornado da query em .csv
df_total_error_404.to_csv("D:\\Pessoais\\Semantix\\results\\Questao_02_total_error_404.csv",index=False)

#===================================================================================
#Respondendo a Questao 03. Os 5 URLs que mais causaram erro 404.
#===================================================================================

#selecionando os conteudos das duas tabelas
df3_merged = sqlContext.sql("""SELECT * \
                                FROM( \
                                    SELECT _c0 as host, \
                                            count(*) as qtde \
                                         FROM access_log_Aug95 \
                                        WHERE _c6 = '404' \
                                        GROUP BY _c0 \
                                             UNION \
                                        SELECT _c0 as host, \
                                                count(*) as qtde \
                                          FROM access_log_Jul95 \
                                         WHERE _c6 = '404'
                                         GROUP BY _c0 \
                                    ) ORDER BY qtde DESC \
                               limit 5 \
                                 """)

#convertento Spark DataFrame para Pandas DataFrame
df_top_5_hosts_error_404 = df3_merged.toPandas()

#exportando o conteúdo retornado da query em .csv
df_top_5_hosts_error_404.to_csv("D:\\Pessoais\\Semantix\\results\\Questao_03_top_5_hosts_error_404.csv",index=False)

#===================================================================================
#Respondendo a Questao 04. Quantidade de erros 404 por dia.
#===================================================================================

#selecionando os conteudos das duas tabelas
df4_merged = sqlContext.sql("""SELECT   dia, mes, \
                                        sum(qtde_erros_404) as qtde_erros_404 \
                                FROM( \
                                        SELECT  substr(_c3,2,2) as dia, \
                                                substr(_c3,5,3) as mes, \
                                                count(*) as qtde_erros_404 \
                                         FROM access_log_Aug95 \
                                         WHERE _c6 = '404' \
                                         GROUP BY substr(_c3,2,2), \
                                                  substr(_c3,5,3) \
                                             UNION \
                                        SELECT substr(_c3,2,2) as dia, \
                                                substr(_c3,5,3) as mes, \
                                                count(*) as qtde_erros_404 \
                                          FROM access_log_Jul95 \
                                          WHERE _c6 = '404' \
                                         GROUP BY substr(_c3,2,2), \
                                                  substr(_c3,5,3) \
                                 ) \
                             GROUP BY dia, mes \
                             ORDER BY mes DESC, dia ASC\
                                 """)

#convertento Spark DataFrame para Pandas DataFrame
df_qtde_error_404_dia = df4_merged.toPandas()

#exportando o conteúdo retornado da query em .csv
df_qtde_error_404_dia.to_csv("D:\\Pessoais\\Semantix\\results\\Questao_04_qtde_error_404_dia.csv",index=False)

#===================================================================================
#Respondendo a Questao 05. O total de bytes retornados.
#===================================================================================
df5_merged = sqlContext.sql("""SELECT sum(total_bytes) as total_bytes_retornados \
                                FROM( \
                                        SELECT sum(_c7) as total_bytes \
                                         FROM access_log_Aug95 \
                                             UNION \
                                        SELECT sum(_c7) as total_bytes \
                                          FROM access_log_Jul95 \
                                 )
                                 """)

#convertento Spark DataFrame para Pandas DataFrame
df_total_bytes_retornados = df5_merged.toPandas()

#exportando o conteúdo retornado da query em .csv
df_total_bytes_retornados.to_csv("D:\\Pessoais\\Semantix\\results\\Questao_05_total_bytes_retornados.csv",index=False)                      

#===================================================================================

#fechando o SparkContext
sc.stop()

"""
#Fim do Script