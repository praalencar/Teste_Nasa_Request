# Teste_Nasa_Request
Teste Apache Spark: Desafio da empresa Semantix utilizando Spark.

**Qual o objetivo do comando cache em Spark?**

**Resposta:** O objetivo do método .cache() em comandos que gerem como resultado o formato RDD (Resillient Distributed Datasets) permite que os dados sejam armazenados em memória, facilitando computações de dados sobre dados oriundos deste formato RDD.

**O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?**

**Resposta:** Ambos são utilizados para processamento de dados distribuídos em ambiente Hadoop. Porém o processamento realizado pelo Spark é realizado utilizando memória, enquanto o MapReduce precisa de ler e escrever seus resultados em disco. Pesquisas destacam que uma mesma consulta realizada em Spark chega a ser processada 100 vezes mais rápida do que o MapReduce, devido a isto é que ultimamente a quantidade de processamentos utilizando Spark é exponencialmente maior do que o MapReduce.

**Qual é a função do SparkContext ?**

**Resposta:** O SparkContext é a classe do Spark (spark em Scala ou pyspark em Python) que faz todos os tratamentos e manipulações de dados com a engine do ambiente Spark instalado na máquina. Permite que seja importado dados de diversos tipos de arquivos (Text, CSV, XML, Parquet, ORC, bancos de dados transacionais ou NoSQL), criar RDDs, DataFrames, variáveis, etc.

**Explique com suas palavras o que é Resilient Distributed Datasets (RDD).**

**Resposta:** Um RDD é uma estrutura de dados utilizada pelo Spark, como uma coleção de dados no Python, que é processada de forma distribuída em múltiplas Java Virtual Machines (também conhecido como nós de um cluster).
GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê? Resposta: Ambos groupByKey e reduceByKey irão gerar mesmos resultados. Porém em grandes datasets, já sabendo que o Spark processa seus dados em múltiplos nós, que podem estar em máquinas diferentes, dentro de uma mesma rede. O método reduceByKey faz com que o Spark consiga combinar os dados do resultado através de uma chave comum destes nós (partição) antes mesmo de juntar os dados no dataset de output.

**Explique o que o código Scala abaixo faz.**

val textFile = sc.textFile(“hdfs://…”) val counts = textFile.flatMap(line => line.split(" ")) .map(word => (word, 1)) .reduceByKey(_ + _) counts.saveAsTextFile(“hdfs://…”)
**Resposta:** a instância sc. é referente a importação da biblioteca SparkContext que criará uma sessão para poder trabalhar com os dados de forma distribuída. Na primeira linha, é definida a variável textFile que receberá o conteúdo de um arquivo texto disponível em algum diretório HDFS (Hadoop Distributed FyleSystem) da máquina. Na segunda linha é criada a variável que percorre cada linha desta variável textFile e separa as palavras entre espaços em branco, fazendo um mapeando de cada palavra que for encontrada com o número 1, e reduzindo-las pela somatória de valores por palavras iguais. Após concluir o processamento, é exportado o resultado da variável counts para um arquivo Texto a ser definido a localização e o nome do arquivo que será salvo dentro do ambiente HDFS.

**HTTP requests to the NASA Kennedy Space Center WWW server**
**Questões Responda as seguintes questões devem ser desenvolvidas em Spark utilizando a sua linguagem de preferência.**

**1 - Número de hosts únicos.**
**Resposta:** O número de hosts únicos é de 137979 registros df_merged_distinct.count() Out[89]: 137979 file output: “\results\Questao_01_distinct_hosts.csv”

**2 - O total de erros 404.**

**Resposta:** O total de erros 404 foram encontrados em 20871 registros
df2_qtde_error_404.show() ±--------+ |sum(qtde)| ±--------+ | 20871| ±--------+

file output: “\results\Questao_02_total_error_404.csv”Files

**3 - Os 5 URLs que mais causaram erro 404.**
**Resposta:** Segue abaixo a lista dos 5 maiores hosts com erros 404 df3_merged.show() ±–±—+ | host|qtde| ±------±—+ |hoohoo.ncsa.uiuc.edu| 251| |jbiagioni.npt.nuw…| 131| |piweba3y.prodigy.com| 109| |piweba1y.prodigy.com| 92| |phaelon.ksc.nasa.gov| 64| ±----±—+

#### file output: “\results\Questao_03_top_5_hosts_error_404.csv”

**4 - Quantidade de erros 404 por dia.**
**Resposta:** Quantidade de erros 404 por dia está disponível no arquivo .CSV na pasta Results, e uma amostra dos dados está listada abaixo.

df4_merged.show() ±–±--±-+ |dia|mes|qtde_erros_404| ±–±--±–+ | 01|Jul| 316| | 02|Jul| 291| | 03|Jul| 470| | 04|Jul| 359| | 05|Jul| 497| | 06|Jul| 640| | 07|Jul| 569| | 08|Jul| 302| | 09|Jul| 348| | 10|Jul| 398| | 11|Jul| 471| | 12|Jul| 470| | 13|Jul| 531| | 14|Jul| 411| | 15|Jul| 254| | 16|Jul| 257| | 17|Jul| 406| | 18|Jul| 465| | 19|Jul| 638| | 20|Jul| 428| ±–±--±—+ only showing top 20 rows

**5 - O total de bytes retornados.**

**Resposta:5.  O total retornados de requisições enviadas em Julho e Agosto de 1995 foi de 6.5524319796E10 bytes.
df5_merged.show() ±-+ |total_bytes_retornados| ±–+ | 6.5524319796E10| ±-----+

**file output: “\results\Questao_05_total_bytes_retornados.csv”**
**file output: “\results\Questao_04_qtde_error_404_dia.csv”**
