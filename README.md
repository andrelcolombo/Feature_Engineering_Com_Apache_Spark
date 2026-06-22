# 🗽 NYC Airbnb Data Pipeline & Feature Engineering

[![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![Parquet](https://img.shields.io/badge/Storage-Parquet-002538?style=for-the-badge)](https://parquet.apache.org/)


<p align="center">
  <img src="https://spark.apache.org/images/spark-logo-trademark.png" alt="Apache Spark Logo" width="200" style="padding: 10px;"/>
</p>

Este repositório apresenta um pipeline especializado em **Engenharia de Atributos (Feature Engineering) de Alta Performance** desenvolvido em **PySpark**, utilizando a base de dados públicos do Airbnb de Nova York (NYC). O core do projeto está no design e na construção de atributos preditivos robustos a partir de dados brutos textuais, numéricos e temporais.

---

## 🏗️ Arquitetura do Pipeline

O pipeline segue os princípios de arquitetura de medalhão, focando na transformação e enriquecimento dos dados até a sua persistência em uma camada otimizada de consumo.

1. **Ingestão & Limpeza Inicial**: Identificação e tratamento de registros nulos e normalização de tipos de dados.
2. **Engenharia de Atributos Temporais**: Extração de sazonalidade e comportamento de reviews no tempo.
3. **Engenharia de Atributos Numéricos**: Categorização de variáveis de negócio e transformação logarítmica de escala.
4. **Análise Avançada com Window Functions**: Computação de métricas comparativas regionais sem perda de granularidade.
5. **Persistência Otimizada**: Armazenamento final no formato colunar Parquet.

---

## 🛠️ Detalhes das Implementações Técnicas

### 1. Tratamento de Sazonalidade e Nulos
As datas de última avaliação (`last_review`) foram decompostas para capturar o comportamento dos usuários. Campos nulos foram mapeados utilizando substituições seguras (`coalesce` e `lit`) para garantir a integridade estatística da base:
* Extração de Ano, Mês e Dia da semana.
* Criação de flag booleana para reviews realizados em finais de semana (`is_weekend_review`).
* Inputação de `0.0` para registros sem histórico em `reviews_per_month`.

### 2. Transformação Logarítmica (`log_price`)
Dados de preço no mercado imobiliário são tipicamente **assimétricos à direita** (*right-skewed*), com longas caudas causadas por imóveis de luxo (*outliers*). 
* Aplicou-se a função $\log(x + 1)$ para comprimir a escala dos preços e aproximá-los de uma distribuição normal (Gaussiana), melhorando a convergência de modelos preditivos.
* O uso de $x + 1$ atua como um *guardrail* matemático, evitando que eventuais preços zerados resultem em $-\infty$.

### 3. Agrupamento de Negócio (*Binning*)
O tempo mínimo de reserva foi categorizado em três faixas de mercado estratégicas utilizando estruturas condicionais eficientes do Spark (`F.when`):
* **Curta**: 1 a 2 noites
* **Média**: 3 a 7 noites
* **Longa**: Mais de 7 noites

### 4. Métricas Regionais via Window Functions
Para evitar a perda de granularidade que um `groupBy` tradicional causaria, foram utilizadas **Window Functions** particionadas por região (`neighbourhood_group`).
* Computou-se o preço médio da região geográfica.
* Calculou-se a razão de preço (`price_ratio_neighbourhood`), determinando o quão caro ou barato um imóvel é em relação à sua vizinhança direta linha por linha, sem a necessidade de joins custosos.

---

## 💾 Armazenamento e Persistência

A base final processada é persistida utilizando o modo de sobrescrita (`overwrite`) no formato **Apache Parquet**:

```python
df_final.write.mode("overwrite").parquet("/opt/spark/resources/NYC/base/airbnb_feature_store.parquet")
```
---

<p align="center">
  <b>Desenvolvido por André Luiz Colombo</b>
</p>
