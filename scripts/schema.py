from pyspark.sql.types import(
    StructType,
    StructField,
    StringType
)

# external schema specification 
NEWS_ARTICLE_SCHEMA = StructType([
    StructField(
        "source",
        StructType([
            StructField("id",StringType(), True),
            StructField("name",StringType(), True)
        ]),
        True,
    ),
    StructField("author",StringType(),True),
    StructField("title",StringType(),True),
    StructField("description",StringType(),True),
    StructField("url",StringType(),True),
    StructField("urlToImage",StringType(),True),
    StructField("publishedAt",StringType(),True),
    StructField("content",StringType(),True),


])