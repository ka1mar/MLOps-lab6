from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, count, when, isnan
from pyspark.sql.types import NumericType
import numpy as np
import os
import logging
import argparse

class ClickHouseConnector:
    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)

    def read(self, table):
        return self.spark.read \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", os.getenv('CLICKHOUSE_URL')) \
            .option("user", os.getenv('CLICKHOUSE_USER')) \
            .option("password", os.getenv('CLICKHOUSE_PASSWORD')) \
            .option("dbtable", table) \
            .load()

    def write(self, df, table):
        df.write \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", os.getenv('CLICKHOUSE_URL')) \
            .option("user", os.getenv('CLICKHOUSE_USER')) \
            .option("password", os.getenv('CLICKHOUSE_PASSWORD')) \
            .option("dbtable", table) \
            .option("truncate", "false") \
            .mode("append") \
            .save()
        self.logger.info(f"Данные успешно записаны в таблицу: {table}")

class AutoClusteringPipeline:  
    def __init__(self, input_table, output_table, output_path, max_missing=0.3, min_unique=0.3):
        self.input_table = input_table
        self.output_table = output_table
        self.output_path = output_path
        self.max_missing = max_missing
        self.min_unique = min_unique
        self.spark = self._init_spark()
        self.db_connector = ClickHouseConnector(self.spark)
        self.numeric_columns = []
        self.logger = logging.getLogger(self.__class__.__name__)

    def _init_spark(self):
        return SparkSession.builder \
            .config("spark.executor.cores", os.getenv('SPARK_EXECUTOR_CORES'))  \
            .config("spark.driver.memory", os.getenv('SPARK_DRIVER_MEMORY')) \
            .config("spark.executor.memory", os.getenv('SPARK_EXECUTOR_MEMORY')) \
            .config("spark.default.parallelism", os.getenv('SPARK_DEFAULT_PARALLELISM')) \
            .config("spark.sql.shuffle.partitions", os.getenv('SPARK_SQL_SHUFFLE_PARTITIONS')) \
            .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.4.6,com.clickhouse:clickhouse-http-client:0.4.6,org.apache.httpcomponents.client5:httpclient5:5.2.1") \
            .appName("AutoClustering") \
            .getOrCreate()

    def _find_numeric_columns(self, df):
        numeric_cols = [
            field.name for field in df.schema.fields
            if isinstance(field.dataType, NumericType)
        ]
        self.logger.info(f"Найдены числовые колонки: {numeric_cols}")
        return numeric_cols

    def _filter_columns(self, df, numeric_cols):
        total_count = df.count()
        filtered_cols = []
    
        for col_name in numeric_cols:
            # Проверка на пропущенные значения
            missing = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
            missing_ratio = missing / total_count

            # Проверка на уникальные значения
            unique_count = df.select(col_name).distinct().count()
            unique_ratio = unique_count / total_count
        
            if missing_ratio < self.max_missing and unique_ratio >= self.min_unique:
                filtered_cols.append(col_name)
                self.logger.info(f"Колонка {col_name} прошла фильтрацию: пропуски={missing_ratio:.1%}, уникальные={unique_ratio:.1%}")
            else:
                self.logger.warning(f"Исключена колонка {col_name}: пропуски={missing_ratio:.1%}, уникальные={unique_ratio:.1%}")
    
        return filtered_cols

    def load_and_preprocess(self):
        # Загрузка данных через коннектор
        df = self.db_connector.read(self.input_table)
    
        # Определение числовых колонок
        numeric_cols = self._find_numeric_columns(df)
        if not numeric_cols:
            raise ValueError("В данных отсутствуют числовые колонки для анализа")
    
        # Фильтрация колонок
        self.numeric_columns = self._filter_columns(df, numeric_cols)
        if not self.numeric_columns:
            raise ValueError("Нет колонок, удовлетворяющих критериям качества данных")

        # Обработка пропусков
        imputer = Imputer(
            inputCols=self.numeric_columns,
            outputCols=self.numeric_columns
        ).setStrategy("mean")
    
        return imputer.fit(df).transform(df).select(self.numeric_columns)

    def feature_engineering(self, df):
        assembler = VectorAssembler(
            inputCols=self.numeric_columns,
            outputCol="raw_features"
        )
        assembled_df = assembler.transform(df)

        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        scaled_df = scaler.fit(assembled_df).transform(assembled_df)
        return scaled_df

    def train(self, df, k=11):
        return KMeans(featuresCol="features", k=k, seed=42).fit(df)

    def save_results(self, model, df):
        results = model.transform(df).select("prediction", *self.numeric_columns)
        self.db_connector.write(results.limit(20), self.output_table)
        model.write().overwrite().save(f"{self.output_path}/model")

    def run(self):
        try:
            df = self.load_and_preprocess()
            processed_df = self.feature_engineering(df)
            model = self.train(processed_df)
            self.save_results(model, processed_df)
            self.logger.info("Кластеризация успешно завершена!")
        except Exception as e:
            self.logger.error(f"Ошибка: {str(e)}", exc_info=True)
            raise
        finally:
            self.spark.stop()
            self.logger.info("Spark сессия остановлена")

class AutoClusteringConfig:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Автоматическая кластеризация")
        self._setup_arguments()
    
    def _setup_arguments(self):
        self.parser.add_argument("--input_table", required=True,
                                help="Имя таблицы в ClickHouse для чтения данных")
        self.parser.add_argument("--output_table", required=True,
                                help="Имя таблицы в ClickHouse для сохранения результатов")
        self.parser.add_argument("-o", "--output", required=True,
                                help="Директория для сохранения весов модели")
        self.parser.add_argument("--max_missing", type=float, default=0.3,
                                help="Максимальная доля пропущенных значений (по умолчанию 0.3)")
        self.parser.add_argument("--min_unique", type=float, default=0.3,
                                help="Минимальная доля уникальных значений (по умолчанию 0.3)")
        self.parser.add_argument("-v", "--verbose", action="store_true",
                                help="Включить детальное логирование")

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    config = AutoClusteringConfig()
    args = config.parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    pipeline = AutoClusteringPipeline(
        input_table=args.input_table,
        output_table=args.output_table,
        output_path=args.output,
        max_missing=args.max_missing,
        min_unique=args.min_unique
    )
    pipeline.run()

if __name__ == "__main__":
    main()
