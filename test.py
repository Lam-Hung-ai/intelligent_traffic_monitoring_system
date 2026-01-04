import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import ArrayType, FloatType
import torch
import io

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("YOLOv11-Distributed-Inference")   \
    .config()