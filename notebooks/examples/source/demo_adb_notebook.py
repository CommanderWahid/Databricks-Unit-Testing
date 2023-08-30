# Databricks notebook source

data = spark.sql('SELECT * FROM samples.tpch.region where r_regionkey = 0')
data.show()