# Databricks notebook source
# Imports
from pyspark.sql.functions import (
    input_file_name,
    split,
    col,
    coalesce,
    concat_ws,
    size,
    when,
    to_date,
)
from pyspark.sql.types import IntegerType
from io_utils.df_names import DataFrameNames
from io_utils.df_factory import DataFrameHandlerFactory, Extension
from io_utils.paths import Paths
from schemas import (
    PlayersSchema,
    PlayersCreatedSchema,
    PlayersStatisticsSchema,
    MatchesSchema,
    MatchesCreatedSchema,
)

# fmt: off
# COMMAND ----------

bronze_files = dbutils.fs.ls(Paths.BRONZE_PATH.value)
matches_csv_paths = [file.path for file in bronze_files if "matches" in file.name]

# Instantiate spark dfs from paths
csv_handler = DataFrameHandlerFactory.get_handler(Extension.CSV)
df_players = csv_handler.load(spark=spark, path=Paths.BRONZE_PATH, name=DataFrameNames.ATP_PLAYERS)
df_matches = csv_handler.load(spark=spark, path=matches_csv_paths, name=DataFrameNames.DF_MATCHES)

# COMMAND ----------


# To the matches dataframe, add a column with the original path, in order to create a year col as int (leave path just in case)
df_matches = df_matches.withColumn(MatchesCreatedSchema.PATH, input_file_name())
df_matches = df_matches.withColumn(MatchesCreatedSchema.YEAR, split(split(df_matches[MatchesCreatedSchema.PATH], ".csv").getItem(0), "_").getItem(2).cast(IntegerType()))

# COMMAND ----------

# Dont even need this col
df_players = df_players.drop(PlayersSchema.WIKIDATA_ID)

# There are some players who dont have first or last name, and that they are not found in the df_matches by their IDs, so just remove them
df_players = df_players.filter(
    ((col(PlayersSchema.NAME_FIRST).isNotNull()) & (col(PlayersSchema.NAME_LAST).isNotNull()))
)

# Convert dob str -> int -> date 
df_players = df_players.withColumn(
    PlayersSchema.DOB,
    to_date(col(PlayersSchema.DOB).cast("int"), "yyyyMMdd")
)

# Rename ioc to "country"
df_players = df_players.withColumnRenamed(
    PlayersSchema.IOC,
    PlayersCreatedSchema.COUNTRY
)

# COMMAND ----------

# Name cols in df_players "winner_name" and "loser_name" refer to the ENTIRE name, not like in df_players in which we have "name_first" and "name_last", so create a new col in the original one to account for that in the case we find cross information to fill potential missing name values in both dfs
df_players = df_players.withColumn(PlayersCreatedSchema.FULL_NAME, concat_ws(" ", col(PlayersSchema.NAME_FIRST), col(PlayersSchema.NAME_LAST))) 
 
# Fill missing first names in df_players from df_matches
df_matches_winner = df_matches.select(MatchesSchema.WINNER_ID, MatchesSchema.WINNER_NAME).distinct()
df_matches_loser = df_matches.select(MatchesSchema.LOSER_ID, MatchesSchema.LOSER_NAME).distinct()

# Merge winner and loser names to df_players df, in order to see if we can fill missing names with df_matches data.
df_players = df_players.join(
    df_matches_winner,
    (df_players.player_id == df_matches_winner.winner_id),
    "left"
).join(
    df_matches_loser,
    (df_players.player_id == df_matches_loser.loser_id),
    "left"
)

# # Duplicated ID=104273
df_players = df_players.withColumn(
    MatchesSchema.WINNER_NAME,
    when(
        col(PlayersCreatedSchema.FULL_NAME) != col(MatchesSchema.WINNER_NAME),
        col(PlayersCreatedSchema.FULL_NAME)
        ).otherwise(col(MatchesSchema.WINNER_NAME))
).withColumn(
    MatchesSchema.LOSER_NAME,
    when(
        col(PlayersCreatedSchema.FULL_NAME) != col(MatchesSchema.LOSER_NAME),
        col(PlayersCreatedSchema.FULL_NAME)
    ).otherwise(col(MatchesSchema.LOSER_NAME))
).dropDuplicates()

# Drop the added winner_id, winner_name, loser_id, loser_name cols (dont need them anymoer)
df_players = df_players.drop(MatchesSchema.WINNER_ID, MatchesSchema.LOSER_ID, MatchesSchema.LOSER_NAME, MatchesSchema.WINNER_NAME)


# COMMAND ----------

# Process matches names, use df_players "full_name" as source of truth for por potential misspelled names
df_matches = df_matches.alias("m1").join(
    df_players.alias("p1").select(col(PlayersSchema.PLAYER_ID), col(PlayersCreatedSchema.FULL_NAME).alias(MatchesCreatedSchema.WINNER_FULL_NAME)),
    col("m1.winner_id") == col("p1.player_id"),

    "left"
)

df_matches = df_matches.alias("m2").join(
    df_players.alias("p2").select(col(PlayersSchema.PLAYER_ID), col(PlayersCreatedSchema.FULL_NAME).alias(MatchesCreatedSchema.LOSER_FULL_NAME)),
    col("m2.loser_id") == col("p2.player_id"), 
    "left"
)

# Replace the winner name and loser names with the created full_name using df_players "name_first" + "name_second" (more reliable)
df_matches = df_matches.withColumn(
    MatchesSchema.WINNER_NAME,
    when(
        col(MatchesSchema.WINNER_NAME) != col(MatchesCreatedSchema.WINNER_FULL_NAME),
        col(MatchesCreatedSchema.WINNER_FULL_NAME)
    ).otherwise(col(MatchesSchema.WINNER_NAME))
).withColumn(
    MatchesSchema.LOSER_NAME,
    when(
        col(MatchesSchema.LOSER_NAME) != col(MatchesCreatedSchema.LOSER_FULL_NAME),
        col(MatchesCreatedSchema.LOSER_FULL_NAME)
    ).otherwise(col(MatchesSchema.LOSER_NAME))
)

# We've created duplicated "player_id" cols (from "m2" and "p2"), alongside "winner_full_name" and "loser_full_name" cols, remove them
df_matches = df_matches.drop(MatchesCreatedSchema.WINNER_FULL_NAME, MatchesCreatedSchema.LOSER_FULL_NAME, PlayersSchema.PLAYER_ID)


# COMMAND ----------

# Save them as csv (TODO: use delta format later) in the silver layer
csv_handler.save(
    path=Paths.SILVER_PATH,
    df=df_players,
    name=DataFrameNames.DF_PLAYERS,
)
csv_handler.save(
    path=Paths.SILVER_PATH,
    df=df_matches,
    name=DataFrameNames.DF_MATCHES,
)
