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
    round,
    to_date,
    avg,
    sum,
    countDistinct,
    expr,
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
from cols_uniques import TourneyLevel, Round
# fmt: off


# COMMAND ----------

silver_files = dbutils.fs.ls(Paths.SILVER_PATH.value)
# matches_csv_path = [file.path for file in silver_files if "matches" in file.name][0]
# players_csv_path = [file.path for file in silver_files if "players" in file.name][0]


# COMMAND ----------

# Handlers
delta_handler = DataFrameHandlerFactory.get_handler(Extension.DELTA)
csv_handler = DataFrameHandlerFactory.get_handler(Extension.CSV)

# For loading, use the csv handler. For writing, the delta one
df_players = csv_handler.load(spark=spark, path=Paths.SILVER_PATH, name=DataFrameNames.DF_PLAYERS)
df_matches = csv_handler.load(spark=spark, path=Paths.SILVER_PATH, name=DataFrameNames.DF_MATCHES)

# COMMAND ----------

# Get the big three
ID_DJOKOVIC = df_players.filter(col(PlayersCreatedSchema.FULL_NAME) == "Novak Djokovic").select(PlayersSchema.PLAYER_ID).collect()[0][0]
ID_FEDERER = df_players.filter(col(PlayersCreatedSchema.FULL_NAME) == "Roger Federer").select(PlayersSchema.PLAYER_ID).collect()[0][0]
ID_NADAL = df_players.filter(col(PlayersCreatedSchema.FULL_NAME) == "Rafael Nadal").select(PlayersSchema.PLAYER_ID).collect()[0][0]

# COMMAND ----------

#  WIN_PERCENTAGE and LOSS_PERCENTAGE

# Losses count
df_losers = df_matches.groupBy(MatchesSchema.LOSER_ID).count().withColumnRenamed("count", PlayersStatisticsSchema.MATCHES_LOST).withColumnRenamed(MatchesSchema.LOSER_ID, PlayersSchema.PLAYER_ID)

# Wins count
df_winners = df_matches.groupBy(MatchesSchema.WINNER_ID).count().withColumnRenamed("count", PlayersStatisticsSchema.MATCHES_WON).withColumnRenamed(MatchesSchema.WINNER_ID, PlayersSchema.PLAYER_ID)

# Merge them into 1 df, so for ID, get losses count and wins count
df_wins_losses = df_losers.alias("l").join(
    df_winners.alias("w"),
    col("l.player_id") == col("w.player_id"),
    "outer"
).select(
    coalesce(col("l.player_id"), col("w.player_id")).alias(PlayersSchema.PLAYER_ID),
    col("l.matches_lost"),
    col("w.matches_won")
)

# Add matches played and win percentage cols
df_wins_losses = df_wins_losses.withColumn(
    PlayersStatisticsSchema.MATCHES_PLAYED,
    col(PlayersStatisticsSchema.MATCHES_LOST) + col(PlayersStatisticsSchema.MATCHES_WON)
).withColumn(
    PlayersStatisticsSchema.WIN_PERCENTAGE,
    round(col(PlayersStatisticsSchema.MATCHES_WON) / col(PlayersStatisticsSchema.MATCHES_PLAYED) * 100, 2)
)


# COMMAND ----------

# Create a df_players_statistics, and start appending per-player calculated metrics into it (based on df_matches)
df_players_statistics = df_players.join(
    df_wins_losses,
    df_players.player_id == df_wins_losses.player_id,
    "left"
).drop(df_wins_losses.player_id) # remove the 2nd player_id col to avoid duplicate cols later on 

# COMMAND ----------

# Cast to int -> (W_BP_saved, W_BP_faced) and (L_BP_saved, L_BP_faced)
df_matches = df_matches.withColumn(
    MatchesSchema.W_BP_SAVED, 
    col(MatchesSchema.W_BP_SAVED).cast("int")
).withColumn(
    MatchesSchema.W_BP_FACED,
    col(MatchesSchema.W_BP_FACED).cast("int")
).withColumn(
    MatchesSchema.L_BP_SAVED,
    col(MatchesSchema.L_BP_SAVED).cast("int")
).withColumn(
    MatchesSchema.L_BP_FACED,
    col(MatchesSchema.L_BP_FACED).cast("int")
)

# Calculate BP saved % for won matches
df_won_bp_saved = df_matches.groupBy(MatchesSchema.WINNER_ID).agg(
    round((sum(MatchesSchema.W_BP_SAVED) / sum(MatchesSchema.W_BP_FACED)) * 100, 2).alias(PlayersStatisticsSchema.CARRER_W_BP_SAVED_PERCENTAGE)
).withColumnRenamed(MatchesSchema.WINNER_ID, PlayersSchema.PLAYER_ID)

# Calculate BP saved % for lost matches
df_lost_bp_saved = df_matches.groupBy(MatchesSchema.LOSER_ID).agg(
    round((sum(MatchesSchema.L_BP_SAVED) / sum(MatchesSchema.L_BP_FACED)) * 100, 2).alias(PlayersStatisticsSchema.CARRER_L_BP_SAVED_PERCENTAGE)
).withColumnRenamed(MatchesSchema.LOSER_ID, PlayersSchema.PLAYER_ID)

# COMMAND ----------

# df_lost_bp_saved.filter(col(PlayersSchema.PLAYER_ID) == ID_DJOKOVIC).show()
# df_won_bp_saved.filter(col(PlayersSchema.PLAYER_ID) == ID_DJOKOVIC).show()
# df_lost_bp_saved

# NOTE: this is correct! adjust the aliases (win %, loss %, avg %) but the numbers are correct. Join with the statistics df
df_carrer_bp_saved = df_lost_bp_saved.join(
    df_won_bp_saved,
    df_lost_bp_saved.player_id == df_won_bp_saved.player_id,
    "outer"
).drop(df_lost_bp_saved.player_id).withColumn(
    PlayersStatisticsSchema.CARRER_AVG_BP_SAVED_PERCENTAGE,
    round((col(PlayersStatisticsSchema.CARRER_W_BP_SAVED_PERCENTAGE) + col(PlayersStatisticsSchema.CARRER_L_BP_SAVED_PERCENTAGE)) / 2,2)
)

# Add carrer w,l, avg bp saved % info to statistics df
df_players_statistics = df_players_statistics.join(
    df_carrer_bp_saved,
    df_carrer_bp_saved.player_id == df_players_statistics.player_id,
    "left"
).drop(df_carrer_bp_saved.player_id)

# COMMAND ----------

# FINALS_APPEARANCES_PERCENTAGE = "finals appearances %"
# FINALS_WIN_PERCENTAGE = "finals win %"
# GS_WIN_PERCENTAGE = "gs win %"

# COMMAND ----------

# Calculate the number of tournaments played by each player
df_tournaments_played = df_matches.select(
    col(MatchesSchema.WINNER_ID).alias(PlayersSchema.PLAYER_ID),
    col(MatchesSchema.TOURNEY_ID)
).union(
    df_matches.select(
        col(MatchesSchema.LOSER_ID).alias(PlayersSchema.PLAYER_ID),
        col(MatchesSchema.TOURNEY_ID)
    )
).distinct().groupBy(
    PlayersSchema.PLAYER_ID
).agg(
    countDistinct(MatchesSchema.TOURNEY_ID).alias(PlayersStatisticsSchema.TOURNAMETS_PLAYED)
)

# Out of all the tournaments played, in how many finals they were?

# Filter only final round matches
df_finals = df_matches.filter(col(MatchesSchema.ROUND) == Round.F)

# Get the n of finals appearances, each row will be a final match they played
df_finals_played_rows = df_finals.select(
    MatchesSchema.WINNER_ID, MatchesSchema.TOURNEY_ID
).withColumnRenamed(
    MatchesSchema.WINNER_ID, PlayersSchema.PLAYER_ID
).union(
    df_finals.select(
        MatchesSchema.LOSER_ID, MatchesSchema.TOURNEY_ID
    ).withColumnRenamed(
        MatchesSchema.LOSER_ID, PlayersSchema.PLAYER_ID
    )
).distinct()

# Since each row in the previous df means a match final, just count them for each player to get the n of finals played
df_finals_played = df_finals_played_rows.groupBy(PlayersSchema.PLAYER_ID).agg(
    countDistinct(MatchesSchema.TOURNEY_ID).alias(PlayersStatisticsSchema.FINALS_PLAYED)
)

# Get only the winner_ids from the finals df, groupby it to get the number of finals played
df_finals_won = df_finals.select(
    MatchesSchema.TOURNEY_ID, MatchesSchema.WINNER_ID
).withColumnRenamed(
    MatchesSchema.WINNER_ID, PlayersSchema.PLAYER_ID
).groupBy(PlayersSchema.PLAYER_ID).agg(
    countDistinct(MatchesSchema.TOURNEY_ID
).alias(PlayersStatisticsSchema.FINALS_WON))


# COMMAND ----------

# Now that we have tournaments played, finals appearances, and finals won, we can calculate the finals appearances %, finals % percentage, and tournament % percentage (% of winning a tournament they play)

df_players_statistics = df_players_statistics.join(
    df_finals_played,
    df_players_statistics.player_id == df_finals_played.player_id,
    "left"
).drop(df_finals_played.player_id).join(
    df_finals_won,
    df_players_statistics.player_id == df_finals_won.player_id,
    "left"
).drop(df_finals_won.player_id).join(
    df_tournaments_played,
    df_players_statistics.player_id == df_tournaments_played.player_id,
    "left"
).drop(df_tournaments_played.player_id)

df_players_statistics = df_players_statistics.withColumn(
    PlayersStatisticsSchema.FINALS_PLAYED_PERCENTAGE,
    round(col(PlayersStatisticsSchema.FINALS_PLAYED) / col(PlayersStatisticsSchema.TOURNAMETS_PLAYED) * 100, 2)
).withColumn(
    PlayersStatisticsSchema.FINALS_WIN_PERCENTAGE,
    round(col(PlayersStatisticsSchema.FINALS_WON) / col(PlayersStatisticsSchema.FINALS_PLAYED) * 100, 2)
).withColumn(
    PlayersStatisticsSchema.TOURNAMENT_WIN_PERCENTAGE,
    round(col(PlayersStatisticsSchema.FINALS_WON) / col(PlayersStatisticsSchema.TOURNAMETS_PLAYED) * 100, 2)
)

# COMMAND ----------

# # Get distinct tourney_level values, used to add the "_appearances" suffix later
# tourney_levels = df_matches.select(MatchesSchema.TOURNEY_LEVEL).distinct().rdd.flatMap(lambda x: x).collect()

# # Wins appearances by tourney level (by player id) 
# df_win_appearances = df_matches.groupBy(MatchesSchema.WINNER_ID).pivot(MatchesSchema.TOURNEY_LEVEL).count().na.fill(0).withColumnRenamed(
#     MatchesSchema.WINNER_ID,
#     PlayersSchema.PLAYER_ID,
# )

# # loss appearances by tourney level (by player id) 
# df_loss_appearances = df_matches.groupBy(MatchesSchema.LOSER_ID).pivot(MatchesSchema.TOURNEY_LEVEL).count().na.fill(0).withColumnRenamed(
#     MatchesSchema.LOSER_ID,
#     PlayersSchema.PLAYER_ID,
# )


# df_appearances = df_loss_appearances.join(
#     df_win_appearances,
#     df_loss_appearances.player_id == df_win_appearances.player_id,
#     "outer"
# ).drop(df_win_appearances.player_id)

# df_loss_appearances.filter(col("player_id") == ID_DJOKOVIC).show(10)
# df_win_appearances.filter(col("player_id") == ID_DJOKOVIC).show(10)
# df_appearances.filter(col("player_id") == ID_DJOKOVIC).show(10)


# Gs % percentage:
#   need to get, for player_id, the  (winner_id / winner_id + loser_id) where tournamente level is GS.
# For final appearances %:
#   need to get all of their matches played (winner_id + loser_id), and from those, get the (winner_id + loser_id) where those are final stages

# COMMAND ----------

df_gs_matches = df_matches.filter(col(MatchesSchema.TOURNEY_LEVEL) == TourneyLevel.GS)

df_gs_played = df_gs_matches.select(
    col(MatchesSchema.WINNER_ID).alias(PlayersSchema.PLAYER_ID),
    col(MatchesSchema.TOURNEY_ID)
).union(
    df_gs_matches.select(
        col(MatchesSchema.LOSER_ID).alias(PlayersSchema.PLAYER_ID),
        col(MatchesSchema.TOURNEY_ID)
    )
).groupBy(
    PlayersSchema.PLAYER_ID
).agg(countDistinct(MatchesSchema.TOURNEY_ID).alias(PlayersStatisticsSchema.GS_PLAYED))


df_gs_won = df_gs_matches.select(
    col(MatchesSchema.WINNER_ID).alias(PlayersSchema.PLAYER_ID)
).filter(
    col(MatchesSchema.ROUND) == Round.F
).groupBy(PlayersSchema.PLAYER_ID).count().withColumnRenamed(
    "count",
    PlayersStatisticsSchema.GS_WON
)


df_gs_matches_played = df_gs_matches.select(
    col(MatchesSchema.WINNER_ID).alias(PlayersSchema.PLAYER_ID)
).union(
    df_gs_matches.select(
        col(MatchesSchema.LOSER_ID).alias(PlayersSchema.PLAYER_ID)
    )
).groupBy(PlayersSchema.PLAYER_ID).count().withColumnRenamed(
    "count",
    PlayersStatisticsSchema.GS_MATCHES_PLAYED
)


df_gs_matches_won = df_gs_matches.select(
    col(MatchesSchema.WINNER_ID).alias(PlayersSchema.PLAYER_ID)
).groupBy(PlayersSchema.PLAYER_ID).count().withColumnRenamed(
    "count",
    PlayersStatisticsSchema.GS_MATCHES_WON
)


# COMMAND ----------


# Now, Join all of them

df_gs_statistics = df_gs_played.join(
    df_gs_won,
    df_gs_played.player_id == df_gs_won.player_id,
    "left"
).drop(df_gs_won.player_id).join(
    df_gs_matches_played,
    df_gs_played.player_id == df_gs_matches_played.player_id,
    "left"
).drop(df_gs_matches_played.player_id).join(
    df_gs_matches_won,
    df_gs_played.player_id == df_gs_matches_won.player_id,
    "left"
).drop(df_gs_matches_won.player_id)


# And calculate the chance to win a GS they play in, their gs matches % percentage 
df_gs_statistics = df_gs_statistics.withColumn(
    PlayersStatisticsSchema.GS_WIN_PERCENTAGE,
    round((col(PlayersStatisticsSchema.GS_WON) / col(PlayersStatisticsSchema.GS_PLAYED)) * 100, 2)
).withColumn(
    PlayersStatisticsSchema.GS_MATCHES_WIN_PERCENTAGE,
    round((col(PlayersStatisticsSchema.GS_MATCHES_WON) / col(PlayersStatisticsSchema.GS_MATCHES_PLAYED)) * 100, 2)
)


# COMMAND ----------

# Save them as delta format 
delta_handler.save(
    path=Paths.GOLD_PATH,
    df=df_players,
    name=DataFrameNames.DF_PLAYERS,
)
delta_handler.save(
    path=Paths.GOLD_PATH,
    df=df_gs_statistics,
    name=DataFrameNames.DF_GS_STATISTICS,
)
delta_handler.save(
    path=Paths.GOLD_PATH,
    df=df_matches,
    name=DataFrameNames.DF_MATCHES,
)
