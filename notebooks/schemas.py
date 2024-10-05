# Schema namespaces
class PlayersSchema:
    PLAYER_ID = "player_id"
    NAME_FIRST = "name_first"
    NAME_LAST = "name_last"
    IOC = "ioc"
    DOB = "dob"
    WIKIDATA_ID = "wikidata_id"
    
class PlayersCreatedSchema:
    FULL_NAME = "full_name"
    COUNTRY = "country"

class MatchesSchema:
    WINNER_NAME = "winner_name"
    LOSER_NAME = "loser_name"
    WINNER_ID = "winner_id"
    LOSER_ID = "loser_id"
    TOURNEY_NAME = "tourney_name"
    TOURNEY_ID = "tourney_id"
    SURFACE = "surface"
    ROUND = "round"
    TOURNEY_LEVEL = "tourney_level"
    W_BP_SAVED = "w_bpSaved"
    W_BP_FACED = "w_bpFaced"
    L_BP_SAVED = "l_bpSaved"
    L_BP_FACED = "l_bpFaced"
    
class MatchesCreatedSchema:
    YEAR = "year"
    PATH = "path"
    LOSER_FULL_NAME = "loser_full_name"
    WINNER_FULL_NAME  = "winner_full_name"


class PlayersStatisticsSchema:
    MATCHES_PLAYED = "matches_played"
    MATCHES_LOST = "matches_lost"
    MATCHES_WON = "matches_won"
    WIN_PERCENTAGE = "win_%"
    LOSS_PERCENTAGE = "loss_%"
    GS_PLAYED = "gs_played"
    GS_WON = "gs_won"
    GS_MATCHES_PLAYED = "gs_matches_played"
    GS_MATCHES_WON = "gs_matches_won"
    GS_MATCHES_LOST = "gs_matches_lost"
    GS_MATCHES_WIN_PERCENTAGE = "gs_matches_win_%"
    MASTERS_1000_MATCHES_PLAYED = "masters_1000_matches_played"
    FINALS_PLAYED = "finals_played"
    MASTERS_1000_PLAYED = "masters_1000_played"
    DAVIS_CUP_PLAYED = "davis_cup_played"
    CARRER_W_BP_SAVED_PERCENTAGE = "carrer_wins_bp_saved_%"  # in their won matches, how many of them did they save?
    CARRER_L_BP_SAVED_PERCENTAGE = "carrer_losses_bp_saved_%"  # in their lost matches, how many of them did they save?
    CARRER_AVG_BP_SAVED_PERCENTAGE = "carrer_avg_bp_saved_%"  # avg of the above two
    # BP_FACED_PERCENTAGE = "bp faced %"  # NOTE: cant calculate it, since we cant calculate the total number of points played it seems
    FINALS_PLAYED_PERCENTAGE = "finals_played_%"
    FINALS_WON = "finals_won"
    FINALS_WIN_PERCENTAGE = "finals_win_%"
    GS_WIN_PERCENTAGE = "gs_win_%"
    MASTERS_1000_WIN_PERCENTAGE = "masters_1000_win_%"
    DAVIS_WIN_PERCENTAGE = "davis_win_%"
    TOURNAMETS_PLAYED = "tournaments_played"
    TOURNAMENT_WIN_PERCENTAGE = "tournament_win_%"
