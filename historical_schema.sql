CREATE TABLE IF NOT EXISTS public.historical_data (
    -- Primary Keys (ESPN doesn't have a strict unique constraint per row across all CSVs, so we'll use a composite if needed, or no PK and rely on deduplication in the frontend)
    _db_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    league_id BIGINT NOT NULL,
    
    -- Metadata
    season_year INT,
    year INT,
    team_id INT,
    scoring_period_id INT,
    id INT, -- player_id
    onteamid INT,
    "fullName" TEXT,
    "lineupSlotID" INT,
    
    -- Stats (ESPN stat IDs 0-99)
    "0" FLOAT8, "1" FLOAT8, "2" FLOAT8, "3" FLOAT8, "4" FLOAT8, "5" FLOAT8, "6" FLOAT8, "7" FLOAT8, "8" FLOAT8, "9" FLOAT8,
    "10" FLOAT8, "11" FLOAT8, "12" FLOAT8, "13" FLOAT8, "14" FLOAT8, "15" FLOAT8, "16" FLOAT8, "17" FLOAT8, "18" FLOAT8, "19" FLOAT8,
    "20" FLOAT8, "21" FLOAT8, "22" FLOAT8, "23" FLOAT8, "24" FLOAT8, "25" FLOAT8, "26" FLOAT8, "27" FLOAT8, "28" FLOAT8, "29" FLOAT8,
    "30" FLOAT8, "31" FLOAT8, "32" FLOAT8, "33" FLOAT8, "34" FLOAT8, "35" FLOAT8, "36" FLOAT8, "37" FLOAT8, "38" FLOAT8, "39" FLOAT8,
    "40" FLOAT8, "41" FLOAT8, "42" FLOAT8, "43" FLOAT8, "44" FLOAT8, "45" FLOAT8, "46" FLOAT8, "47" FLOAT8, "48" FLOAT8, "49" FLOAT8,
    "50" FLOAT8, "51" FLOAT8, "52" FLOAT8, "53" FLOAT8, "54" FLOAT8, "55" FLOAT8, "56" FLOAT8, "57" FLOAT8, "58" FLOAT8, "59" FLOAT8,
    "60" FLOAT8, "61" FLOAT8, "62" FLOAT8, "63" FLOAT8, "64" FLOAT8, "65" FLOAT8, "66" FLOAT8, "67" FLOAT8, "68" FLOAT8, "69" FLOAT8,
    "70" FLOAT8, "71" FLOAT8, "72" FLOAT8, "73" FLOAT8, "74" FLOAT8, "75" FLOAT8, "76" FLOAT8, "77" FLOAT8, "78" FLOAT8, "79" FLOAT8,
    "80" FLOAT8, "81" FLOAT8, "82" FLOAT8, "83" FLOAT8, "99" FLOAT8,
    
    UNIQUE (league_id, season_year, scoring_period_id, team_id, id)
);
