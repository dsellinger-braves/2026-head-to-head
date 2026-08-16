// src/utils/liveMLB.js

export const fetchLiveScoreboard = async (date = 'today') => {
  try {
    const dateParam = date === 'today' ? '' : `&date=${date}`;
    const url = `https://statsapi.mlb.com/api/v1/schedule?sportId=1${dateParam}&hydrate=linescore,team,probablePitcher,person,decisions`;
    
    const response = await fetch(url);
    const data = await response.json();
    
    if (!data.dates || data.dates.length === 0) return [];
    return data.dates[0].games;
  } catch (err) {
    console.error("Error fetching live MLB data:", err);
    return [];
  }
};

// Strips accents, punctuation, and common suffixes for a clean match
export const normalizeName = (name) => {
  if (!name) return "";
  return name
    .toLowerCase()
    .normalize("NFD") 
    .replace(/[\u0300-\u036f]/g, "") 
    .replace(/[^a-z]/g, "") 
    .replace(/(jr|sr|ii|iii|iv)$/, ""); 
};

export const buildRosterDictionary = (todaysRecords) => {
  const dict = {};
  todaysRecords.forEach(record => {
    const cleanName = normalizeName(record.full_name);
    // We can also store the lineup slot so the UI knows if they are benched
    dict[cleanName] = {
      teamId: record.team_id,
      isBench: record.lineup_slot_id >= 16
    };
  });
  return dict;
};

// Add this to the bottom of src/utils/liveMLB.js

// FETCH SINGLE GAME BOXSCORE
// This gives us every player on the roster for a specific game, plus their live stats
export const fetchGameBoxscore = async (gamePk) => {
  try {
    const url = `https://statsapi.mlb.com/api/v1/game/${gamePk}/boxscore`;
    const response = await fetch(url);
    const data = await response.json();
    return data;
  } catch (err) {
    console.error(`Error fetching boxscore for game ${gamePk}:`, err);
    return null;
  }
};