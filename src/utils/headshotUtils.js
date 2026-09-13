// --- UNIVERSAL PLAYER HEADSHOT RESOLUTION ENGINE ---

const globalPlayerLookup = new Map();

export function updateGlobalPlayerLookup(playerList) {
  if (!Array.isArray(playerList)) return;
  playerList.forEach(p => {
    if (!p) return;
    const espnId = String(p['ESPN PlayerID'] || p.espn_player_id || p.player_id || p.id || '');
    if (espnId && !globalPlayerLookup.has(espnId)) {
      globalPlayerLookup.set(espnId, p);
    }
  });
}

export function getPlayerHeadshotUrl(player) {
  if (!player) {
    return 'https://midfield.mlbstatic.com/v1/people/generic/spots/120';
  }

  // Handle case where an ID or partial pick was passed
  let resolved = player;
  if (typeof player === 'string' || typeof player === 'number') {
    resolved = globalPlayerLookup.get(String(player)) || { 'ESPN PlayerID': String(player) };
  } else if (!resolved.MLBAMID && !resolved.mlbamid) {
    const espnId = String(resolved['ESPN PlayerID'] || resolved.espn_player_id || resolved.player_id || resolved.id || '');
    if (espnId && globalPlayerLookup.has(espnId)) {
      resolved = { ...globalPlayerLookup.get(espnId), ...resolved };
    }
  }

  const mlbId = resolved.MLBAMID || resolved.mlbamid || resolved.mlbam_id || resolved.playerid;
  if (mlbId) {
    return `https://midfield.mlbstatic.com/v1/people/${mlbId}/spots/120`;
  }

  const espnId = resolved['ESPN PlayerID'] || resolved.espn_player_id || resolved.player_id || resolved.id;
  if (espnId) {
    return `https://a.espncdn.com/combiner/i?img=/i/headshots/mlb/players/full/${espnId}.png&w=350&h=254`;
  }

  return 'https://midfield.mlbstatic.com/v1/people/generic/spots/120';
}

export function handleHeadshotError(e, player) {
  const espnId = player?.['ESPN PlayerID'] || player?.espn_player_id || player?.player_id || player?.id;
  if (espnId && !e.target.src.includes('espncdn.com')) {
    e.target.src = `https://a.espncdn.com/combiner/i?img=/i/headshots/mlb/players/full/${espnId}.png&w=350&h=254`;
  } else {
    e.target.src = 'https://midfield.mlbstatic.com/v1/people/generic/spots/120';
  }
}
