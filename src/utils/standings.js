// src/utils/standings.js
import { TEAMS } from '../schedule';

export function calculateStandings(processedWeeks, upToWeek = 100) {
  const standings = {};
  
  // Initialize
  Object.keys(TEAMS).forEach(id => {
    standings[id] = { 
      id: parseInt(id), 
      wins: 0, losses: 0, ties: 0, 
      score: 0 
    };
  });

  // Calculate Records
  processedWeeks.forEach(week => {
    if (week.weekId > upToWeek) return;

    if (week.matchups) {
      week.matchups.forEach(m => {
        if (!m.result || m.isPlaceholder) return;

        // --- TRIO SCORING ---
        if (m.type === 'trio') {
          m.teams.forEach(team => {
            const tr = m.result[team.id];
            if (!tr) return;

            // 1st place = Win, 3rd place = Loss, 2nd place = Tie
            if (tr.rank === 1) standings[team.id].wins++;
            else if (tr.rank === 3) standings[team.id].losses++;
            else standings[team.id].ties++;

            standings[team.id].score += tr.points;
          });
        } 
        // --- HEAD-TO-HEAD SCORING ---
        else {
          if (!m.homeTeam?.id || !m.awayTeam?.id) return;

          const home = standings[m.homeTeam.id];
          const away = standings[m.awayTeam.id];

          // If score is 0-0, the week probably hasn't happened yet
          if (m.result.homeScore + m.result.awayScore === 0 && m.result.ties === 0) return;

          if (m.result.homeScore > m.result.awayScore) home.wins++;
          else if (m.result.homeScore < m.result.awayScore) home.losses++;
          else home.ties++;

          if (m.result.awayScore > m.result.homeScore) away.wins++;
          else if (m.result.awayScore < m.result.homeScore) away.losses++;
          else away.ties++;

          // Add Category Score for Tiebreakers
          home.score += m.result.homeScore + (m.result.ties * 0.5);
          away.score += m.result.awayScore + (m.result.ties * 0.5);
        }
      });
    }
  });

  // Convert to Array and Sort
  return Object.values(standings).sort((a, b) => {
    // 1. Sort by Win Pct
    const totalA = a.wins + a.losses + a.ties;
    const totalB = b.wins + b.losses + b.ties;
    
    const pctA = totalA > 0 ? (a.wins + (a.ties * 0.5)) / totalA : 0;
    const pctB = totalB > 0 ? (b.wins + (b.ties * 0.5)) / totalB : 0;
    
    if (pctB !== pctA) return pctB - pctA;

    // 2. Tiebreaker: Total Category Score
    return b.score - a.score;
  });
}