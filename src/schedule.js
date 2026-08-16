// --- ADDED parseISO to this import list ---
import { addDays, format, differenceInDays, parseISO } from 'date-fns';

// --- CONFIGURATION: ROLLOVER HERE YEAR TO YEAR ---
const SEASON_START_DATE = "2025-03-18"; // The date of Scoring Period 1
const SEASON_YEAR = 2025;

// 1. Define the Teams (IDs based on your CSV)
export const TEAMS = {
  1: { name: "Tim", owner: "Tim" },
  2: { name: "Adrian", owner: "Adrian" },
  3: { name: "Garrett", owner: "Garrett" },
  5: { name: "Daniel", owner: "Daniel" },
  6: { name: "Anil", owner: "Anil" },
  8: { name: "Alex", owner: "Alex" },
  12: { name: "Will", owner: "Will" },
  13: { name: "Mark", owner: "Mark" }
};

// Helper to generate Round Robin pairings
// We have 8 teams. A full cycle is 7 weeks.
const TEAM_IDS = [1, 2, 3, 5, 6, 8, 12, 13];
// 1. Convert Date -> Scoring Period ID (e.g., "2025-03-25" -> 2)
export function getScoringPeriodId(dateString) {
  const start = parseISO(SEASON_START_DATE);
  const target = parseISO(dateString);
  const diff = differenceInDays(target, start);
  
  // Scoring Period 1 is day 0 diff, so we add 1
  // If the date is before the season, returning < 1 is fine (handled by callers)
  return diff + 1; 
}

// 2. Convert Scoring Period ID -> Date String (e.g., 2 -> "2025-03-25")
export function getDateFromPeriodId(periodId) {
  const start = parseISO(SEASON_START_DATE);
  // Period 1 is the start date, so add (periodId - 1) days
  const target = addDays(start, periodId - 1);
  return format(target, 'yyyy-MM-dd');
}

// 3. Get the Range of IDs for a Week (Used by your App.jsx fetch)
export function getPeriodRangeForWeek(weekObj) {
  const startId = getScoringPeriodId(weekObj.startDate);
  const endId = getScoringPeriodId(weekObj.endDate);
  return { startId, endId };
}

// --- ROUND ROBIN LOGIC (Unchanged) ---
function getRoundRobinMatchups(weekIndex) {
  const numTeams = TEAM_IDS.length; 
  const numRounds = numTeams - 1;   
  const round = weekIndex % numRounds;
  const fixedTeam = TEAM_IDS[0];
  const rotators = TEAM_IDS.slice(1);
  const n = rotators.length;
  const rotated = [...rotators.slice(n - round), ...rotators.slice(0, n - round)];
  const currentOrder = [fixedTeam, ...rotated];
  
  const matchups = [];
  for (let i = 0; i < numTeams / 2; i++) {
    matchups.push({
      id: `${weekIndex + 1}-${i}`,
      homeTeamId: currentOrder[i],
      awayTeamId: currentOrder[numTeams - 1 - i]
    });
  }
  return matchups;
}

// --- THE SCHEDULE ---
// Note: Scoring Period IDs are now dynamic. 
// Example: Week 1 ends April 6. getScoringPeriodId("2025-04-06") will automatically calculate ID 14.
export const LEAGUE_SCHEDULE = [
  { weekId: 1, name: "Week 1", startDate: "2025-03-18", endDate: "2025-04-06", matchups: getRoundRobinMatchups(0) },
  { weekId: 2, name: "Week 2", startDate: "2025-04-07", endDate: "2025-04-13", matchups: getRoundRobinMatchups(1) },
  { weekId: 3, name: "Week 3", startDate: "2025-04-14", endDate: "2025-04-20", matchups: getRoundRobinMatchups(2) },
  { weekId: 4, name: "Week 4", startDate: "2025-04-21", endDate: "2025-04-27", matchups: getRoundRobinMatchups(3) },
  { weekId: 5, name: "Week 5", startDate: "2025-04-28", endDate: "2025-05-04", matchups: getRoundRobinMatchups(4) },
  { weekId: 6, name: "Week 6", startDate: "2025-05-05", endDate: "2025-05-11", matchups: getRoundRobinMatchups(5) },
  { weekId: 7, name: "Week 7", startDate: "2025-05-12", endDate: "2025-05-18", matchups: getRoundRobinMatchups(6) },
  { weekId: 8, name: "Week 8", startDate: "2025-05-19", endDate: "2025-05-25", matchups: getRoundRobinMatchups(7) },
  { weekId: 9, name: "Week 9", startDate: "2025-05-26", endDate: "2025-06-01", matchups: getRoundRobinMatchups(0) }, // Cycle 2 Start
  { weekId: 10, name: "Week 10", startDate: "2025-06-02", endDate: "2025-06-08", matchups: getRoundRobinMatchups(1) },
  { weekId: 11, name: "Week 11", startDate: "2025-06-09", endDate: "2025-06-15", matchups: getRoundRobinMatchups(2) },
  { weekId: 12, name: "Week 12", startDate: "2025-06-16", endDate: "2025-06-22", matchups: getRoundRobinMatchups(3) },
  { weekId: 13, name: "Week 13", startDate: "2025-06-23", endDate: "2025-06-29", matchups: getRoundRobinMatchups(4) },
  { weekId: 14, name: "Week 14", startDate: "2025-06-30", endDate: "2025-07-06", matchups: getRoundRobinMatchups(5) },
  { weekId: 15, name: "Week 15", startDate: "2025-07-07", endDate: "2025-07-20", matchups: getRoundRobinMatchups(6) }, // ASG Long Week
  { weekId: 16, name: "Week 16", startDate: "2025-07-21", endDate: "2025-07-27", matchups: getRoundRobinMatchups(7) },
  { weekId: 17, name: "Week 17", startDate: "2025-07-28", endDate: "2025-08-03", matchups: getRoundRobinMatchups(0) }, // Cycle 3 Start
  { weekId: 18, name: "Week 18", startDate: "2025-08-04", endDate: "2025-08-10", matchups: getRoundRobinMatchups(1) },
  { weekId: 19, name: "Week 19", startDate: "2025-08-11", endDate: "2025-08-17", matchups: getRoundRobinMatchups(2) },
  { weekId: 20, name: "Week 20", startDate: "2025-08-18", endDate: "2025-08-24", matchups: getRoundRobinMatchups(3) },
  { weekId: 21, name: "Week 21", startDate: "2025-08-25", endDate: "2025-08-31", matchups: getRoundRobinMatchups(4) },
  { weekId: 22, name: "Week 22", startDate: "2025-09-01", endDate: "2025-09-07", matchups: getRoundRobinMatchups(5) },
  { weekId: 23, name: "Week 23", startDate: "2025-09-08", endDate: "2025-09-14", matchups: getRoundRobinMatchups(6) },
  { weekId: 24, name: "Playoffs 1", startDate: "2025-09-15", endDate: "2025-09-21", matchups: [] },
  { weekId: 25, name: "Championship", startDate: "2025-09-22", endDate: "2025-09-28", matchups: [] },
];