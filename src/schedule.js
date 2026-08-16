// 1. DEFINE TEAMS
// We added ID 9 for the new 9th owner, and ID 99 for the Ghost team
export const TEAMS = {
  1: { id: 1, name: "Tim", owner: "Tim" },
  2: { id: 2, name: "Adrian", owner: "Adrian" },
  3: { id: 3, name: "Garrett", owner: "Garrett" },
  5: { id: 5, name: "Daniel", owner: "Daniel" }, 
  6: { id: 6, name: "Anil", owner: "Anil" },
  8: { id: 8, name: "Alex", owner: "Alex" },
  12: { id: 12, name: "Will", owner: "Will" },
  13: { id: 13, name: "Mark", owner: "Mark" },
  14: { id: 14, name: "Preston", owner: "Preston" }, // <-- The 9th Team
  99: { id: 99, name: "League Average", owner: "The House" } // <-- The Ghost Team
};

// 2. DEFINE DATES
const SEASON_DATES = [
  { weekId: 1, startDate: "2026-03-25", endDate: "2026-04-05" },
  { weekId: 2, startDate: "2026-04-06", endDate: "2026-04-12" },
  { weekId: 3, startDate: "2026-04-13", endDate: "2026-04-19" },
  { weekId: 4, startDate: "2026-04-20", endDate: "2026-04-26" },
  { weekId: 5, startDate: "2026-04-27", endDate: "2026-05-03" },
  { weekId: 6, startDate: "2026-05-04", endDate: "2026-05-10" },
  { weekId: 7, startDate: "2026-05-11", endDate: "2026-05-17" },
  { weekId: 8, startDate: "2026-05-18", endDate: "2026-05-24" },
  { weekId: 9, startDate: "2026-05-25", endDate: "2026-05-31" },
  { weekId: 10, startDate: "2026-06-01", endDate: "2026-06-07" },
  { weekId: 11, startDate: "2026-06-08", endDate: "2026-06-14" },
  { weekId: 12, startDate: "2026-06-15", endDate: "2026-06-21" },
  { weekId: 13, startDate: "2026-06-22", endDate: "2026-06-28" }, // Mid-Season 1
  { weekId: 14, startDate: "2026-06-29", endDate: "2026-07-05" }, // Mid-Season 2
  { weekId: 15, startDate: "2026-07-06", endDate: "2026-07-19" }, // ASG (Jul 14-15) / Split Start
  { weekId: 16, startDate: "2026-07-20", endDate: "2026-07-26" },
  { weekId: 17, startDate: "2026-07-27", endDate: "2026-08-02" },
  { weekId: 18, startDate: "2026-08-03", endDate: "2026-08-09" },
  { weekId: 19, startDate: "2026-08-10", endDate: "2026-08-16" },
  { weekId: 20, startDate: "2026-08-17", endDate: "2026-08-23" },
  { weekId: 21, startDate: "2026-08-24", endDate: "2026-08-30" },
  { weekId: 22, startDate: "2026-08-31", endDate: "2026-09-06" },
  { weekId: 23, startDate: "2026-09-07", endDate: "2026-09-13" }, // Split End
  { weekId: 24, startDate: "2026-09-14", endDate: "2026-09-20" }, // Semis
  { weekId: 25, startDate: "2026-09-21", endDate: "2026-09-27" }  // Finals
];

// 3. HELPER: TRIOS GENERATOR (Weeks 1-12)
// This uses a perfect mathematical rotation for 9 teams over 4 weeks
const getTriosMatchups = (cycle, roundInCycle) => {
  const teamIds = [1, 2, 3, 5, 6, 8, 12, 13, 14]; // The 9 human teams
  
  // Base pattern indices for Kirkman's 9-item problem
  const rotations = [
    [[0,1,2], [3,4,5], [6,7,8]], // Week 1
    [[0,3,6], [1,4,7], [2,5,8]], // Week 2
    [[0,4,8], [1,5,6], [2,3,7]], // Week 3
    [[0,5,7], [1,3,8], [2,4,6]]  // Week 4
  ];

  const matchups = [];
  const groups = rotations[roundInCycle % 4]; 

  groups.forEach((groupIndices, i) => {
    matchups.push({
      id: `c${cycle}_r${roundInCycle}_m${i}`,
      teamIds: [teamIds[groupIndices[0]], teamIds[groupIndices[1]], teamIds[groupIndices[2]]],
      type: 'trio'
    });
  });

  return matchups;
};


// 4. MAIN GENERATOR FUNCTION
export const generateSchedule = () => {
  const schedule = [];

  // -------------------------------------------------------------------
  // PHASE 1: TRIPLE CROWN (Weeks 1-12)
  // 3 Cycles of 4 weeks each. Every team plays everyone exactly 3 times.
  // -------------------------------------------------------------------
  for (let i = 0; i < 12; i++) {
    const weekData = SEASON_DATES[i];
    const cycle = Math.floor(i / 4); 
    schedule.push({
      ...weekData,
      name: `Week ${weekData.weekId}`,
      matchups: getTriosMatchups(cycle, i),
      phase: 1
    });
  }

  // -------------------------------------------------------------------
  // PHASE 2: MID-SEASON CHAMPIONSHIP (Weeks 13-14)
  // Teams are seeded 1-9 based on Phase 1 standings.
  // -------------------------------------------------------------------
  const midSeasonMatchups = [
    { id: 'mid_top', teamIds: ['SEED_1', 'SEED_2', 'SEED_3'], type: 'trio', label: "🏆 First Half Title" },
    { id: 'mid_mid', teamIds: ['SEED_4', 'SEED_5', 'SEED_6'], type: 'trio', label: "⬆️ Promotion Battle" },
    { id: 'mid_bot', teamIds: ['SEED_7', 'SEED_8', 'SEED_9'], type: 'trio', label: "🛡️ Relegation Fight" }
  ];

  schedule.push({
    ...SEASON_DATES[12],
    name: "Mid-Season: Round 1",
    matchups: midSeasonMatchups,
    phase: 2
  });

  schedule.push({
    ...SEASON_DATES[13],
    name: "Mid-Season: Round 2",
    matchups: midSeasonMatchups, // Same matchups (App.jsx can aggregate these if desired)
    phase: 2
  });

  // -------------------------------------------------------------------
  // PHASE 3: SPLIT LEAGUES (Weeks 15-23)
  // Winners League (Top 4) vs. Consolation League (Bottom 5 + 1 Ghost)
  // Both transition to standard Head-to-Head 1v1 matchups.
  // -------------------------------------------------------------------
  const winnersRotation = [
    [['SEED_1', 'SEED_4'], ['SEED_2', 'SEED_3']],
    [['SEED_1', 'SEED_3'], ['SEED_4', 'SEED_2']],
    [['SEED_1', 'SEED_2'], ['SEED_3', 'SEED_4']]
  ];

  // 6-Team Polygon Rotation for Consolation (Seed 5-9 + Ghost)
  const consolationRotation = [
    [['SEED_5', 99], ['SEED_6', 'SEED_9'], ['SEED_7', 'SEED_8']],
    [['SEED_5', 'SEED_9'], [99, 'SEED_8'], ['SEED_6', 'SEED_7']],
    [['SEED_5', 'SEED_8'], ['SEED_9', 'SEED_7'], [99, 'SEED_6']],
    [['SEED_5', 'SEED_7'], ['SEED_8', 'SEED_6'], ['SEED_9', 99]],
    [['SEED_5', 'SEED_6'], ['SEED_7', 99], ['SEED_8', 'SEED_9']]
  ];

  for (let i = 0; i < 9; i++) {
    const weekData = SEASON_DATES[14 + i]; 
    const wPattern = winnersRotation[i % 3]; // Repeats every 3 weeks (3 cycles)
    const cPattern = consolationRotation[i % 5]; // Repeats every 5 weeks (1.8 cycles)

    schedule.push({
      ...weekData,
      name: `Week ${weekData.weekId} (Split)`,
      phase: 3,
      matchups: [
        // Winners League H2H
        { id: `w${weekData.weekId}_w1`, type: 'h2h', homeTeamId: wPattern[0][0], awayTeamId: wPattern[0][1], label: "Winners League" },
        { id: `w${weekData.weekId}_w2`, type: 'h2h', homeTeamId: wPattern[1][0], awayTeamId: wPattern[1][1], label: "Winners League" },
        
        // Consolation League H2H
        { id: `w${weekData.weekId}_c1`, type: 'h2h', homeTeamId: cPattern[0][0], awayTeamId: cPattern[0][1], label: "Consolation League" },
        { id: `w${weekData.weekId}_c2`, type: 'h2h', homeTeamId: cPattern[1][0], awayTeamId: cPattern[1][1], label: "Consolation League" },
        { id: `w${weekData.weekId}_c3`, type: 'h2h', homeTeamId: cPattern[2][0], awayTeamId: cPattern[2][1], label: "Consolation League" }
      ]
    });
  }

  // -------------------------------------------------------------------
  // PHASE 4: PLAYOFFS (Weeks 24-25)
  // App.jsx resolves the SEED placeholders based on Phase 3 standings
  // -------------------------------------------------------------------
  schedule.push({
    ...SEASON_DATES[23],
    name: "Playoff Semi-Finals",
    phase: 4,
    matchups: [
      { id: 'sf1', type: 'h2h', homeTeamId: 'SEED_1', awayTeamId: 'SEED_4', label: "Semi-Final A" },
      { id: 'sf2', type: 'h2h', homeTeamId: 'SEED_2', awayTeamId: 'SEED_3', label: "Semi-Final B" },
      { id: 'c1',  type: 'h2h', homeTeamId: 'SEED_5', awayTeamId: 'SEED_8', label: "Consolation A" },
      { id: 'c2',  type: 'h2h', homeTeamId: 'SEED_6', awayTeamId: 'SEED_7', label: "Consolation B" }
    ]
  });

  schedule.push({
    ...SEASON_DATES[24],
    name: "Championship Week",
    phase: 4,
    matchups: [
      { id: 'final', type: 'h2h', homeTeamId: 'WINNER_SF1', awayTeamId: 'WINNER_SF2', label: "🏆 Championship" },
      { id: '3rd',   type: 'h2h', homeTeamId: 'LOSER_SF1', awayTeamId: 'LOSER_SF2', label: "3rd Place Match" }
    ]
  });

  return schedule;
};

// 5. DATE-TO-ID MAPPER
export const getPeriodRangeForWeek = (week) => {
  const seasonStart = new Date("2026-03-25");
  const start = new Date(week.startDate);
  const end = new Date(week.endDate);

  const diffTime = start - seasonStart;
  const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24)); 
  
  const startId = Math.max(1, diffDays + 1);
  const duration = Math.ceil((end - start) / (1000 * 60 * 60 * 24)) + 1;
  const endId = startId + duration - 1;

  return { startId, endId };
};

// 6. ID-TO-DATE MAPPER
// Returns "YYYY-MM-DD" so callers can format as needed.
// ProgressionView uses .slice(5) → "MM-DD"; other views render the full string.
export const SEASON_START_DATES = {
  2012: "2012-03-28", 2013: "2013-03-31", 2014: "2014-03-22",
  2015: "2015-04-05", 2016: "2016-04-03", 2017: "2017-04-02",
  2018: "2018-03-29", 2019: "2019-03-28", 2020: "2020-07-23",
  2021: "2021-04-01", 2022: "2022-04-07", 2023: "2023-03-30",
  2024: "2024-03-20", 2025: "2025-03-27", 2026: "2026-03-25",
};

export const getDateFromPeriodId = (periodId, year = 2026) => {
  const startStr = SEASON_START_DATES[year] || "2026-03-25";
  const [sy, sm, sd] = startStr.split('-').map(Number);
  const d = new Date(sy, sm - 1, sd); // local-time constructor avoids UTC offset issues
  d.setDate(d.getDate() + (periodId - 1));
  const mo = String(d.getMonth() + 1).padStart(2, '0');
  const da = String(d.getDate()).padStart(2, '0');
  return `${d.getFullYear()}-${mo}-${da}`;
};