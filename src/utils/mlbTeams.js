// src/utils/mlbTeams.js

export const MLB_TEAMS = [
  // AL East
  { code: 'BAL', name: 'Baltimore Orioles', division: 'AL East', league: 'AL', color: '#DF4601' },
  { code: 'BOS', name: 'Boston Red Sox', division: 'AL East', league: 'AL', color: '#BD3039' },
  { code: 'NYY', name: 'New York Yankees', division: 'AL East', league: 'AL', color: '#0C2340' },
  { code: 'TB',  name: 'Tampa Bay Rays', division: 'AL East', league: 'AL', color: '#092C5C' },
  { code: 'TOR', name: 'Toronto Blue Jays', division: 'AL East', league: 'AL', color: '#134A8E' },

  // AL Central
  { code: 'CWS', name: 'Chicago White Sox', division: 'AL Central', league: 'AL', color: '#27251F' },
  { code: 'CLE', name: 'Cleveland Guardians', division: 'AL Central', league: 'AL', color: '#E31937' },
  { code: 'DET', name: 'Detroit Tigers', division: 'AL Central', league: 'AL', color: '#0C2340' },
  { code: 'KC',  name: 'Kansas City Royals', division: 'AL Central', league: 'AL', color: '#004687' },
  { code: 'MIN', name: 'Minnesota Twins', division: 'AL Central', league: 'AL', color: '#002B5C' },

  // AL West
  { code: 'HOU', name: 'Houston Astros', division: 'AL West', league: 'AL', color: '#002D62' },
  { code: 'LAA', name: 'Los Angeles Angels', division: 'AL West', league: 'AL', color: '#BA0021' },
  { code: 'ATH', name: 'Athletics', division: 'AL West', league: 'AL', color: '#003831' },
  { code: 'SEA', name: 'Seattle Mariners', division: 'AL West', league: 'AL', color: '#0C2340' },
  { code: 'TEX', name: 'Texas Rangers', division: 'AL West', league: 'AL', color: '#003278' },

  // NL East
  { code: 'ATL', name: 'Atlanta Braves', division: 'NL East', league: 'NL', color: '#CE1141' },
  { code: 'MIA', name: 'Miami Marlins', division: 'NL East', league: 'NL', color: '#00A3E0' },
  { code: 'NYM', name: 'New York Mets', division: 'NL East', league: 'NL', color: '#FF5910' },
  { code: 'PHI', name: 'Philadelphia Phillies', division: 'NL East', league: 'NL', color: '#E81828' },
  { code: 'WSH', name: 'Washington Nationals', division: 'NL East', league: 'NL', color: '#AB0003' },

  // NL Central
  { code: 'CHC', name: 'Chicago Cubs', division: 'NL Central', league: 'NL', color: '#0E3386' },
  { code: 'CIN', name: 'Cincinnati Reds', division: 'NL Central', league: 'NL', color: '#C6011F' },
  { code: 'MIL', name: 'Milwaukee Brewers', division: 'NL Central', league: 'NL', color: '#12284B' },
  { code: 'PIT', name: 'Pittsburgh Pirates', division: 'NL Central', league: 'NL', color: '#FDB827' },
  { code: 'STL', name: 'St. Louis Cardinals', division: 'NL Central', league: 'NL', color: '#C41E3A' },

  // NL West
  { code: 'AZ',  name: 'Arizona Diamondbacks', division: 'NL West', league: 'NL', color: '#A71930' },
  { code: 'COL', name: 'Colorado Rockies', division: 'NL West', league: 'NL', color: '#33006F' },
  { code: 'LAD', name: 'Los Angeles Dodgers', division: 'NL West', league: 'NL', color: '#005A9C' },
  { code: 'SD',  name: 'San Diego Padres', division: 'NL West', league: 'NL', color: '#2F241D' },
  { code: 'SF',  name: 'San Francisco Giants', division: 'NL West', league: 'NL', color: '#FD5A1E' }
];

export const MLB_DIVISIONS = [
  'AL East',
  'AL Central',
  'AL West',
  'NL East',
  'NL Central',
  'NL West',
];

export const PICKEM_RULES = {
  division_winner: 3,
  wild_card: 2,
  pennant: 5,
  world_series: 7,
  award: 4,
  win_loss: 3,
  prizes: {
    1: 4,
    2: 3,
    3: 3,
    4: 3,
    5: 1
  }
};

export const LEAGUE_OWNERS = [
  { id: 1, name: 'Tim' },
  { id: 2, name: 'Adrian' },
  { id: 3, name: 'Garrett' },
  { id: 5, name: 'Daniel' },
  { id: 6, name: 'Anil' },
  { id: 8, name: 'Alex' },
  { id: 12, name: 'Will' },
  { id: 13, name: 'Mark' },
  { id: 14, name: 'Preston' },
];

export const PROMINENT_AWARD_CANDIDATES = {
  al_mvp: ['Aaron Judge', 'Bobby Witt Jr.', 'Gunnar Henderson', 'Juan Soto', 'Corey Seager', 'Jose Ramirez', 'Kyle Tucker', 'Yordan Alvarez', 'Adley Rutschman', 'Rafael Devers', 'Vladimir Guerrero Jr.', 'Jarren Duran', 'Julio Rodriguez'],
  nl_mvp: ['Shohei Ohtani', 'Francisco Lindor', 'Bryce Harper', 'Mookie Betts', 'Freddie Freeman', 'Fernando Tatis Jr.', 'Ronald Acuna Jr.', 'Elly De La Cruz', 'Jackson Chourio', 'Corbin Carroll', 'Ketel Marte', 'Austin Riley', 'Manny Machado'],
  al_cy_young: ['Tarik Skubal', 'Corbin Burnes', 'Seth Lugo', 'Cole Ragans', 'Logan Gilbert', 'George Kirby', 'Garrett Crochet', 'Framber Valdez', 'Ronel Blanco', 'Kevin Gausman', 'Tanner Houck', 'Gerrit Cole'],
  nl_cy_young: ['Chris Sale', 'Zack Wheeler', 'Paul Skenes', 'Dylan Cease', 'Logan Webb', 'Shota Imanaga', 'Yoshinobu Yamamoto', 'Freddy Peralta', 'Ranger Suarez', 'Sonny Gray', 'Tyler Glasnow'],
  al_roy: ['Colton Cowser', 'Luis Gil', 'Austin Wells', 'Mason Miller', 'Wilyer Abreu', 'Wyatt Langford', 'Ceddanne Rafaela', 'Jackson Holliday', 'Brooks Lee'],
  nl_roy: ['Paul Skenes', 'Jackson Merrill', 'Jackson Chourio', 'Shota Imanaga', 'Michael Busch', 'Masyn Winn', 'Tyler Fitzgerald', 'Gavin Stone'],
  AL_MVP: ['Aaron Judge', 'Bobby Witt Jr.', 'Gunnar Henderson', 'Juan Soto', 'Corey Seager', 'Jose Ramirez', 'Kyle Tucker', 'Yordan Alvarez', 'Adley Rutschman', 'Rafael Devers', 'Vladimir Guerrero Jr.', 'Jarren Duran', 'Julio Rodriguez'],
  NL_MVP: ['Shohei Ohtani', 'Francisco Lindor', 'Bryce Harper', 'Mookie Betts', 'Freddie Freeman', 'Fernando Tatis Jr.', 'Ronald Acuna Jr.', 'Elly De La Cruz', 'Jackson Chourio', 'Corbin Carroll', 'Ketel Marte', 'Austin Riley', 'Manny Machado'],
  AL_CY: ['Tarik Skubal', 'Corbin Burnes', 'Seth Lugo', 'Cole Ragans', 'Logan Gilbert', 'George Kirby', 'Garrett Crochet', 'Framber Valdez', 'Ronel Blanco', 'Kevin Gausman', 'Tanner Houck', 'Gerrit Cole'],
  NL_CY: ['Chris Sale', 'Zack Wheeler', 'Paul Skenes', 'Dylan Cease', 'Logan Webb', 'Shota Imanaga', 'Yoshinobu Yamamoto', 'Freddy Peralta', 'Ranger Suarez', 'Sonny Gray', 'Tyler Glasnow'],
  AL_ROY: ['Colton Cowser', 'Luis Gil', 'Austin Wells', 'Mason Miller', 'Wilyer Abreu', 'Wyatt Langford', 'Ceddanne Rafaela', 'Jackson Holliday', 'Brooks Lee'],
  NL_ROY: ['Paul Skenes', 'Jackson Merrill', 'Jackson Chourio', 'Shota Imanaga', 'Michael Busch', 'Masyn Winn', 'Tyler Fitzgerald', 'Gavin Stone']
};
