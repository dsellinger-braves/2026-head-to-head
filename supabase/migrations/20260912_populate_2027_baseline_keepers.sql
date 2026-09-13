-- Population of 2027 baseline keepers with Midpoint Token applied
DELETE FROM public.draft_keepers WHERE season_year = 2027;

INSERT INTO public.draft_keepers (
  season_year, owner, team_id, keeper_slot, player_name, espn_player_id, position, mlb_team, rank, cost, token_applied, prior_cost, new_cost, updated_at
) VALUES 
(2027, 'Adrian', 2, 1, 'Juan Soto', '36969', 'OF', 'NYM', 3, 33, false, 33, 33, NOW()),
(2027, 'Adrian', 2, 2, 'Elly De La Cruz', '4917694', 'SS', 'CIN', 10, 25, false, 25, 25, NOW()),
(2027, 'Adrian', 2, 3, 'Logan Gilbert', '41221', 'SP', 'SEA', 30, 16.5, true, 16, 17, NOW()),
(2027, 'Adrian', 2, 4, 'Max Fried', '32685', 'SP', 'NYY', 39, 15, false, 14, 15, NOW()),
(2027, 'Adrian', 2, 5, 'Cole Ragans', '41054', 'SP', 'KC', 47, 13, false, 13, 13, NOW()),
(2027, 'Alex', 8, 1, 'Tarik Skubal', '42409', 'SP', 'DET', 2, 36, false, 36, 36, NOW()),
(2027, 'Alex', 8, 2, 'Cal Raleigh', '41292', 'C, DH', 'SEA', 33, 16, false, 17, 16, NOW()),
(2027, 'Alex', 8, 3, 'Cristopher Sanchez', '42359', 'SP', 'PHI', 17, 20, true, 19, 21, NOW()),
(2027, 'Alex', 8, 4, 'Eugenio Suarez', '32367', '3B', 'CIN', 170, 0, false, 6, 0, NOW()),
(2027, 'Alex', 8, 5, 'Bryce Harper', '30951', '1B', 'PHI', 58, 11, false, 13, 11, NOW()),
(2027, 'Anil', 6, 1, 'Ben Rice', '5016968', 'C, 1B, DH', 'NYY', 209, 0, false, 4, 0, NOW()),
(2027, 'Anil', 6, 2, 'Bryan Woo', '4629089', 'SP', 'SEA', 15, 22, true, 22, 22, NOW()),
(2027, 'Anil', 6, 3, 'Junior Caminero', '4905921', '3B', 'TB', 25, 19, false, 20, 19, NOW()),
(2027, 'Anil', 6, 4, 'Tyler Soderstrom', '4686066', '1B, OF', 'ATH', 190, 0, false, 3, 0, NOW()),
(2027, 'Anil', 6, 5, 'Konnor Griffin', '5218285', 'SS', 'PIT', 137, 3, false, 13, 3, NOW()),
(2027, 'Daniel', 5, 1, 'Shohei Ohtani', '39832', 'DH, SP', 'LAD', 1, 40, false, 40, 40, NOW()),
(2027, 'Daniel', 5, 2, 'Logan Webb', '41216', 'SP', 'SF', 16, 21.5, true, 21, 22, NOW()),
(2027, 'Daniel', 5, 3, 'James Wood', '4918256', 'OF, DH', 'WSH', 26, 18, false, 20, 18, NOW()),
(2027, 'Daniel', 5, 4, 'Corey Seager', '32691', 'SS', 'TEX', 243, 0, false, 0, 0, NOW()),
(2027, 'Daniel', 5, 5, 'Alex Bregman', '34886', '3B', 'CHC', 1272, 0, false, 0, 0, NOW()),
(2027, 'Garrett', 3, 1, 'Ronald Acuna Jr.', '36185', 'OF', 'ATL', 7, 28, false, 29, 28, NOW()),
(2027, 'Garrett', 3, 2, 'Gunnar Henderson', '42507', 'SS', 'BAL', 19, 21, false, 24, 21, NOW()),
(2027, 'Garrett', 3, 3, 'Brent Rooker', '40926', 'OF, DH', 'ATH', 29, 17, false, 18, 17, NOW()),
(2027, 'Garrett', 3, 4, 'Manny Machado', '31097', '3B', 'SD', 169, 0, false, 2, 0, NOW()),
(2027, 'Garrett', 3, 5, 'Nolan McLean', '4433874', 'SP', 'NYM', 1337, 0, true, 0, 0, NOW()),
(2027, 'Mark', 13, 1, 'Paul Skenes', '4719507', 'SP', 'PIT', 5, 30, false, 30, 30, NOW()),
(2027, 'Mark', 13, 2, 'Garrett Crochet', '4297835', 'SP', 'BOS', 6, 28.5, true, 28, 29, NOW()),
(2027, 'Mark', 13, 3, 'Corbin Carroll', '42404', 'OF', 'ARI', 8, 27, false, 27, 27, NOW()),
(2027, 'Mark', 13, 4, 'Kyle Tucker', '34967', 'OF', 'LAD', 12, 23, false, 26, 23, NOW()),
(2027, 'Mark', 13, 5, 'Kevin McGonigle', '5149072', 'SS', 'DET', 1716, 0, false, 0, 0, NOW()),
(2027, 'Preston', 14, 1, 'Aaron Judge', '33192', 'OF, DH', 'NYY', 4, 31, true, 31, 31, NOW()),
(2027, 'Preston', 14, 2, 'Vladimir Guerrero Jr.', '35002', '1B, DH', 'TOR', 13, 23, false, 23, 23, NOW()),
(2027, 'Preston', 14, 3, 'Dylan Cease', '34943', 'SP', 'TOR', 44, 14, false, 14, 14, NOW()),
(2027, 'Preston', 14, 4, 'Trea Turner', '33710', 'SS', 'PHI', 107, 5, false, 11, 5, NOW()),
(2027, 'Preston', 14, 5, 'Maikel Garcia', '4905884', '3B', 'KC', 124, 4, false, 8, 4, NOW()),
(2027, 'Tim', 1, 1, 'Fernando Tatis Jr.', '35983', 'OF', 'SD', 11, 23, true, 23, 23, NOW()),
(2027, 'Tim', 1, 2, 'Yordan Alvarez', '36018', 'OF, DH', 'HOU', 60, 11, false, 11, 11, NOW()),
(2027, 'Tim', 1, 3, 'Nick Kurtz', '4966637', '1B', 'ATH', 20, 20, false, 21, 20, NOW()),
(2027, 'Tim', 1, 4, 'Jazz Chisholm Jr.', '41433', '2B, 3B', 'NYY', 57, 11, false, 15, 11, NOW()),
(2027, 'Tim', 1, 5, 'Pete Alonso', '37498', '1B', 'BAL', 38, 15, false, 17, 15, NOW()),
(2027, 'Will', 12, 1, 'Julio Rodriguez', '41044', 'OF', 'SEA', 14, 23, false, 22, 23, NOW()),
(2027, 'Will', 12, 2, 'Kyle Schwarber', '33712', 'DH', 'PHI', 23, 19, false, 21, 19, NOW()),
(2027, 'Will', 12, 3, 'Jose Ramirez', '32801', '3B, DH', 'CLE', 24, 19, false, 19, 19, NOW()),
(2027, 'Will', 12, 4, 'Wyatt Langford', '4719324', 'OF', 'TEX', 21, 19, true, 18, 20, NOW()),
(2027, 'Will', 12, 5, 'Jacob deGrom', '32796', 'SP', 'TEX', 45, 14, false, 14, 14, NOW());

UPDATE public.draft_team_budgets SET keeper_spend = 102.5, final_budget = 100 - 102.5, updated_at = NOW() WHERE season_year = 2027 AND owner = 'Adrian';
UPDATE public.draft_team_budgets SET keeper_spend = 83, final_budget = 100 - 83, updated_at = NOW() WHERE season_year = 2027 AND owner = 'Alex';
UPDATE public.draft_team_budgets SET keeper_spend = 44, final_budget = 100 - 44, updated_at = NOW() WHERE season_year = 2027 AND owner = 'Anil';
UPDATE public.draft_team_budgets SET keeper_spend = 79.5, final_budget = 100 - 79.5, updated_at = NOW() WHERE season_year = 2027 AND owner = 'Daniel';
UPDATE public.draft_team_budgets SET keeper_spend = 66, final_budget = 100 - 66, updated_at = NOW() WHERE season_year = 2027 AND owner = 'Garrett';
UPDATE public.draft_team_budgets SET keeper_spend = 108.5, final_budget = 100 - 108.5, updated_at = NOW() WHERE season_year = 2027 AND owner = 'Mark';
UPDATE public.draft_team_budgets SET keeper_spend = 77, final_budget = 100 - 77, updated_at = NOW() WHERE season_year = 2027 AND owner = 'Preston';
UPDATE public.draft_team_budgets SET keeper_spend = 80, final_budget = 100 - 80, updated_at = NOW() WHERE season_year = 2027 AND owner = 'Tim';
UPDATE public.draft_team_budgets SET keeper_spend = 94, final_budget = 100 - 94, updated_at = NOW() WHERE season_year = 2027 AND owner = 'Will';
