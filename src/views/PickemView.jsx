// src/views/PickemView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import { MLB_TEAMS, LEAGUE_OWNERS, PROMINENT_AWARD_CANDIDATES } from '../utils/mlbTeams';

export default function PickemView() {
  const [seasons, setSeasons] = useState([]);
  const [selectedSeason, setSelectedSeason] = useState(2026);
  const [activeTab, setActiveTab] = useState('board'); // 'board', 'entry', 'history', 'admin'
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(null);
  const [errorMessage, setErrorMessage] = useState(null);

  // Current Season Data
  const [questions, setQuestions] = useState([]);
  const [picks, setPicks] = useState([]);
  const [scores, setScores] = useState([]);

  // All-time scores for Hall of Fame
  const [allTimeScores, setAllTimeScores] = useState([]);

  // Pick Submission / Edit Form State
  const [entryOwner, setEntryOwner] = useState(LEAGUE_OWNERS[3].name); // default Daniel
  const [entryPicks, setEntryPicks] = useState({});

  // Admin / Grading State
  const [gradingAnswers, setGradingAnswers] = useState({});
  const [newSeasonYear, setNewSeasonYear] = useState(2028);

  // Filter for grid
  const [gridCategoryFilter, setGridCategoryFilter] = useState('ALL');

  // Load Seasons & All-time Scores on Mount
  useEffect(() => {
    fetchInitialData();
  }, []);

  // When selectedSeason changes, fetch that season's details
  useEffect(() => {
    if (selectedSeason) {
      fetchSeasonDetails(selectedSeason);
    }
  }, [selectedSeason]);

  // When entryOwner or picks change, populate entry form if existing
  useEffect(() => {
    if (picks.length > 0 && entryOwner) {
      const ownerPicks = {};
      picks
        .filter(p => p.owner_name?.toLowerCase() === entryOwner?.toLowerCase())
        .forEach(p => {
          const q = questions.find(item => item.id === p.question_id);
          if (q) {
            ownerPicks[q.question_key] = p.pick_value;
          }
        });
      setEntryPicks(ownerPicks);
    } else {
      setEntryPicks({});
    }
  }, [entryOwner, picks, questions]);

  // Populate grading answers when questions load
  useEffect(() => {
    const answers = {};
    questions.forEach(q => {
      answers[q.question_key] = q.correct_answer || q.actual_answer || '';
    });
    setGradingAnswers(answers);
  }, [questions]);

  async function fetchInitialData() {
    try {
      setLoading(true);
      // Fetch seasons
      const { data: sData, error: sErr } = await supabase
        .from('pickem_seasons')
        .select('*')
        .order('season_year', { ascending: false });

      if (sErr) throw sErr;
      if (sData && sData.length > 0) {
        setSeasons(sData);
        const currentActive = sData.find(s => s.season_year === 2026) || sData[0];
        setSelectedSeason(currentActive.season_year);
      }

      // Fetch all scores for Hall of Fame
      const { data: allScores, error: scErr } = await supabase
        .from('pickem_scores')
        .select('*')
        .order('season_year', { ascending: false });

      if (!scErr && allScores) {
        setAllTimeScores(allScores);
      }
    } catch (err) {
      console.error('Error loading pickem initial data:', err);
      setErrorMessage(err.message || 'Failed to load Pickem data');
    } finally {
      setLoading(false);
    }
  }

  async function fetchSeasonDetails(year) {
    try {
      setLoading(true);
      setErrorMessage(null);

      const [qRes, pRes, scRes] = await Promise.all([
        supabase
          .from('pickem_questions')
          .select('*')
          .eq('season_year', year)
          .order('display_order', { ascending: true }),
        supabase
          .from('pickem_picks')
          .select('*')
          .eq('season_year', year),
        supabase
          .from('pickem_scores')
          .select('*')
          .eq('season_year', year)
          .order('place', { ascending: true })
      ]);

      if (qRes.error) throw qRes.error;
      if (pRes.error) throw pRes.error;
      if (scRes.error) throw scRes.error;

      setQuestions(qRes.data || []);
      setPicks(pRes.data || []);
      setScores(scRes.data || []);
    } catch (err) {
      console.error(`Error loading details for ${year}:`, err);
      setErrorMessage(err.message || 'Failed to load season details');
    } finally {
      setLoading(false);
    }
  }

  // Handle Pick Input Change
  const handlePickChange = (questionKey, value) => {
    setEntryPicks(prev => ({
      ...prev,
      [questionKey]: value
    }));
  };

  // Submit / Update Picks in Supabase
  const handleSavePicks = async (e) => {
    if (e) e.preventDefault();
    if (!entryOwner) {
      alert('Please select or specify an owner name');
      return;
    }

    setSaving(true);
    setSaveSuccess(null);
    setErrorMessage(null);

    try {
      const ownerObj = LEAGUE_OWNERS.find(o => o.name.toLowerCase() === entryOwner.toLowerCase());
      const teamId = ownerObj ? ownerObj.id : null;

      const pickRows = [];
      for (const q of questions) {
        const val = entryPicks[q.question_key];
        if (val && val.trim() !== '') {
          pickRows.push({
            season_year: selectedSeason,
            question_id: q.id,
            owner_name: entryOwner,
            team_id: teamId,
            pick_value: val.trim(),
            points_awarded: 0,
            is_correct: false
          });
        }
      }

      if (pickRows.length === 0) {
        throw new Error('Please fill in at least one pick before submitting');
      }

      // Upsert picks into Supabase
      const { error: upsertErr } = await supabase
        .from('pickem_picks')
        .upsert(pickRows, { onConflict: 'season_year,question_id,owner_name' });

      if (upsertErr) throw upsertErr;

      // Ensure an initial record in pickem_scores exists for this owner
      const existingScore = scores.find(s => s.owner_name?.toLowerCase() === entryOwner.toLowerCase());
      if (!existingScore) {
        await supabase
          .from('pickem_scores')
          .upsert({
            season_year: selectedSeason,
            owner_name: entryOwner,
            team_id: teamId,
            total_points: 0,
            place: scores.length + 1,
            budget_awarded: null,
            category_scores: {}
          }, { onConflict: 'season_year,owner_name' });
      }

      setSaveSuccess(`Picks successfully saved for ${entryOwner} in ${selectedSeason}! 🎉`);
      await fetchSeasonDetails(selectedSeason);
      setTimeout(() => setSaveSuccess(null), 5000);
    } catch (err) {
      console.error('Save picks failed:', err);
      setErrorMessage(err.message || 'Failed to save picks');
    } finally {
      setSaving(false);
    }
  };

  // Admin: Save Actual Answers & Grade Season
  const handleGradeSeason = async () => {
    setSaving(true);
    try {
      // 1. Update questions correct_answer
      for (const q of questions) {
        const actual = gradingAnswers[q.question_key] || null;
        if (actual !== q.correct_answer) {
          await supabase
            .from('pickem_questions')
            .update({ correct_answer: actual })
            .eq('id', q.id);
        }
      }

      // 2. Fetch updated questions & picks
      const { data: updatedQuestions } = await supabase
        .from('pickem_questions')
        .select('*')
        .eq('season_year', selectedSeason);

      const { data: curPicks } = await supabase
        .from('pickem_picks')
        .select('*')
        .eq('season_year', selectedSeason);

      // 3. Compute score for each pick
      const updatedPicks = [];
      const ownerTotals = {};

      const qMap = {};
      updatedQuestions.forEach(q => { qMap[q.id] = q; });

      // Build playoff pool for crossover checking
      const alPlayoffs = new Set();
      const nlPlayoffs = new Set();
      updatedQuestions.forEach(q => {
        if (q.correct_answer) {
          const ans = q.correct_answer.toLowerCase().trim();
          if (q.question_key.startsWith('al_')) alPlayoffs.add(ans);
          if (q.question_key.startsWith('nl_')) nlPlayoffs.add(ans);
        }
      });

      for (const p of curPicks) {
        const q = qMap[p.question_id];
        if (!q) continue;

        let isCorrect = false;
        let points = 0;

        const pVal = p.pick_value?.toLowerCase().trim() || '';
        const aVal = q.correct_answer?.toLowerCase().trim() || '';

        if (aVal && pVal) {
          if (pVal === aVal || aVal.includes(pVal) || pVal.includes(aVal)) {
            isCorrect = true;
            points = q.points_exact || 3;
          } else if (q.category === 'wild_card' || q.category === 'division') {
            // Check crossover: if picked team made playoffs
            const inAL = alPlayoffs.has(pVal);
            const inNL = nlPlayoffs.has(pVal);
            if ((q.question_key.startsWith('al_') && inAL) || (q.question_key.startsWith('nl_') && inNL)) {
              isCorrect = false;
              points = q.points_partial || 2; // Crossover wild card value
            }
          }
        }

        updatedPicks.push({
          id: p.id,
          season_year: selectedSeason,
          question_id: p.question_id,
          owner_name: p.owner_name,
          team_id: p.team_id,
          pick_value: p.pick_value,
          points_awarded: points,
          is_correct: isCorrect
        });

        if (!ownerTotals[p.owner_name]) {
          ownerTotals[p.owner_name] = { total: 0, teamId: p.team_id, breakdown: {} };
        }
        ownerTotals[p.owner_name].total += points;
      }

      // Batch update picks
      await supabase.from('pickem_picks').upsert(updatedPicks);

      // Rank owners & determine prize money
      const sortedOwners = Object.entries(ownerTotals).sort((a, b) => b[1].total - a[1].total);
      const scoreRows = sortedOwners.map(([name, data], idx) => {
        const place = idx + 1;
        let prize = null;
        if (place === 1) prize = 4;
        else if (place >= 2 && place <= 4) prize = 3;
        else if (place === 5) prize = 1;

        return {
          season_year: selectedSeason,
          owner_name: name,
          team_id: data.teamId,
          total_points: data.total,
          place,
          budget_awarded: prize,
          category_scores: data.breakdown
        };
      });

      if (scoreRows.length > 0) {
        await supabase.from('pickem_scores').upsert(scoreRows, { onConflict: 'season_year,owner_name' });
      }

      setSaveSuccess(`Season ${selectedSeason} successfully graded and scores recalculated! 🏆`);
      await fetchSeasonDetails(selectedSeason);
      setTimeout(() => setSaveSuccess(null), 5000);
    } catch (err) {
      console.error('Grading failed:', err);
      setErrorMessage(err.message || 'Failed to grade season');
    } finally {
      setSaving(false);
    }
  };

  // Admin: Bootstrap New Season
  const handleBootstrapSeason = async () => {
    if (!newSeasonYear || isNaN(newSeasonYear)) return;
    setSaving(true);
    try {
      const yearInt = parseInt(newSeasonYear);
      // 1. Create season
      await supabase.from('pickem_seasons').upsert({
        season_year: yearInt,
        status: 'open'
      });

      // 2. Standard 23 questions definition
      const standardQuestions = [
        { key: 'nl_east', label: 'NL East', cat: 'division', exact: 3, partial: 2, ord: 1 },
        { key: 'nl_central', label: 'NL Central', cat: 'division', exact: 3, partial: 2, ord: 2 },
        { key: 'nl_west', label: 'NL West', cat: 'division', exact: 3, partial: 2, ord: 3 },
        { key: 'nl_wc_1', label: 'NL Wild Card 1', cat: 'wild_card', exact: 2, partial: 2, ord: 4 },
        { key: 'nl_wc_2', label: 'NL Wild Card 2', cat: 'wild_card', exact: 2, partial: 2, ord: 5 },
        { key: 'nl_wc_3', label: 'NL Wild Card 3', cat: 'wild_card', exact: 2, partial: 2, ord: 6 },
        { key: 'al_east', label: 'AL East', cat: 'division', exact: 3, partial: 2, ord: 7 },
        { key: 'al_central', label: 'AL Central', cat: 'division', exact: 3, partial: 2, ord: 8 },
        { key: 'al_west', label: 'AL West', cat: 'division', exact: 3, partial: 2, ord: 9 },
        { key: 'al_wc_1', label: 'AL Wild Card 1', cat: 'wild_card', exact: 2, partial: 2, ord: 10 },
        { key: 'al_wc_2', label: 'AL Wild Card 2', cat: 'wild_card', exact: 2, partial: 2, ord: 11 },
        { key: 'al_wc_3', label: 'AL Wild Card 3', cat: 'wild_card', exact: 2, partial: 2, ord: 12 },
        { key: 'nl_pennant', label: 'NL Pennant', cat: 'playoff_result', exact: 5, partial: 0, ord: 13 },
        { key: 'al_pennant', label: 'AL Pennant', cat: 'playoff_result', exact: 5, partial: 0, ord: 14 },
        { key: 'world_series', label: 'World Series Winner', cat: 'playoff_result', exact: 7, partial: 0, ord: 15 },
        { key: 'nl_mvp', label: 'NL MVP', cat: 'award', exact: 4, partial: 0, ord: 16 },
        { key: 'nl_cy_young', label: 'NL Cy Young', cat: 'award', exact: 4, partial: 0, ord: 17 },
        { key: 'nl_roy', label: 'NL Rookie of the Year', cat: 'award', exact: 4, partial: 0, ord: 18 },
        { key: 'al_mvp', label: 'AL MVP', cat: 'award', exact: 4, partial: 0, ord: 19 },
        { key: 'al_cy_young', label: 'AL Cy Young', cat: 'award', exact: 4, partial: 0, ord: 20 },
        { key: 'al_roy', label: 'AL Rookie of the Year', cat: 'award', exact: 4, partial: 0, ord: 21 },
        { key: 'most_wins', label: 'Most Team Wins (reg. season)', cat: 'extremes', exact: 3, partial: 0, ord: 22 },
        { key: 'most_losses', label: 'Most Team Losses (reg. season)', cat: 'extremes', exact: 3, partial: 0, ord: 23 },
      ];

      const qRows = standardQuestions.map(q => ({
        season_year: yearInt,
        question_key: q.key,
        question_label: q.label,
        category: q.cat,
        options_type: q.cat === 'award' ? 'player_text' : 'mlb_team',
        display_order: q.ord,
        points_exact: q.exact,
        points_partial: q.partial
      }));

      await supabase.from('pickem_questions').upsert(qRows, { onConflict: 'season_year,question_key' });

      setSaveSuccess(`Successfully bootstrapped Season ${yearInt}! 🚀`);
      await fetchInitialData();
      setSelectedSeason(yearInt);
      setActiveTab('entry');
    } catch (err) {
      console.error('Bootstrap failed:', err);
      setErrorMessage(err.message || 'Failed to bootstrap season');
    } finally {
      setSaving(false);
    }
  };

  // Determine distinct owners who have picks or scores this season
  const seasonOwners = useMemo(() => {
    const map = new Map();
    // From scores first
    scores.forEach(s => {
      map.set(s.owner_name.toLowerCase(), { name: s.owner_name, team_id: s.team_id });
    });
    // From picks next
    picks.forEach(p => {
      const k = p.owner_name.toLowerCase();
      if (!map.has(k)) {
        map.set(k, { name: p.owner_name, team_id: p.team_id });
      }
    });
    return Array.from(map.values());
  }, [scores, picks]);

  // Map of question_id -> { [owner_name]: pickObj }
  const pickMatrix = useMemo(() => {
    const matrix = {};
    picks.forEach(p => {
      if (!matrix[p.question_id]) matrix[p.question_id] = {};
      matrix[p.question_id][p.owner_name.toLowerCase()] = p;
    });
    return matrix;
  }, [picks]);

  // Filtered questions
  const filteredQuestions = useMemo(() => {
    if (gridCategoryFilter === 'ALL') return questions;
    return questions.filter(q => q.category === gridCategoryFilter);
  }, [questions, gridCategoryFilter]);

  // Hall of Fame Aggregates
  const hallOfFame = useMemo(() => {
    const records = {};
    allTimeScores.forEach(s => {
      const name = s.owner_name;
      if (!records[name]) {
        records[name] = {
          name,
          titles: 0,
          top3: 0,
          totalPrizes: 0,
          totalPoints: 0,
          seasonsCount: 0,
          bestScore: 0,
          bestYear: null
        };
      }
      const r = records[name];
      const place = s.place || 99;
      const budget = s.budget_awarded || 0;
      r.seasonsCount += 1;
      r.totalPoints += (s.total_points || 0);
      r.totalPrizes += budget;
      if (place === 1) r.titles += 1;
      if (place <= 3) r.top3 += 1;
      if ((s.total_points || 0) > r.bestScore) {
        r.bestScore = s.total_points;
        r.bestYear = s.season_year;
      }
    });

    return Object.values(records).sort((a, b) => {
      if (b.titles !== a.titles) return b.titles - a.titles;
      if (b.totalPrizes !== a.totalPrizes) return b.totalPrizes - a.totalPrizes;
      return b.totalPoints - a.totalPoints;
    });
  }, [allTimeScores]);

  const currentSeasonObj = seasons.find(s => s.season_year === selectedSeason);
  const isSummaryEra = selectedSeason <= 2018;
  const isUpcoming = currentSeasonObj?.status === 'open' || currentSeasonObj?.status === 'upcoming';
  const isInProgress = currentSeasonObj?.status === 'in_progress' || selectedSeason === 2026;

  // Category labels helper
  const getCategoryLabel = (cat) => {
    switch (cat) {
      case 'division': return 'Division';
      case 'wild_card': return 'Wild Card';
      case 'playoff_result': return 'Playoffs / WS';
      case 'award': return 'Awards';
      case 'extremes': return 'Win / Loss Extremes';
      default: return cat;
    }
  };

  return (
    <div className="space-y-6 pb-12">
      {/* HEADER BANNER */}
      <div className="bg-gradient-to-r from-slate-900 via-indigo-950 to-slate-900 border border-indigo-800/40 rounded-2xl p-6 shadow-xl relative overflow-hidden">
        <div className="absolute -right-12 -bottom-12 w-64 h-64 bg-indigo-500/10 rounded-full blur-3xl pointer-events-none"></div>
        <div className="absolute -left-12 -top-12 w-64 h-64 bg-purple-500/10 rounded-full blur-3xl pointer-events-none"></div>

        <div className="relative z-10 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <div className="flex items-center gap-3">
              <span className="text-3xl">🔮</span>
              <h1 className="text-2xl sm:text-3xl font-black tracking-tight text-white">
                Annual MLB Pick'em Championship
              </h1>
            </div>
            <p className="text-indigo-200/80 text-sm mt-1 max-w-2xl">
              League forecasting war room. Predict division champions, wild cards, pennants, World Series, award winners, and extreme win totals to earn auction draft budget cash!
            </p>
          </div>

          {/* Quick Season Switcher */}
          <div className="flex items-center gap-3 bg-slate-950/70 p-2 rounded-xl border border-indigo-900/60 shadow-inner">
            <span className="text-xs font-bold uppercase tracking-wider text-indigo-300 pl-2">Season:</span>
            <select
              value={selectedSeason}
              onChange={(e) => setSelectedSeason(parseInt(e.target.value))}
              className="bg-indigo-950 text-white font-bold text-sm px-3 py-1.5 rounded-lg border border-indigo-700/60 focus:outline-none focus:ring-2 focus:ring-indigo-400 cursor-pointer"
            >
              {seasons.map(s => (
                <option key={s.season_year} value={s.season_year}>
                  {s.season_year} {s.season_year === 2027 ? '🚀 (Open Entry)' : s.season_year === 2026 ? '⚡ (Live)' : '🏆 (Final)'}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Season Badges & Rule Summary */}
        <div className="relative z-10 mt-5 pt-4 border-t border-indigo-800/30 flex flex-wrap items-center justify-between gap-3 text-xs">
          <div className="flex items-center gap-2">
            <span className="font-semibold text-slate-300">Status:</span>
            {isUpcoming ? (
              <span className="px-2.5 py-1 rounded-full bg-emerald-950/80 border border-emerald-500/40 text-emerald-300 font-bold flex items-center gap-1.5 animate-pulse">
                <span className="w-2 h-2 rounded-full bg-emerald-400"></span> Open for Owner Entries
              </span>
            ) : isInProgress ? (
              <span className="px-2.5 py-1 rounded-full bg-amber-950/80 border border-amber-500/40 text-amber-300 font-bold flex items-center gap-1.5">
                <span className="w-2 h-2 rounded-full bg-amber-400"></span> 2026 Live In-Progress
              </span>
            ) : (
              <span className="px-2.5 py-1 rounded-full bg-indigo-950/80 border border-indigo-500/40 text-indigo-300 font-bold">
                ✓ Season Completed & Settled
              </span>
            )}
            <span className="text-slate-500 mx-1">•</span>
            <span className="text-slate-400">Questions:</span>
            <span className="text-slate-200 font-medium">
              {questions.length || 23} Categories
            </span>
          </div>

          {/* Scoring Quick Legend */}
          <div className="flex flex-wrap items-center gap-2 sm:gap-3 text-slate-300">
            <span className="text-slate-400 font-medium">Scoring:</span>
            <span className="px-2 py-0.5 rounded bg-emerald-950/60 border border-emerald-500/30 text-emerald-300 font-semibold">Division: 3 pts</span>
            <span className="px-2 py-0.5 rounded bg-sky-950/60 border border-sky-500/30 text-sky-300 font-semibold">Wild Card / Playoff: 2 pts</span>
            <span className="px-2 py-0.5 rounded bg-purple-950/60 border border-purple-500/30 text-purple-300 font-semibold">Pennant: 5 pts</span>
            <span className="px-2 py-0.5 rounded bg-amber-950/60 border border-amber-500/30 text-amber-300 font-semibold">World Series: 7 pts</span>
            <span className="px-2 py-0.5 rounded bg-pink-950/60 border border-pink-500/30 text-pink-300 font-semibold">Award: 4 pts</span>
          </div>
        </div>
      </div>

      {/* TOAST / ALERTS */}
      {saveSuccess && (
        <div className="bg-emerald-900/60 border border-emerald-500/50 text-emerald-200 p-4 rounded-xl flex items-center gap-3 shadow-lg">
          <span className="text-2xl">🎉</span>
          <div className="font-semibold text-sm">{saveSuccess}</div>
        </div>
      )}
      {errorMessage && (
        <div className="bg-rose-950/60 border border-rose-500/50 text-rose-200 p-4 rounded-xl flex items-center gap-3 shadow-lg">
          <span className="text-2xl">⚠️</span>
          <div className="font-semibold text-sm">{errorMessage}</div>
        </div>
      )}

      {/* NAVIGATION TABS */}
      <div className="flex items-center justify-between border-b border-slate-700/60 pb-3">
        <div className="flex items-center gap-2 overflow-x-auto pb-1">
          <button
            onClick={() => setActiveTab('board')}
            className={`px-4 py-2 rounded-xl text-sm font-bold flex items-center gap-2 transition-all cursor-pointer ${
              activeTab === 'board'
                ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30 ring-2 ring-indigo-400'
                : 'bg-slate-800/80 text-slate-300 hover:bg-slate-700'
            }`}
          >
            <span>📊</span>
            <span>Leaderboard & Picks</span>
          </button>

          <button
            onClick={() => setActiveTab('entry')}
            className={`px-4 py-2 rounded-xl text-sm font-bold flex items-center gap-2 transition-all cursor-pointer ${
              activeTab === 'entry'
                ? 'bg-emerald-600 text-white shadow-md shadow-emerald-600/30 ring-2 ring-emerald-400'
                : 'bg-slate-800/80 text-slate-300 hover:bg-slate-700'
            }`}
          >
            <span>✍️</span>
            <span>Submit / Edit Picks</span>
            {selectedSeason === 2027 && <span className="w-2 h-2 rounded-full bg-emerald-300 animate-ping"></span>}
          </button>

          <button
            onClick={() => setActiveTab('history')}
            className={`px-4 py-2 rounded-xl text-sm font-bold flex items-center gap-2 transition-all cursor-pointer ${
              activeTab === 'history'
                ? 'bg-amber-600 text-white shadow-md shadow-amber-600/30 ring-2 ring-amber-400'
                : 'bg-slate-800/80 text-slate-300 hover:bg-slate-700'
            }`}
          >
            <span>🏛️</span>
            <span>Hall of Fame (2016-2026)</span>
          </button>

          <button
            onClick={() => setActiveTab('admin')}
            className={`px-4 py-2 rounded-xl text-sm font-bold flex items-center gap-2 transition-all cursor-pointer ${
              activeTab === 'admin'
                ? 'bg-purple-600 text-white shadow-md shadow-purple-600/30 ring-2 ring-purple-400'
                : 'bg-slate-800/80 text-slate-300 hover:bg-slate-700'
            }`}
          >
            <span>⚙️</span>
            <span>Commissioner Tools</span>
          </button>
        </div>

        {/* Total picks indicator */}
        <div className="hidden sm:flex items-center gap-2 text-xs font-medium text-slate-400">
          <span>{picks.length} picks</span>
          <span>•</span>
          <span>{scores.length || seasonOwners.length} owners</span>
        </div>
      </div>

      {loading ? (
        <div className="flex flex-col items-center justify-center py-24 space-y-4">
          <div className="w-12 h-12 border-4 border-indigo-500 border-t-transparent rounded-full animate-spin"></div>
          <p className="text-slate-400 font-medium text-sm">Loading Pick'em war room data...</p>
        </div>
      ) : (
        <>
          {/* ========================================================= */}
          {/* TAB 1: BOARD (LEADERBOARD + MASTER PICKS GRID)           */}
          {/* ========================================================= */}
          {activeTab === 'board' && (
            <div className="space-y-8">
              {/* PODIUM & STANDINGS CARD */}
              <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 border-b border-slate-800 gap-2">
                  <div>
                    <h2 className="text-xl font-bold text-white flex items-center gap-2">
                      <span>🏆</span> {selectedSeason} Standings & Budget Prizes
                    </h2>
                    <p className="text-xs text-slate-400 mt-0.5">
                      Top 5 finishers earn additional auction draft dollars for next season's draft: 1st (+$4), 2nd-4th (+$3), 5th (+$1).
                    </p>
                  </div>
                  {isUpcoming && selectedSeason === 2027 && (
                    <span className="text-xs bg-emerald-950 text-emerald-300 px-3 py-1 rounded-full border border-emerald-800/60 font-semibold self-start">
                      Picks open – scores pending season results
                    </span>
                  )}
                </div>

                {scores.length > 0 ? (
                  <>
                    {/* TOP 3 PODIUM */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 my-6">
                      {/* 2nd Place */}
                      <div className="order-2 md:order-1 bg-gradient-to-b from-slate-800/60 to-slate-900/90 border border-slate-700/60 rounded-xl p-4 text-center flex flex-col items-center justify-center relative">
                        <span className="text-3xl mb-1">🥈</span>
                        <span className="text-xs uppercase font-bold tracking-widest text-slate-400">2nd Place</span>
                        <div className="text-lg font-black text-white mt-1">
                          {scores[1]?.owner_name || '—'}
                        </div>
                        <div className="text-2xl font-black text-indigo-300 mt-0.5">
                          {scores[1]?.total_points ?? 0} <span className="text-xs text-slate-400 font-normal">pts</span>
                        </div>
                        {(scores[1]?.budget_awarded || 0) > 0 && (
                          <span className="mt-2 text-xs font-extrabold px-2.5 py-0.5 rounded-full bg-emerald-950 border border-emerald-500/40 text-emerald-300">
                            +${scores[1].budget_awarded} Draft Budget
                          </span>
                        )}
                      </div>

                      {/* 1st Place (Champion) */}
                      <div className="order-1 md:order-2 bg-gradient-to-b from-amber-950/40 via-slate-800/90 to-slate-900 border-2 border-amber-500/60 rounded-xl p-5 text-center flex flex-col items-center justify-center relative shadow-lg shadow-amber-950/20 transform md:-translate-y-2">
                        <div className="absolute -top-3 bg-amber-500 text-slate-950 text-[10px] uppercase font-black px-3 py-0.5 rounded-full tracking-wider shadow">
                          CHAMPION
                        </div>
                        <span className="text-4xl mb-1 mt-1">🥇</span>
                        <span className="text-xs uppercase font-bold tracking-widest text-amber-400">1st Place</span>
                        <div className="text-xl font-black text-white mt-1">
                          {scores[0]?.owner_name || '—'}
                        </div>
                        <div className="text-3xl font-black text-amber-300 mt-0.5">
                          {scores[0]?.total_points ?? 0} <span className="text-xs text-slate-400 font-normal">pts</span>
                        </div>
                        {(scores[0]?.budget_awarded || 0) > 0 && (
                          <span className="mt-2 text-xs font-black px-3 py-1 rounded-full bg-amber-400 text-slate-950 shadow">
                            +${scores[0].budget_awarded} Draft Budget
                          </span>
                        )}
                      </div>

                      {/* 3rd Place */}
                      <div className="order-3 bg-gradient-to-b from-amber-950/20 to-slate-900/90 border border-amber-900/40 rounded-xl p-4 text-center flex flex-col items-center justify-center relative">
                        <span className="text-3xl mb-1">🥉</span>
                        <span className="text-xs uppercase font-bold tracking-widest text-amber-600">3rd Place</span>
                        <div className="text-lg font-black text-white mt-1">
                          {scores[2]?.owner_name || '—'}
                        </div>
                        <div className="text-2xl font-black text-indigo-300 mt-0.5">
                          {scores[2]?.total_points ?? 0} <span className="text-xs text-slate-400 font-normal">pts</span>
                        </div>
                        {(scores[2]?.budget_awarded || 0) > 0 && (
                          <span className="mt-2 text-xs font-extrabold px-2.5 py-0.5 rounded-full bg-emerald-950 border border-emerald-500/40 text-emerald-300">
                            +${scores[2].budget_awarded} Draft Budget
                          </span>
                        )}
                      </div>
                    </div>

                    {/* FULL STANDINGS TABLE */}
                    <div className="overflow-x-auto mt-4">
                      <table className="w-full text-left text-sm border-collapse">
                        <thead>
                          <tr className="border-b border-slate-800 text-xs uppercase font-bold text-slate-400">
                            <th className="py-2.5 px-3">Rank</th>
                            <th className="py-2.5 px-3">Owner</th>
                            <th className="py-2.5 px-3 text-right">Total Points</th>
                            <th className="py-2.5 px-3 text-center">Prize Money</th>
                            {isSummaryEra ? (
                              <>
                                <th className="py-2.5 px-3 text-right text-xs">NL Play</th>
                                <th className="py-2.5 px-3 text-right text-xs">AL Play</th>
                                <th className="py-2.5 px-3 text-right text-xs">Postseason</th>
                                <th className="py-2.5 px-3 text-right text-xs">NL Award</th>
                                <th className="py-2.5 px-3 text-right text-xs">AL Award</th>
                                <th className="py-2.5 px-3 text-right text-xs">Wins/Loss</th>
                              </>
                            ) : (
                              <th className="py-2.5 px-3 text-slate-400">Performance Status</th>
                            )}
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-800/60">
                          {scores.map((sc) => {
                            const place = sc.place || 99;
                            const isPodium = place <= 3;
                            const budget = sc.budget_awarded || 0;
                            const cats = sc.category_scores || {};

                            return (
                              <tr
                                key={sc.id || sc.owner_name}
                                className={`hover:bg-slate-800/40 transition-colors ${
                                  place === 1 ? 'bg-amber-950/10' : isPodium ? 'bg-indigo-950/10' : ''
                                }`}
                              >
                                <td className="py-2.5 px-3 font-extrabold">
                                  {place === 1 ? '🥇 1' : place === 2 ? '🥈 2' : place === 3 ? '🥉 3' : `#${place}`}
                                </td>
                                <td className="py-2.5 px-3 font-bold text-white flex items-center gap-2">
                                  <span>{sc.owner_name}</span>
                                  {sc.team_id && (
                                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-slate-800 text-indigo-300 font-normal">
                                      Team {sc.team_id}
                                    </span>
                                  )}
                                </td>
                                <td className="py-2.5 px-3 text-right font-black text-indigo-300 text-base">
                                  {sc.total_points ?? 0}
                                </td>
                                <td className="py-2.5 px-3 text-center">
                                  {budget > 0 ? (
                                    <span className="px-2.5 py-1 rounded-full text-xs font-black bg-emerald-950/80 border border-emerald-500/50 text-emerald-300 inline-block shadow-xs">
                                      +${budget}
                                    </span>
                                  ) : (
                                    <span className="text-slate-600 text-xs font-medium">—</span>
                                  )}
                                </td>

                                {isSummaryEra ? (
                                  <>
                                    <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.nl_playoffs ?? cats['NL Playoffs'] ?? '—'}</td>
                                    <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.al_playoffs ?? cats['AL Playoffs'] ?? '—'}</td>
                                    <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.playoff_result ?? cats['Playoff Result'] ?? '—'}</td>
                                    <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.nl_award ?? cats['NL Award'] ?? '—'}</td>
                                    <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats.al_award ?? cats['AL Award'] ?? '—'}</td>
                                    <td className="py-2.5 px-3 text-right text-xs text-slate-300 font-mono">{cats['most_wins/losses'] ?? cats['Most Wins/Losses'] ?? '—'}</td>
                                  </>
                                ) : (
                                  <td className="py-2.5 px-3 text-xs text-slate-400">
                                    {place === 1 ? '🏆 Champion' : place <= 4 ? '✨ In the Money' : place === 5 ? '🎯 Budget Cash' : 'Out of the Money'}
                                  </td>
                                )}
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </>
                ) : (
                  <div className="text-center py-10 text-slate-400">
                    <p className="text-sm">No scores recorded yet for {selectedSeason}.</p>
                    <button
                      onClick={() => setActiveTab('entry')}
                      className="mt-3 px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold rounded-lg transition-colors cursor-pointer"
                    >
                      Submit picks for this season →
                    </button>
                  </div>
                )}
              </div>

              {/* MASTER PICK MATRIX (GRID) */}
              {!isSummaryEra && (
                <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
                  <div className="flex flex-col md:flex-row md:items-center justify-between pb-4 border-b border-slate-800 gap-3">
                    <div>
                      <h2 className="text-xl font-bold text-white flex items-center gap-2">
                        <span>📋</span> {selectedSeason} Master Pick Matrix
                      </h2>
                      <p className="text-xs text-slate-400 mt-0.5">
                        Side-by-side comparison of every owner's predictions and scoring results.
                      </p>
                    </div>

                    {/* Filter by category */}
                    <div className="flex items-center gap-1.5 overflow-x-auto text-xs">
                      {['ALL', 'division', 'wild_card', 'playoff_result', 'award', 'extremes'].map(cat => (
                        <button
                          key={cat}
                          onClick={() => setGridCategoryFilter(cat)}
                          className={`px-2.5 py-1 rounded-lg font-medium whitespace-nowrap transition-colors cursor-pointer ${
                            gridCategoryFilter === cat
                              ? 'bg-indigo-600 text-white font-bold'
                              : 'bg-slate-800 text-slate-300 hover:bg-slate-700'
                          }`}
                        >
                          {cat === 'ALL' ? 'All Questions' : getCategoryLabel(cat)}
                        </button>
                      ))}
                    </div>
                  </div>

                  {/* Pick Matrix Table */}
                  <div className="overflow-x-auto mt-4 max-h-[750px] overflow-y-auto scrollbar-thin">
                    <table className="w-full text-left text-xs border-collapse">
                      <thead className="sticky top-0 bg-slate-950 z-20 shadow-md">
                        <tr className="border-b border-slate-800 text-slate-300 font-bold uppercase tracking-wider">
                          <th className="py-3 px-3 min-w-[200px]">Question</th>
                          <th className="py-3 px-3 min-w-[130px] bg-slate-900/90 text-emerald-300 border-x border-slate-800">
                            Actual Outcome
                          </th>
                          {seasonOwners.map(o => (
                            <th key={o.name} className="py-3 px-3 min-w-[140px] text-center">
                              <span className="font-extrabold text-white text-sm block">{o.name}</span>
                              {o.team_id && <span className="text-[10px] text-slate-400 font-normal">Team {o.team_id}</span>}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-800/50">
                        {filteredQuestions.map(q => {
                          const outcome = q.correct_answer || q.actual_answer;
                          const hasActual = outcome && outcome.trim() !== '';

                          return (
                            <tr key={q.id} className="hover:bg-slate-800/30 transition-colors">
                              {/* Question Column */}
                              <td className="py-2.5 px-3">
                                <div className="font-bold text-white text-sm">{q.question_label || q.question_text}</div>
                                <div className="flex items-center gap-2 mt-0.5">
                                  <span className="text-[10px] px-1.5 py-0.2 rounded bg-indigo-950/80 border border-indigo-700/50 text-indigo-300 font-semibold">
                                    {getCategoryLabel(q.category)}
                                  </span>
                                  <span className="text-[10px] text-slate-400 font-medium">
                                    {q.points_exact || 3} pts
                                  </span>
                                </div>
                              </td>

                              {/* Actual Answer Column */}
                              <td className="py-2.5 px-3 bg-slate-950/40 border-x border-slate-800">
                                {hasActual ? (
                                  <span className="font-bold text-emerald-300 bg-emerald-950/80 px-2 py-1 rounded border border-emerald-600/40 block text-center">
                                    {outcome}
                                  </span>
                                ) : (
                                  <span className="text-slate-600 italic block text-center">Pending</span>
                                )}
                              </td>

                              {/* Owner Pick Columns */}
                              {seasonOwners.map(o => {
                                const pick = pickMatrix[q.id]?.[o.name.toLowerCase()];
                                const pickVal = pick?.pick_value;

                                if (!pickVal) {
                                  return (
                                    <td key={o.name} className="py-2.5 px-3 text-center text-slate-600 italic">
                                      —
                                    </td>
                                  );
                                }

                                const isExact = pick?.is_correct;
                                const pts = pick?.points_awarded ?? pick?.points_earned ?? 0;
                                const isCrossover = !isExact && pts > 0;

                                return (
                                  <td key={o.name} className="py-2.5 px-3 text-center">
                                    <div
                                      className={`p-2 rounded-lg border transition-all text-xs ${
                                        isExact
                                          ? 'bg-emerald-950/70 border-emerald-500/50 text-emerald-200 font-bold shadow-xs'
                                          : isCrossover
                                          ? 'bg-sky-950/70 border-sky-500/50 text-sky-200 font-semibold'
                                          : hasActual
                                          ? 'bg-slate-950/40 border-slate-800 text-slate-400 line-through opacity-75'
                                          : 'bg-slate-800/40 border-slate-700/60 text-slate-200 font-medium'
                                      }`}
                                    >
                                      <div>{pickVal}</div>
                                      {isExact && (
                                        <div className="text-[10px] text-emerald-400 font-black mt-0.5">
                                          ✓ +{pts} pts
                                        </div>
                                      )}
                                      {isCrossover && (
                                        <div className="text-[10px] text-sky-400 font-black mt-0.5">
                                          ✦ +{pts} pts (WC)
                                        </div>
                                      )}
                                    </div>
                                  </td>
                                );
                              })}
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* ========================================================= */}
          {/* TAB 2: SUBMIT / EDIT PICKS FORM                          */}
          {/* ========================================================= */}
          {activeTab === 'entry' && (
            <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-6">
              <div className="border-b border-slate-800 pb-4 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                <div>
                  <h2 className="text-xl font-bold text-white flex items-center gap-2">
                    <span>✍️</span> Submit & Edit Picks for {selectedSeason}
                  </h2>
                  <p className="text-xs text-slate-400 mt-1">
                    Select your owner profile and make your official predictions for the season. You can revisit and update your picks anytime before the season lock!
                  </p>
                </div>

                {/* Owner selector for entry */}
                <div className="flex items-center gap-2">
                  <label className="text-xs font-bold text-slate-300">Owner:</label>
                  <select
                    value={entryOwner}
                    onChange={(e) => setEntryOwner(e.target.value)}
                    className="bg-slate-800 text-white font-bold text-sm px-3 py-1.5 rounded-lg border border-indigo-700/60 focus:ring-2 focus:ring-indigo-400 cursor-pointer"
                  >
                    {LEAGUE_OWNERS.map(o => (
                      <option key={o.id} value={o.name}>
                        {o.name} (Team {o.id})
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              <form onSubmit={handleSavePicks} className="space-y-8">
                {/* 1. DIVISION CHAMPIONS */}
                <div>
                  <h3 className="text-base font-bold text-indigo-300 flex items-center gap-2 pb-2 border-b border-slate-800">
                    <span>⚾</span> Division Champions (3 pts each)
                  </h3>
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-3">
                    {questions
                      .filter(q => q.category === 'division')
                      .map(q => {
                        let divFilter = null;
                        const key = q.question_key.toLowerCase();
                        if (key.includes('al_east')) divFilter = 'AL East';
                        if (key.includes('al_central')) divFilter = 'AL Central';
                        if (key.includes('al_west')) divFilter = 'AL West';
                        if (key.includes('nl_east')) divFilter = 'NL East';
                        if (key.includes('nl_central')) divFilter = 'NL Central';
                        if (key.includes('nl_west')) divFilter = 'NL West';

                        const divTeams = divFilter
                          ? MLB_TEAMS.filter(t => t.division === divFilter)
                          : MLB_TEAMS;

                        return (
                          <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-1.5">
                            <label className="text-xs font-bold text-slate-200 block">
                              {q.question_label}
                            </label>
                            <select
                              value={entryPicks[q.question_key] || ''}
                              onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                              className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-indigo-400 cursor-pointer"
                            >
                              <option value="">-- Select Team --</option>
                              {divTeams.map(t => (
                                <option key={t.code} value={t.name}>
                                  {t.name}
                                </option>
                              ))}
                            </select>
                          </div>
                        );
                      })}
                  </div>
                </div>

                {/* 2. WILD CARD TEAMS */}
                <div>
                  <h3 className="text-base font-bold text-sky-300 flex items-center gap-2 pb-2 border-b border-slate-800">
                    <span>🎫</span> Wild Card Qualifiers (2 pts each)
                  </h3>
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-3">
                    {questions
                      .filter(q => q.category === 'wild_card')
                      .map(q => {
                        const isAL = q.question_key.toLowerCase().startsWith('al_');
                        const leagueTeams = MLB_TEAMS.filter(t => (isAL ? t.league === 'AL' : t.league === 'NL'));

                        return (
                          <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-1.5">
                            <label className="text-xs font-bold text-slate-200 block">
                              {q.question_label}
                            </label>
                            <select
                              value={entryPicks[q.question_key] || ''}
                              onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                              className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-sky-400 cursor-pointer"
                            >
                              <option value="">-- Select Wild Card --</option>
                              {leagueTeams.map(t => (
                                <option key={t.code} value={t.name}>
                                  {t.name} ({t.division})
                                </option>
                              ))}
                            </select>
                          </div>
                        );
                      })}
                  </div>
                </div>

                {/* 3. POSTSEASON CHAMPIONS */}
                <div>
                  <h3 className="text-base font-bold text-amber-300 flex items-center gap-2 pb-2 border-b border-slate-800">
                    <span>👑</span> Pennants & World Series (5 - 7 pts each)
                  </h3>
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mt-3">
                    {questions
                      .filter(q => q.category === 'playoff_result')
                      .map(q => {
                        let pool = MLB_TEAMS;
                        const key = q.question_key.toLowerCase();
                        if (key.startsWith('al_')) pool = MLB_TEAMS.filter(t => t.league === 'AL');
                        if (key.startsWith('nl_')) pool = MLB_TEAMS.filter(t => t.league === 'NL');

                        return (
                          <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-1.5">
                            <label className="text-xs font-bold text-amber-200 block">
                              {q.question_label} ({q.points_exact} pts)
                            </label>
                            <select
                              value={entryPicks[q.question_key] || ''}
                              onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                              className="w-full bg-slate-900 border border-amber-800/60 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-amber-400 cursor-pointer"
                            >
                              <option value="">-- Select Champion --</option>
                              {pool.map(t => (
                                <option key={t.code} value={t.name}>
                                  {t.name}
                                </option>
                              ))}
                            </select>
                          </div>
                        );
                      })}
                  </div>
                </div>

                {/* 4. INDIVIDUAL AWARDS */}
                <div>
                  <h3 className="text-base font-bold text-pink-300 flex items-center gap-2 pb-2 border-b border-slate-800">
                    <span>🌟</span> Individual Awards (4 pts each)
                  </h3>
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-3">
                    {questions
                      .filter(q => q.category === 'award')
                      .map(q => {
                        const suggestions = PROMINENT_AWARD_CANDIDATES[q.question_key] || [];

                        return (
                          <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-2">
                            <label className="text-xs font-bold text-slate-200 block">
                              {q.question_label}
                            </label>
                            <input
                              type="text"
                              value={entryPicks[q.question_key] || ''}
                              onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                              placeholder="e.g. Player Name"
                              className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-pink-400"
                            />
                            {/* Suggestions pills */}
                            {suggestions.length > 0 && (
                              <div className="flex flex-wrap gap-1 mt-1">
                                {suggestions.slice(0, 4).map(name => (
                                  <button
                                    type="button"
                                    key={name}
                                    onClick={() => handlePickChange(q.question_key, name)}
                                    className="text-[10px] px-1.5 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 transition-colors cursor-pointer"
                                  >
                                    + {name}
                                  </button>
                                ))}
                              </div>
                            )}
                          </div>
                        );
                      })}
                  </div>
                </div>

                {/* 5. WIN / LOSS TOTALS */}
                <div>
                  <h3 className="text-base font-bold text-emerald-300 flex items-center gap-2 pb-2 border-b border-slate-800">
                    <span>📈</span> Extreme Team Win Totals (3 pts each)
                  </h3>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mt-3">
                    {questions
                      .filter(q => q.category === 'extremes')
                      .map(q => (
                        <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-1.5">
                          <label className="text-xs font-bold text-slate-200 block">
                            {q.question_label}
                          </label>
                          <select
                            value={entryPicks[q.question_key] || ''}
                            onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                            className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-emerald-400 cursor-pointer"
                          >
                            <option value="">-- Select Team --</option>
                            {MLB_TEAMS.map(t => (
                              <option key={t.code} value={t.name}>
                                {t.name}
                              </option>
                            ))}
                          </select>
                        </div>
                      ))}
                  </div>
                </div>

                {/* SAVE BUTTON */}
                <div className="pt-4 border-t border-slate-800 flex items-center justify-between">
                  <div className="text-xs text-slate-400">
                    Saving as <span className="text-white font-bold">{entryOwner}</span> for season <span className="text-white font-bold">{selectedSeason}</span>.
                  </div>
                  <button
                    type="submit"
                    disabled={saving}
                    className="px-6 py-3 rounded-xl bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white font-extrabold text-sm shadow-lg shadow-emerald-950/50 transition-all flex items-center gap-2 cursor-pointer disabled:opacity-50"
                  >
                    {saving ? (
                      <>
                        <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                        <span>Saving Picks...</span>
                      </>
                    ) : (
                      <>
                        <span>💾</span>
                        <span>Save & Lock In Picks</span>
                      </>
                    )}
                  </button>
                </div>
              </form>
            </div>
          )}

          {/* ========================================================= */}
          {/* TAB 3: HALL OF FAME & ALL-TIME DYNASTY                   */}
          {/* ========================================================= */}
          {activeTab === 'history' && (
            <div className="space-y-6">
              <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
                <div className="border-b border-slate-800 pb-4">
                  <h2 className="text-xl font-bold text-white flex items-center gap-2">
                    <span>🏛️</span> All-Time Pick'em Dynasty Leaderboard (2016 – 2026)
                  </h2>
                  <p className="text-xs text-slate-400 mt-1">
                    Cumulative championships, total auction budget cash won, and scoring records over 10 seasons of MLB forecasting.
                  </p>
                </div>

                <div className="overflow-x-auto mt-6">
                  <table className="w-full text-left text-sm border-collapse">
                    <thead>
                      <tr className="border-b border-slate-800 text-xs uppercase font-bold text-slate-400">
                        <th className="py-3 px-3">Owner</th>
                        <th className="py-3 px-3 text-center">🏆 Titles</th>
                        <th className="py-3 px-3 text-center">🏅 Top 3</th>
                        <th className="py-3 px-3 text-center">💰 Total Budget Won</th>
                        <th className="py-3 px-3 text-right">Total Points</th>
                        <th className="py-3 px-3 text-right">Seasons Played</th>
                        <th className="py-3 px-3 text-right">Personal Best</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60">
                      {hallOfFame.map((h, idx) => (
                        <tr
                          key={h.name}
                          className={`hover:bg-slate-800/40 transition-colors ${
                            idx === 0 ? 'bg-amber-950/20' : ''
                          }`}
                        >
                          <td className="py-3 px-3 font-bold text-white flex items-center gap-2">
                            {idx === 0 && <span className="text-amber-400">👑</span>}
                            <span>{h.name}</span>
                          </td>
                          <td className="py-3 px-3 text-center">
                            {h.titles > 0 ? (
                              <span className="px-2.5 py-1 rounded-full text-xs font-black bg-amber-950/80 border border-amber-500/50 text-amber-300">
                                {h.titles} {h.titles === 1 ? 'Championship' : 'Championships'}
                              </span>
                            ) : (
                              <span className="text-slate-600 text-xs">—</span>
                            )}
                          </td>
                          <td className="py-3 px-3 text-center text-slate-300 font-bold">
                            {h.top3}
                          </td>
                          <td className="py-3 px-3 text-center">
                            {h.totalPrizes > 0 ? (
                              <span className="px-2.5 py-1 rounded-full text-xs font-black bg-emerald-950/80 border border-emerald-500/50 text-emerald-300">
                                +${h.totalPrizes}
                              </span>
                            ) : (
                              <span className="text-slate-600 text-xs">$0</span>
                            )}
                          </td>
                          <td className="py-3 px-3 text-right font-black text-indigo-300">
                            {h.totalPoints}
                          </td>
                          <td className="py-3 px-3 text-right text-slate-400 font-mono text-xs">
                            {h.seasonsCount}
                          </td>
                          <td className="py-3 px-3 text-right text-slate-300 text-xs font-mono">
                            {h.bestScore} pts <span className="text-slate-500">('{h.bestYear?.toString().slice(2)}')</span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {/* ========================================================= */}
          {/* TAB 4: COMMISSIONER TOOLS & SCORING / BOOTSTRAP          */}
          {/* ========================================================= */}
          {activeTab === 'admin' && (
            <div className="space-y-6">
              {/* BOOTSTRAP FUTURE SEASON */}
              <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
                <h2 className="text-xl font-bold text-white flex items-center gap-2">
                  <span>🚀</span> Bootstrap Upcoming Season
                </h2>
                <p className="text-xs text-slate-400 mt-1">
                  Create a new Pick'em season with all 23 standard categories (Divisions, Wild Cards, Postseason, Awards, Win Totals) so league owners can start making picks.
                </p>

                <div className="mt-4 flex items-center gap-4">
                  <div>
                    <label className="text-xs font-bold text-slate-300 block mb-1">Season Year:</label>
                    <input
                      type="number"
                      value={newSeasonYear}
                      onChange={(e) => setNewSeasonYear(e.target.value)}
                      className="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-white font-bold text-sm w-32 focus:ring-2 focus:ring-indigo-400"
                    />
                  </div>
                  <div className="self-end">
                    <button
                      onClick={handleBootstrapSeason}
                      disabled={saving}
                      className="px-4 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold transition-all shadow cursor-pointer disabled:opacity-50"
                    >
                      Initialize Season {newSeasonYear} →
                    </button>
                  </div>
                </div>
              </div>

              {/* GRADE CURRENT SEASON */}
              <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-3 border-b border-slate-800 gap-2">
                  <div>
                    <h2 className="text-xl font-bold text-white flex items-center gap-2">
                      <span>⚖️</span> Grade Season & Score Recalculator ({selectedSeason})
                    </h2>
                    <p className="text-xs text-slate-400 mt-1">
                      Enter official MLB outcomes for each question. Saving will automatically score all owner picks, determine standings ranks, and award auction draft budget prizes.
                    </p>
                  </div>

                  <button
                    onClick={handleGradeSeason}
                    disabled={saving}
                    className="px-5 py-2.5 rounded-xl bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-500 hover:to-indigo-500 text-white font-bold text-xs shadow-lg transition-all flex items-center gap-2 cursor-pointer disabled:opacity-50"
                  >
                    {saving ? 'Grading...' : '💾 Calculate & Save All Scores'}
                  </button>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-4">
                  {questions.map(q => (
                    <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3 space-y-1.5">
                      <div className="flex items-center justify-between">
                        <label className="text-xs font-bold text-slate-200">
                          {q.question_label}
                        </label>
                        <span className="text-[10px] text-slate-500 font-mono">{q.points_exact || 3} pts</span>
                      </div>
                      <input
                        type="text"
                        value={gradingAnswers[q.question_key] || ''}
                        onChange={(e) => setGradingAnswers({ ...gradingAnswers, [q.question_key]: e.target.value })}
                        placeholder="Actual MLB Outcome"
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-white placeholder-slate-600 focus:ring-2 focus:ring-purple-400"
                      />
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
