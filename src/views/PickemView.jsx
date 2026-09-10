// src/views/PickemView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import { MLB_TEAMS, LEAGUE_OWNERS, PROMINENT_AWARD_CANDIDATES, teamsMatch, PICKEM_RULES } from '../utils/mlbTeams';
import { useAuth } from '../context/useAuth';

export default function PickemView() {
  const { user, profile, isCommissioner, effectiveOwner, effectiveTeamId } = useAuth();
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

  // Live in-progress tracking state (for 2026)
  const [showLiveProjections, setShowLiveProjections] = useState(true);
  const [liveProjectionsData, setLiveProjectionsData] = useState(null);
  const [refreshingLive, setRefreshingLive] = useState(false);
  const [showMethodologyModal, setShowMethodologyModal] = useState(false);

  // Close methodology modal on ESC key press
  useEffect(() => {
    if (!showMethodologyModal) return;
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') setShowMethodologyModal(false);
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [showMethodologyModal]);

  // All-time scores for Hall of Fame
  const [allTimeScores, setAllTimeScores] = useState([]);

  // Pick Submission / Edit Form State
  const [entryOwner, setEntryOwner] = useState(effectiveOwner || LEAGUE_OWNERS[3].name); // default Daniel
  const [entryPicks, setEntryPicks] = useState({});

  // Sync entryOwner when effectiveOwner changes
  useEffect(() => {
    if (effectiveOwner) {
      setEntryOwner(effectiveOwner);
    }
  }, [effectiveOwner]);

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
      if (selectedSeason === 2026) {
        setShowLiveProjections(true);
      } else {
        setShowLiveProjections(false);
      }
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
        if (currentActive.live_projections) {
          setLiveProjectionsData(currentActive.live_projections);
        }
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

      const [qRes, pRes, scRes, sRes] = await Promise.all([
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
          .order('place', { ascending: true }),
        supabase
          .from('pickem_seasons')
          .select('*')
          .eq('season_year', year)
          .single()
      ]);

      if (qRes.error) throw qRes.error;
      if (pRes.error) throw pRes.error;
      if (scRes.error) throw scRes.error;

      setQuestions(qRes.data || []);
      setPicks(pRes.data || []);
      setScores(scRes.data || []);

      if (sRes.data?.live_projections) {
        setLiveProjectionsData(sRes.data.live_projections);
      } else {
        setLiveProjectionsData(null);
      }
    } catch (err) {
      console.error(`Error loading details for ${year}:`, err);
      setErrorMessage(err.message || 'Failed to load season details');
    } finally {
      setLoading(false);
    }
  }

  // Live In-Browser Refresh from MLB Stats API
  const handleRefreshLiveStandings = async () => {
    setRefreshingLive(true);
    try {
      const resp = await fetch('https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&hydrate=team');
      if (!resp.ok) throw new Error('Failed to fetch from MLB Stats API');
      const data = await resp.json();

      // Simple real-time update of division standings
      const updatedCats = { ...(liveProjectionsData?.categories || {}) };
      const divMap = { 201: 'al_east', 202: 'al_central', 200: 'al_west', 204: 'nl_east', 205: 'nl_central', 203: 'nl_west' };

      for (const rec of data.records || []) {
        const divKey = divMap[rec.division?.id];
        if (divKey && rec.teamRecords?.[0]) {
          const top = rec.teamRecords[0];
          const runner = rec.teamRecords[1];
          updatedCats[divKey] = {
            ...updatedCats[divKey],
            leader: top.team.name,
            stat: `${top.wins}-${top.losses} (${top.winningPercentage})`,
            runner_up: runner ? `${runner.team.name} (${runner.gamesBack} GB)` : ''
          };
        }
      }

      setLiveProjectionsData(prev => ({
        ...prev,
        as_of: new Date().toISOString(),
        categories: updatedCats
      }));

      setSaveSuccess('Live MLB Standings refreshed directly from MLB Stats API! ⚡');
      setTimeout(() => setSaveSuccess(null), 4000);
    } catch (err) {
      console.error('Error refreshing MLB standings:', err);
      setErrorMessage('Could not refresh live standings directly: ' + err.message);
    } finally {
      setRefreshingLive(false);
    }
  };

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

    if (!user) {
      setErrorMessage('Please log in with Discord via the top menu to submit or save official picks.');
      return;
    }

    if (!isCommissioner && profile?.owner_name?.toLowerCase() !== entryOwner?.toLowerCase()) {
      setErrorMessage(`You are logged in as ${profile?.owner_name || 'an owner'}. You can only submit picks for your own team.`);
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

      // Build playoff pool for crossover checking (only division and wild card questions qualify)
      const alPlayoffs = [];
      const nlPlayoffs = [];
      updatedQuestions.forEach(q => {
        if (q.correct_answer && (q.category === 'division' || q.category === 'wild_card')) {
          if (q.question_key.startsWith('al_')) alPlayoffs.push(q.correct_answer);
          if (q.question_key.startsWith('nl_')) nlPlayoffs.push(q.correct_answer);
        }
      });

      for (const p of curPicks) {
        const q = qMap[p.question_id];
        if (!q) continue;

        let isCorrect = false;
        let points = 0;

        const pVal = p.pick_value?.trim() || '';
        const aVal = q.correct_answer?.trim() || '';

        if (aVal && pVal) {
          if (teamsMatch(pVal, aVal)) {
            isCorrect = true;
            points = q.points_exact || (q.category === 'division' ? 3 : q.category === 'wild_card' ? 2 : 3);
          } else if (q.category === 'wild_card' || q.category === 'division') {
            const inAL = q.question_key.startsWith('al_') && alPlayoffs.some(ans => teamsMatch(pVal, ans));
            const inNL = q.question_key.startsWith('nl_') && nlPlayoffs.some(ans => teamsMatch(pVal, ans));
            if (inAL || inNL) {
              isCorrect = false;
              points = 2; // Awards 2 points for correct team in wrong playoff spot
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
        const catKey = q.category || 'other';
        ownerTotals[p.owner_name].breakdown[catKey] = (ownerTotals[p.owner_name].breakdown[catKey] || 0) + points;
      }

      await supabase.from('pickem_picks').upsert(updatedPicks);

      // Rank owners & determine prize money (standard competition ranking with ties)
      const sortedOwners = Object.entries(ownerTotals).sort((a, b) => b[1].total - a[1].total);
      let currentRank = 1;
      const scoreRows = sortedOwners.map(([name, data], idx) => {
        if (idx > 0 && data.total < sortedOwners[idx - 1][1].total) {
          currentRank = idx + 1;
        }
        const place = currentRank;
        let prize = null;
        if (place === 1) prize = 4;
        else if (place === 2) prize = 3;
        else if (place === 3) prize = 2;
        else if (place === 4) prize = 1;

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
      await supabase.from('pickem_seasons').upsert({
        season_year: yearInt,
        status: 'open'
      });

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

  // Active Standings Data: Live Projections (if enabled) or Final Settled Scores
  const activeStandings = useMemo(() => {
    if (isInProgress && showLiveProjections && liveProjectionsData?.projected_standings) {
      return liveProjectionsData.projected_standings.map(s => ({
        owner_name: s.owner_name,
        team_id: s.team_id,
        place: s.place,
        total_points: s.projected_points,
        budget_awarded: s.projected_budget,
        is_live: true,
        hits: s.hits,
        total_hits: s.total_hits
      }));
    }
    return scores.map(s => ({
      ...s,
      is_live: false
    }));
  }, [isInProgress, showLiveProjections, liveProjectionsData, scores]);

  // Live evaluation helper for pick cells
  const evaluateLivePick = (qKey, pickVal) => {
    if (!liveProjectionsData?.categories) return null;
    const cat = liveProjectionsData.categories[qKey];
    if (!cat) return null;

    const leaderName = cat.leader || '';
    const pVal = pickVal || '';

    if (!pVal) return null;

    // Check exact match (using teamsMatch for team/player matching)
    if (leaderName && teamsMatch(pVal, leaderName)) {
      let exactPts = 3;
      if (cat.type === 'wild_card') exactPts = 2;
      else if (cat.type === 'division') exactPts = 3;
      else if (cat.type === 'award') exactPts = 4;
      else if (cat.type === 'playoff_result') exactPts = qKey.includes('world_series') ? 7 : 5;
      else if (cat.type === 'extremes') exactPts = 3;

      const label = cat.type === 'award' ? '✓ #1 in WAR (+4 pts)' : `✓ Leading (+${exactPts} pts)`;
      return { status: 'EXACT', label, pts: exactPts, leaderText: cat.leader };
    }

    // Check award contenders
    if (cat.type === 'award') {
      const isContender = (cat.contenders || []).some(c => teamsMatch(pVal, c));
      if (isContender) {
        return { status: 'CONTENDER', label: '⚡ Top Contender', pts: 0, leaderText: cat.leader };
      }
    }

    // Check Playoff Crossover for division and wild card questions
    const isPlayoffQ = cat.type === 'division' || cat.type === 'wild_card' ||
      qKey.includes('east') || qKey.includes('central') || qKey.includes('west') || qKey.includes('wc');

    if (isPlayoffQ) {
      const alPlayoffs = liveProjectionsData.playoff_al || [];
      const nlPlayoffs = liveProjectionsData.playoff_nl || [];

      if (qKey.startsWith('al_') && alPlayoffs.some(t => teamsMatch(pVal, t))) {
        return { status: 'CROSSOVER', label: '✦ In Playoff Spot (+2 pts)', pts: 2, leaderText: cat.leader };
      }
      if (qKey.startsWith('nl_') && nlPlayoffs.some(t => teamsMatch(pVal, t))) {
        return { status: 'CROSSOVER', label: '✦ In Playoff Spot (+2 pts)', pts: 2, leaderText: cat.leader };
      }
    }

    return { status: 'OFF_PACE', label: 'Off Pace', pts: 0, leaderText: cat.leader };
  };

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

          {/* Quick Season Switcher & Live Tracker Controls */}
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2 bg-slate-950/70 p-2 rounded-xl border border-indigo-900/60 shadow-inner">
              <span className="text-xs font-bold uppercase tracking-wider text-indigo-300 pl-2">Season:</span>
              <select
                value={selectedSeason}
                onChange={(e) => setSelectedSeason(parseInt(e.target.value))}
                className="bg-indigo-950 text-white font-bold text-sm px-3 py-1.5 rounded-lg border border-indigo-700/60 focus:outline-none focus:ring-2 focus:ring-indigo-400 cursor-pointer"
              >
                {seasons.map(s => (
                  <option key={s.season_year} value={s.season_year}>
                    {s.season_year} {s.season_year === 2027 ? '🚀 (Open Entry)' : s.season_year === 2026 ? '⚡ (Live Tracker)' : '🏆 (Final)'}
                  </option>
                ))}
              </select>
            </div>

            {/* In-Progress Live Tracker Mode Switch */}
            {isInProgress && liveProjectionsData && (
              <button
                onClick={() => setShowLiveProjections(!showLiveProjections)}
                className={`px-3.5 py-2 rounded-xl text-xs font-bold flex items-center gap-2 transition-all cursor-pointer shadow-md ${
                  showLiveProjections
                    ? 'bg-gradient-to-r from-amber-500 to-orange-500 text-slate-950 ring-2 ring-amber-300 font-black animate-pulse'
                    : 'bg-slate-800 text-slate-300 hover:bg-slate-700 border border-slate-700'
                }`}
                title="Toggle between Live In-Progress Standings and Settled Results"
              >
                <span>⚡</span>
                <span>{showLiveProjections ? 'Live In-Progress Standings: ON' : 'Show Live Projections'}</span>
              </button>
            )}
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
                <span className="w-2 h-2 rounded-full bg-amber-400 animate-ping"></span> Live 2026 In-Progress (YTD MLB Standings + Projected WAR)
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

          {/* Quick Refresh & Methodology Links */}
          <div className="flex items-center gap-2">
            {isInProgress && (
              <>
                <button
                  onClick={handleRefreshLiveStandings}
                  disabled={refreshingLive}
                  className="px-2.5 py-1 rounded bg-indigo-900/60 hover:bg-indigo-800 border border-indigo-700/60 text-indigo-200 text-xs font-semibold flex items-center gap-1.5 transition-colors cursor-pointer disabled:opacity-50"
                  title="Fetch live MLB standings directly from MLB Stats API"
                >
                  <span className={refreshingLive ? 'animate-spin' : ''}>🔄</span>
                  <span>{refreshingLive ? 'Refreshing...' : 'Refresh MLB Standings'}</span>
                </button>
                <button
                  onClick={() => setShowMethodologyModal(true)}
                  className="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold transition-colors cursor-pointer"
                >
                  ℹ️ Projections Methodology
                </button>
              </>
            )}
          </div>
        </div>
      </div>

      {/* METHODOLOGY MODAL */}
      {showMethodologyModal && (
        <div 
          onClick={(e) => e.target === e.currentTarget && setShowMethodologyModal(false)}
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/80 backdrop-blur-xs p-4"
        >
          <div className="bg-slate-900 border border-slate-700 rounded-2xl max-w-xl w-full p-6 shadow-2xl relative space-y-4">
            <div className="flex items-center justify-between border-b border-slate-800 pb-3">
              <h3 className="text-lg font-bold text-white flex items-center gap-2">
                <span>⚡</span> In-Progress Projections Methodology
              </h3>
              <button
                onClick={() => setShowMethodologyModal(false)}
                className="text-slate-400 hover:text-white text-lg font-bold cursor-pointer"
              >
                ✕
              </button>
            </div>

            <div className="text-xs text-slate-300 space-y-3 leading-relaxed">
              <p>
                To track which predictions are working out while the MLB season is actively underway, our system combines live official standings with sabermetric projections:
              </p>
              <ul className="space-y-2 list-disc pl-4 text-slate-300">
                <li>
                  <strong className="text-white">Division Winners (3 pts):</strong> Evaluated against current 1st-place teams in each division via the official MLB Stats API.
                </li>
                <li>
                  <strong className="text-white">Wild Card & Playoff Crossovers (2 pts):</strong> Teams holding playoff seeds in AL and NL (division winners and wild card seeds 1–3). If an owner predicted a team for a playoff spot (division or wild card) and that team qualifies for any playoff spot in that league, they earn 2 points if not an exact match.
                </li>
                <li>
                  <strong className="text-white">Pennants (5 pts) & World Series (7 pts):</strong> Current top seed in each league and best overall record in baseball.
                </li>
                <li>
                  <strong className="text-white">Player Awards Placeholder (4 pts):</strong> Because awards are officially announced in November, we use <span className="text-amber-300 font-bold">Projected Full-Season fWAR</span> (combining FanGraphs actual YTD fWAR + Rest-of-Season projected fWAR). The #1 player in each league's projected fWAR is the current placeholder leader.
                </li>
                <li>
                  <strong className="text-white">Extreme Win / Loss Totals (3 pts):</strong> Teams holding the most total wins and most total losses in Major League Baseball.
                </li>
              </ul>
              <p className="text-[11px] text-slate-400 pt-2 border-t border-slate-800">
                Data sources: Official MLB Stats API (`statsapi.mlb.com`) and FanGraphs Depth Charts / ROS Leaderboards API.
              </p>
            </div>

            <div className="pt-2 text-right">
              <button
                onClick={() => setShowMethodologyModal(false)}
                className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white font-bold text-xs rounded-lg cursor-pointer"
              >
                Got It
              </button>
            </div>
          </div>
        </div>
      )}

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
          <span>{activeStandings.length || seasonOwners.length} owners</span>
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
                    <div className="flex items-center gap-2">
                      <h2 className="text-xl font-bold text-white flex items-center gap-2">
                        <span>🏆</span> {selectedSeason} Standings & Budget Prizes
                      </h2>
                      {isInProgress && showLiveProjections && (
                        <span className="px-2.5 py-0.5 rounded-full text-[11px] font-black bg-amber-400 text-slate-950 uppercase tracking-wider animate-pulse">
                          ⚡ Live Interim Projection
                        </span>
                      )}
                    </div>
                    <p className="text-xs text-slate-400 mt-0.5">
                      {isInProgress && showLiveProjections
                        ? 'Projected standings calculated from current live MLB records, division leaders, wild cards, and FanGraphs projected fWAR for awards.'
                        : "Top 4 finishers earn additional auction draft dollars for next season's draft: 1st (+$4), 2nd (+$3), 3rd (+$2), 4th (+$1). Ties award the full amount for that place."}
                    </p>
                  </div>
                  {isUpcoming && selectedSeason === 2027 && (
                    <span className="text-xs bg-emerald-950 text-emerald-300 px-3 py-1 rounded-full border border-emerald-800/60 font-semibold self-start">
                      Picks open – scores pending season results
                    </span>
                  )}
                </div>

                {activeStandings.length > 0 ? (
                  <>
                    {/* TOP 3 PODIUM */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 my-6">
                      {/* 2nd Place */}
                      <div className="order-2 md:order-1 bg-gradient-to-b from-slate-800/60 to-slate-900/90 border border-slate-700/60 rounded-xl p-4 text-center flex flex-col items-center justify-center relative">
                        <span className="text-3xl mb-1">🥈</span>
                        <span className="text-xs uppercase font-bold tracking-widest text-slate-400">2nd Place</span>
                        <div className="text-lg font-black text-white mt-1">
                          {activeStandings[1]?.owner_name || '—'}
                        </div>
                        <div className="text-2xl font-black text-indigo-300 mt-0.5">
                          {activeStandings[1]?.total_points ?? 0} <span className="text-xs text-slate-400 font-normal">pts</span>
                        </div>
                        {(activeStandings[1]?.budget_awarded || 0) > 0 && (
                          <span className="mt-2 text-xs font-extrabold px-2.5 py-0.5 rounded-full bg-emerald-950 border border-emerald-500/40 text-emerald-300">
                            +${activeStandings[1].budget_awarded} Draft Budget
                          </span>
                        )}
                        {activeStandings[1]?.total_hits > 0 && (
                          <span className="text-[10px] text-slate-400 mt-1">
                            {activeStandings[1].total_hits} active hits
                          </span>
                        )}
                      </div>

                      {/* 1st Place (Leader) */}
                      <div className="order-1 md:order-2 bg-gradient-to-b from-amber-950/40 via-slate-800/90 to-slate-900 border-2 border-amber-500/60 rounded-xl p-5 text-center flex flex-col items-center justify-center relative shadow-lg shadow-amber-950/20 transform md:-translate-y-2">
                        <div className="absolute -top-3 bg-amber-500 text-slate-950 text-[10px] uppercase font-black px-3 py-0.5 rounded-full tracking-wider shadow">
                          {isInProgress && showLiveProjections ? 'CURRENT LEADER' : 'CHAMPION'}
                        </div>
                        <span className="text-4xl mb-1 mt-1">🥇</span>
                        <span className="text-xs uppercase font-bold tracking-widest text-amber-400">1st Place</span>
                        <div className="text-xl font-black text-white mt-1">
                          {activeStandings[0]?.owner_name || '—'}
                        </div>
                        <div className="text-3xl font-black text-amber-300 mt-0.5">
                          {activeStandings[0]?.total_points ?? 0} <span className="text-xs text-slate-400 font-normal">pts</span>
                        </div>
                        {(activeStandings[0]?.budget_awarded || 0) > 0 && (
                          <span className="mt-2 text-xs font-black px-3 py-1 rounded-full bg-amber-400 text-slate-950 shadow">
                            +${activeStandings[0].budget_awarded} Draft Budget
                          </span>
                        )}
                        {activeStandings[0]?.total_hits > 0 && (
                          <span className="text-[10px] text-amber-200/80 mt-1 font-medium">
                            {activeStandings[0].total_hits} active hits
                          </span>
                        )}
                      </div>

                      {/* 3rd Place */}
                      <div className="order-3 bg-gradient-to-b from-amber-950/20 to-slate-900/90 border border-amber-900/40 rounded-xl p-4 text-center flex flex-col items-center justify-center relative">
                        <span className="text-3xl mb-1">🥉</span>
                        <span className="text-xs uppercase font-bold tracking-widest text-amber-600">3rd Place</span>
                        <div className="text-lg font-black text-white mt-1">
                          {activeStandings[2]?.owner_name || '—'}
                        </div>
                        <div className="text-2xl font-black text-indigo-300 mt-0.5">
                          {activeStandings[2]?.total_points ?? 0} <span className="text-xs text-slate-400 font-normal">pts</span>
                        </div>
                        {(activeStandings[2]?.budget_awarded || 0) > 0 && (
                          <span className="mt-2 text-xs font-extrabold px-2.5 py-0.5 rounded-full bg-emerald-950 border border-emerald-500/40 text-emerald-300">
                            +${activeStandings[2].budget_awarded} Draft Budget
                          </span>
                        )}
                        {activeStandings[2]?.total_hits > 0 && (
                          <span className="text-[10px] text-slate-400 mt-1">
                            {activeStandings[2].total_hits} active hits
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
                            <th className="py-2.5 px-3 text-right">
                              {isInProgress && showLiveProjections ? 'Projected Pts' : 'Total Points'}
                            </th>
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
                            ) : isInProgress && showLiveProjections ? (
                              <th className="py-2.5 px-3 text-slate-400">Current Active Hits (YTD Standings & WAR)</th>
                            ) : (
                              <th className="py-2.5 px-3 text-slate-400">Performance Status</th>
                            )}
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-800/60">
                          {activeStandings.map((sc) => {
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
                                ) : isInProgress && showLiveProjections ? (
                                  <td className="py-2.5 px-3 text-xs">
                                    <div className="flex flex-wrap items-center gap-1.5 max-w-lg">
                                      {(sc.hits || []).slice(0, 5).map((h, i) => (
                                        <span
                                          key={i}
                                          className={`px-2 py-0.5 rounded text-[11px] font-semibold ${
                                            h.hit_type === 'EXACT'
                                              ? 'bg-emerald-950/80 border border-emerald-500/40 text-emerald-300'
                                              : 'bg-sky-950/80 border border-sky-500/40 text-sky-300'
                                          }`}
                                          title={`${h.question_label}: ${h.pick_value}`}
                                        >
                                          {h.pick_value} (+{h.points})
                                        </span>
                                      ))}
                                      {(sc.hits || []).length > 5 && (
                                        <span className="text-[10px] text-slate-400 font-bold">
                                          +{(sc.hits || []).length - 5} more
                                        </span>
                                      )}
                                    </div>
                                  </td>
                                ) : (
                                  <td className="py-2.5 px-3 text-xs text-slate-400">
                                    {place === 1 ? '🏆 Champion' : place <= 4 ? '✨ In the Money' : 'Out of the Money'}
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
                        {isInProgress && showLiveProjections
                          ? 'Real-time side-by-side evaluation against live 2026 MLB division leaders and FanGraphs projected fWAR leaders.'
                          : "Side-by-side comparison of every owner's predictions and official outcomes."}
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
                          <th className="py-3 px-3 min-w-[160px] bg-slate-900/90 text-emerald-300 border-x border-slate-800">
                            {isInProgress && showLiveProjections ? '⚡ YTD Leader (MLB / WAR)' : 'Actual Outcome'}
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

                          // Live info for 2026
                          const liveCat = liveProjectionsData?.categories?.[q.question_key];

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

                              {/* Actual Answer / Live Leader Column */}
                              <td className="py-2.5 px-3 bg-slate-950/40 border-x border-slate-800">
                                {hasActual ? (
                                  <span className="font-bold text-emerald-300 bg-emerald-950/80 px-2 py-1 rounded border border-emerald-600/40 block text-center">
                                    {outcome}
                                  </span>
                                ) : isInProgress && showLiveProjections && liveCat ? (
                                  <div className="text-center space-y-0.5">
                                    <div className="font-bold text-amber-300 text-xs flex items-center justify-center gap-1">
                                      <span>⚡</span>
                                      <span>{liveCat.leader}</span>
                                    </div>
                                    <div className="text-[10px] text-slate-400 font-mono">
                                      {liveCat.stat}
                                    </div>
                                  </div>
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

                                // If live in progress mode
                                const liveEval = isInProgress && showLiveProjections
                                  ? evaluateLivePick(q.question_key, pickVal)
                                  : null;

                                return (
                                  <td key={o.name} className="py-2.5 px-3 text-center">
                                    {liveEval ? (
                                      <div
                                        className={`p-2 rounded-lg border transition-all text-xs ${
                                          liveEval.status === 'EXACT'
                                            ? 'bg-emerald-950/80 border-emerald-500/60 text-emerald-200 font-bold shadow-xs'
                                            : liveEval.status === 'CROSSOVER'
                                            ? 'bg-sky-950/80 border-sky-500/60 text-sky-200 font-semibold'
                                            : liveEval.status === 'CONTENDER'
                                            ? 'bg-amber-950/40 border-amber-500/40 text-amber-200'
                                            : 'bg-slate-900/60 border-slate-800 text-slate-400'
                                        }`}
                                      >
                                        <div>{pickVal}</div>
                                        <div
                                          className={`text-[10px] font-black mt-0.5 ${
                                            liveEval.status === 'EXACT'
                                              ? 'text-emerald-400'
                                              : liveEval.status === 'CROSSOVER'
                                              ? 'text-sky-400'
                                              : liveEval.status === 'CONTENDER'
                                              ? 'text-amber-400'
                                              : 'text-slate-500'
                                          }`}
                                        >
                                          {liveEval.label}
                                        </div>
                                      </div>
                                    ) : (
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
                                            ✦ +{pts} pts (Playoff Spot)
                                          </div>
                                        )}
                                      </div>
                                    )}
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
                  {isCommissioner ? (
                    <select
                      value={entryOwner}
                      onChange={(e) => setEntryOwner(e.target.value)}
                      className="bg-slate-800 text-amber-300 font-bold text-sm px-3 py-1.5 rounded-lg border border-amber-500/60 focus:ring-2 focus:ring-amber-400 cursor-pointer shadow-sm"
                    >
                      {LEAGUE_OWNERS.map(o => (
                        <option key={o.id} value={o.name}>
                          {o.name} (Team {o.id}) {o.id === 5 || o.id === 2 ? '👑' : ''}
                        </option>
                      ))}
                    </select>
                  ) : user ? (
                    <div className="bg-slate-800 text-indigo-300 font-bold text-sm px-3 py-1.5 rounded-lg border border-indigo-700/60 flex items-center gap-2">
                      <span>{entryOwner} (Team {effectiveTeamId})</span>
                      <span className="text-[10px] text-emerald-400 font-semibold bg-emerald-950/60 px-1.5 py-0.5 rounded border border-emerald-500/30">Verified</span>
                    </div>
                  ) : (
                    <select
                      value={entryOwner}
                      disabled
                      className="bg-slate-800/50 text-slate-400 font-bold text-sm px-3 py-1.5 rounded-lg border border-slate-700 cursor-not-allowed"
                    >
                      <option value={entryOwner}>{entryOwner} (Log in to Submit)</option>
                    </select>
                  )}
                </div>
              </div>

              {/* Auth / Commissioner status alerts */}
              {!user && (
                <div className="bg-indigo-950/40 border border-indigo-500/40 rounded-xl p-3.5 flex items-center gap-3 text-indigo-200 text-xs shadow-sm">
                  <span className="text-xl">🔒</span>
                  <div>
                    <span className="font-bold text-white">Discord Login Required:</span> Please log in with Discord via the top navigation bar to submit or save your official predictions for {selectedSeason}.
                  </div>
                </div>
              )}

              {isCommissioner && (
                <div className="bg-amber-950/40 border border-amber-500/50 rounded-xl p-3 flex flex-col sm:flex-row sm:items-center justify-between gap-2 text-xs text-amber-200 shadow-sm">
                  <div className="flex items-center gap-2">
                    <span className="text-base">👑</span>
                    <span>
                      <strong>Commissioner Mode Active:</strong> You have commissioner privileges to submit or edit official picks on behalf of <strong>{entryOwner}</strong>.
                    </span>
                  </div>
                  <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-bold border border-amber-500/40 uppercase tracking-wider text-[10px] w-fit">
                    Admin Override Enabled
                  </span>
                </div>
              )}

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
            !isCommissioner ? (
              <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-10 text-center max-w-md mx-auto shadow-2xl space-y-4 my-8">
                <div className="w-16 h-16 rounded-2xl bg-amber-950/40 border border-amber-500/30 flex items-center justify-center text-3xl mx-auto shadow-inner">
                  🔒
                </div>
                <div>
                  <h3 className="text-lg font-bold text-white">Commissioner Access Only</h3>
                  <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                    Official grading, answer resolution, and bootstrapping new Pick'em seasons are restricted to league commissioners (<strong>Dan</strong> and <strong>Adrian</strong>).
                  </p>
                </div>
                {user ? (
                  <div className="pt-2 border-t border-slate-800 text-xs text-slate-400">
                    Logged in as <span className="text-indigo-300 font-bold">{profile?.owner_name || user.email}</span> (Owner)
                  </div>
                ) : (
                  <div className="pt-2 border-t border-slate-800 text-xs text-slate-400">
                    Please log in with Discord via the top navigation bar to verify commissioner permissions.
                  </div>
                )}
              </div>
            ) : (
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
          ))}
        </>
      )}
    </div>
  );
}
