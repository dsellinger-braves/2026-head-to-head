// src/views/PickemView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import { LEAGUE_OWNERS, teamsMatch } from '../utils/mlbTeams';
import { useAuth } from '../context/useAuth';
import {
  STANDARD_PICKEM_QUESTIONS,
  computeHallOfFame
} from './pickem/services/pickemScoringService';
import PickemHeader from './pickem/components/PickemHeader';
import PickemMethodologyModal from './pickem/modals/PickemMethodologyModal';
import PickemBoardTab from './pickem/tabs/PickemBoardTab';
import PickemEntryTab from './pickem/tabs/PickemEntryTab';
import PickemHistoryTab from './pickem/tabs/PickemHistoryTab';
import PickemAdminTab from './pickem/tabs/PickemAdminTab';

export default function PickemView({ initialSeason = 2027, onSeasonChange }) {
  const { user, profile, isCommissioner, effectiveOwner, effectiveTeamId, governanceTitle = 'Commissioner' } = useAuth();
  const [seasons, setSeasons] = useState([]);
  const [selectedSeason, setSelectedSeason] = useState(initialSeason || 2027);
  const [activeTab, setActiveTab] = useState('board'); // 'board', 'entry', 'history', 'admin'
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(null);
  const [errorMessage, setErrorMessage] = useState(null);

  // Sync selectedSeason when initialSeason prop changes
  useEffect(() => {
    if (initialSeason && initialSeason !== selectedSeason) {
      setSelectedSeason(initialSeason);
    }
  }, [initialSeason, selectedSeason]);

  // Current Season Data
  const [questions, setQuestions] = useState([]);
  const [picks, setPicks] = useState([]);
  const [scores, setScores] = useState([]);

  // Live in-progress tracking state (for 2026)
  const [showLiveProjections, setShowLiveProjections] = useState(true);
  const [liveProjectionsData, setLiveProjectionsData] = useState(null);
  const [refreshingLive, setRefreshingLive] = useState(false);
  const [showMethodologyModal, setShowMethodologyModal] = useState(false);

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
    // eslint-disable-next-line react-hooks/exhaustive-deps
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
      const { data: sData, error: sErr } = await supabase
        .from('pickem_seasons')
        .select('*')
        .order('season_year', { ascending: false });

      if (sErr) throw sErr;
      if (sData && sData.length > 0) {
        setSeasons(sData);
        const currentActive = sData.find(s => s.season_year === initialSeason) || sData.find(s => s.season_year === 2027) || sData.find(s => s.season_year === 2026) || sData[0];
        setSelectedSeason(currentActive.season_year);
        if (currentActive.live_projections) {
          setLiveProjectionsData(currentActive.live_projections);
        }
      }

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

  // Live In-Browser Refresh from MLB Stats API & Supabase
  const handleRefreshLiveStandings = async () => {
    setRefreshingLive(true);
    setErrorMessage(null);
    try {
      const { data: seasonData, error: sErr } = await supabase
        .from('pickem_seasons')
        .select('live_projections')
        .eq('season_year', selectedSeason)
        .single();

      if (!sErr && seasonData?.live_projections) {
        setLiveProjectionsData(seasonData.live_projections);
        setSaveSuccess('Live Pick\'em Standings updated with latest official data! ⚡');
        setTimeout(() => setSaveSuccess(null), 4000);
        return;
      }

      const resp = await fetch('https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&hydrate=team');
      if (!resp.ok) throw new Error('Failed to fetch from MLB Stats API');
      const data = await resp.json();

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
      setErrorMessage('Could not refresh live standings: ' + err.message);
    } finally {
      setRefreshingLive(false);
    }
  };

  const handlePickChange = (questionKey, value) => {
    setEntryPicks(prev => ({
      ...prev,
      [questionKey]: value
    }));
  };

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

      const { error: upsertErr } = await supabase
        .from('pickem_picks')
        .upsert(pickRows, { onConflict: 'season_year,question_id,owner_name' });

      if (upsertErr) throw upsertErr;

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

  const handleGradeSeason = async () => {
    setSaving(true);
    try {
      for (const q of questions) {
        const actual = gradingAnswers[q.question_key] || null;
        if (actual !== q.correct_answer) {
          await supabase
            .from('pickem_questions')
            .update({ correct_answer: actual })
            .eq('id', q.id);
        }
      }

      const { data: updatedQuestions } = await supabase
        .from('pickem_questions')
        .select('*')
        .eq('season_year', selectedSeason);

      const { data: curPicks } = await supabase
        .from('pickem_picks')
        .select('*')
        .eq('season_year', selectedSeason);

      const updatedPicks = [];
      const ownerTotals = {};
      const qMap = {};
      (updatedQuestions || []).forEach(q => { qMap[q.id] = q; });

      const alPlayoffs = [];
      const nlPlayoffs = [];
      (updatedQuestions || []).forEach(q => {
        if (q.correct_answer && (q.category === 'division' || q.category === 'wild_card')) {
          if (q.question_key.startsWith('al_')) alPlayoffs.push(q.correct_answer);
          if (q.question_key.startsWith('nl_')) nlPlayoffs.push(q.correct_answer);
        }
      });

      for (const p of (curPicks || [])) {
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
              points = 2;
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

  const handleBootstrapSeason = async () => {
    if (!newSeasonYear || isNaN(newSeasonYear)) return;
    setSaving(true);
    try {
      const yearInt = parseInt(newSeasonYear, 10);
      await supabase.from('pickem_seasons').upsert({
        season_year: yearInt,
        status: 'open'
      });

      const qRows = STANDARD_PICKEM_QUESTIONS.map(q => ({
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

  const seasonOwners = useMemo(() => {
    const map = new Map();
    scores.forEach(s => {
      map.set(s.owner_name.toLowerCase(), { name: s.owner_name, team_id: s.team_id });
    });
    picks.forEach(p => {
      const k = p.owner_name.toLowerCase();
      if (!map.has(k)) {
        map.set(k, { name: p.owner_name, team_id: p.team_id });
      }
    });
    return Array.from(map.values());
  }, [scores, picks]);

  const pickMatrix = useMemo(() => {
    const matrix = {};
    picks.forEach(p => {
      if (!matrix[p.question_id]) matrix[p.question_id] = {};
      matrix[p.question_id][p.owner_name.toLowerCase()] = p;
    });
    return matrix;
  }, [picks]);

  const filteredQuestions = useMemo(() => {
    if (gridCategoryFilter === 'ALL') return questions;
    return questions.filter(q => q.category === gridCategoryFilter);
  }, [questions, gridCategoryFilter]);

  const hallOfFame = useMemo(() => {
    return computeHallOfFame(allTimeScores);
  }, [allTimeScores]);

  const currentSeasonObj = seasons.find(s => s.season_year === selectedSeason);
  const isSummaryEra = selectedSeason <= 2018;
  const isUpcoming = currentSeasonObj?.status === 'open' || currentSeasonObj?.status === 'upcoming';
  const isInProgress = currentSeasonObj?.status === 'in_progress' || selectedSeason === 2026;

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

  return (
    <div className="space-y-6 pb-12">
      {/* METHODOLOGY MODAL */}
      <PickemMethodologyModal
        isOpen={showMethodologyModal}
        onClose={() => setShowMethodologyModal(false)}
      />

      {/* HEADER BANNER & TAB NAVIGATION */}
      <PickemHeader
        selectedSeason={selectedSeason}
        onSeasonChange={(yr) => {
          setSelectedSeason(yr);
          onSeasonChange?.(yr);
        }}
        seasons={seasons}
        isUpcoming={isUpcoming}
        isInProgress={isInProgress}
        questionsCount={questions.length}
        showLiveProjections={showLiveProjections}
        setShowLiveProjections={setShowLiveProjections}
        liveProjectionsData={liveProjectionsData}
        refreshingLive={refreshingLive}
        onRefreshLiveStandings={handleRefreshLiveStandings}
        onOpenMethodology={() => setShowMethodologyModal(true)}
        saveSuccess={saveSuccess}
        errorMessage={errorMessage}
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        totalPicks={picks.length}
        totalOwners={seasonOwners.length}
      />

      {/* LOADING STATE */}
      {loading ? (
        <div className="bg-slate-900/80 border border-slate-800 rounded-2xl p-12 text-center shadow-xl">
          <div className="inline-block w-8 h-8 border-4 border-indigo-500 border-t-transparent rounded-full animate-spin mb-3"></div>
          <p className="text-slate-400 text-sm">Loading Pick'em Championship data...</p>
        </div>
      ) : (
        <>
          {/* TAB 1: LEADERBOARD & PICKS */}
          {activeTab === 'board' && (
            <PickemBoardTab
              selectedSeason={selectedSeason}
              isInProgress={isInProgress}
              showLiveProjections={showLiveProjections}
              isUpcoming={isUpcoming}
              isSummaryEra={isSummaryEra}
              activeStandings={activeStandings}
              seasonOwners={seasonOwners}
              filteredQuestions={filteredQuestions}
              pickMatrix={pickMatrix}
              gridCategoryFilter={gridCategoryFilter}
              setGridCategoryFilter={setGridCategoryFilter}
              liveProjectionsData={liveProjectionsData}
              setActiveTab={setActiveTab}
            />
          )}

          {/* TAB 2: SUBMIT / EDIT PICKS FORM */}
          {activeTab === 'entry' && (
            <PickemEntryTab
              selectedSeason={selectedSeason}
              entryOwner={entryOwner}
              setEntryOwner={setEntryOwner}
              entryPicks={entryPicks}
              handlePickChange={handlePickChange}
              handleSavePicks={handleSavePicks}
              saving={saving}
              isCommissioner={isCommissioner}
              user={user}
              effectiveTeamId={effectiveTeamId}
              governanceTitle={governanceTitle}
              questions={questions}
            />
          )}

          {/* TAB 3: HALL OF FAME & ALL-TIME DYNASTY */}
          {activeTab === 'history' && (
            <PickemHistoryTab hallOfFame={hallOfFame} />
          )}

          {/* TAB 4: COMMISSIONER TOOLS & SCORING / BOOTSTRAP */}
          {activeTab === 'admin' && (
            <PickemAdminTab
              isCommissioner={isCommissioner}
              user={user}
              profile={profile}
              selectedSeason={selectedSeason}
              newSeasonYear={newSeasonYear}
              setNewSeasonYear={setNewSeasonYear}
              handleBootstrapSeason={handleBootstrapSeason}
              handleGradeSeason={handleGradeSeason}
              saving={saving}
              questions={questions}
              gradingAnswers={gradingAnswers}
              setGradingAnswers={setGradingAnswers}
            />
          )}
        </>
      )}
    </div>
  );
}
