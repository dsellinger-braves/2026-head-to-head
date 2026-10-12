// src/views/LeagueHistoryView.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { TEAMS } from '../schedule';
import {
  CATEGORIES,
  LEGACY_CATEGORIES,
  normalizeOwner,
  fetchHistoricalFinishes,
  computeTeamsPerSeason,
  computeAugmentedFinishes,
  computeHighlightsData,
  computeFranchiseRecords,
} from './history/services/historyFinishesService';
import HistoryHeaderNav from './history/components/HistoryHeaderNav';
import EraAdjustmentModal from './history/modals/EraAdjustmentModal';
import HistoryStandingsTab from './history/tabs/HistoryStandingsTab';
import HistoryHighlightsTab from './history/tabs/HistoryHighlightsTab';
import FranchiseHallOfFameTab from './history/tabs/FranchiseHallOfFameTab';

const HASH_BY_HISTORY_TAB = {
  standings: '#/history/standings',
  highlights: '#/history/highlights',
  records: '#/history/records',
};

export default function LeagueHistoryView({
  selectedSeason = 2026,
  onOwnerClick,
  subTab = 'standings',
}) {
  const [localSubTab, setLocalSubTab] = useState('standings');
  const activeSubTab = (subTab && ['standings', 'highlights', 'records'].includes(subTab)) ? subTab : localSubTab;
  const [loading, setLoading] = useState(true);
  const [rawFinishes, setRawFinishes] = useState([]);
  const [selectedYear, setSelectedYear] = useState(selectedSeason ? String(selectedSeason) : 'ALL');
  const [selectedOwner, setSelectedOwner] = useState('ALL');
  const [dataSource, setDataSource] = useState('');

  // Era adjustment states
  const [adjustRoster, setAdjustRoster] = useState(false);
  const [adjustMlb, setAdjustMlb] = useState(false);
  const [showMethodologyModal, setShowMethodologyModal] = useState(false);
  const [highlightFilter, setHighlightFilter] = useState('all'); // 'all' | 'highlights' | 'lowlights'
  const [exclude2020Lowlights, setExclude2020Lowlights] = useState(true);
  const [highlightViewMode, setHighlightViewMode] = useState('totals'); // 'totals' | 'relative' | 'margins'
  const [marginsGapType, setMarginsGapType] = useState('all'); // 'all' | '1st_2nd' | 'last_2nd_last'

  // Standings display controls
  const [showRawStats, setShowRawStats] = useState(selectedYear === 'ALL');
  const [currentCategoriesOnly, setCurrentCategoriesOnly] = useState(false);

  // Highlights stat records filter
  const [selectedStatCategory, setSelectedStatCategory] = useState('ALL');

  // Sorting state for standings table
  const [sortField, setSortField] = useState('points');
  const [sortDirection, setSortDirection] = useState('desc');

  // Sorting state for Franchise Hall leaderboard table
  const [hallSortField, setHallSortField] = useState('titles');
  const [hallSortDirection, setHallSortDirection] = useState('desc');

  const handleSubTabChange = (tabKey) => {
    setLocalSubTab(tabKey);
    const targetHash = HASH_BY_HISTORY_TAB[tabKey];
    if (targetHash && window.location.hash !== targetHash) {
      window.location.hash = targetHash;
    }
  };

  // Load historical finishes
  useEffect(() => {
    let isMounted = true;
    async function loadData() {
      setLoading(true);
      const res = await fetchHistoricalFinishes();
      if (isMounted) {
        setRawFinishes(res.records || []);
        setDataSource(res.source || 'Supabase Warehouse');
        setLoading(false);
      }
    }
    loadData();
    return () => {
      isMounted = false;
    };
  }, []);

  // Compute total teams per season
  const teamsPerSeason = useMemo(() => {
    return computeTeamsPerSeason(rawFinishes);
  }, [rawFinishes]);

  // Augmented finishes with era adjustment metrics
  const augmentedFinishes = useMemo(() => {
    return computeAugmentedFinishes(rawFinishes, teamsPerSeason, adjustRoster, adjustMlb);
  }, [rawFinishes, teamsPerSeason, adjustRoster, adjustMlb]);

  // Filtered finishes based on user selection
  const filteredFinishes = useMemo(() => {
    return augmentedFinishes.filter(r => {
      if (currentCategoriesOnly && r.year < 2019) return false;
      if (selectedYear !== 'ALL' && r.year !== parseInt(selectedYear, 10)) return false;
      if (selectedOwner !== 'ALL' && normalizeOwner(r.owner).toLowerCase() !== normalizeOwner(selectedOwner).toLowerCase()) return false;
      return true;
    });
  }, [augmentedFinishes, selectedYear, selectedOwner, currentCategoriesOnly]);

  // Sorted finishes for Year-by-Year table
  const sortedFinishes = useMemo(() => {
    const list = [...filteredFinishes];
    list.sort((a, b) => {
      let valA, valB;
      if (sortField === 'place') {
        valA = a.place;
        valB = b.place;
        return sortDirection === 'asc' ? valA - valB : valB - valA;
      }
      if (sortField === 'points') {
        valA = a.isAdjusted ? a.adjustedPoints : a.points;
        valB = b.isAdjusted ? b.adjustedPoints : b.points;
      } else if (sortField === 'hitting') {
        valA = a.hittingPoints;
        valB = b.hittingPoints;
      } else if (sortField === 'pitching') {
        valA = a.pitchingPoints;
        valB = b.pitchingPoints;
      } else if (CATEGORIES.some(c => c.key === sortField)) {
        const isRawSort = showRawStats || selectedYear === 'ALL';
        if (isRawSort) {
          let rawA = a.rawStats?.[sortField];
          let rawB = b.rawStats?.[sortField];
          if (rawA === undefined || rawA === null) {
            if (sortField === 'OBP' && a.year === 2012) rawA = a.rawStats?.AVG;
            else if (sortField === 'QS' && a.year <= 2013) rawA = a.rawStats?.W;
            else if (sortField === 'SVHLD' && a.year <= 2018) rawA = a.rawStats?.SV;
          }
          if (rawB === undefined || rawB === null) {
            if (sortField === 'OBP' && b.year === 2012) rawB = b.rawStats?.AVG;
            else if (sortField === 'QS' && b.year <= 2013) rawB = b.rawStats?.W;
            else if (sortField === 'SVHLD' && b.year <= 2018) rawB = b.rawStats?.SV;
          }

          if (rawA !== undefined && rawA !== null && rawB !== undefined && rawB !== null) {
            valA = parseFloat(rawA) || 0;
            valB = parseFloat(rawB) || 0;
          } else {
            valA = a.categoryRanks[sortField] || 0;
            valB = b.categoryRanks[sortField] || 0;
          }
        } else {
          valA = a.categoryRanks[sortField] || 0;
          valB = b.categoryRanks[sortField] || 0;
        }
      } else {
        valA = a[sortField];
        valB = b[sortField];
      }

      if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
      if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
      return a.place - b.place;
    });
    return list;
  }, [filteredFinishes, sortField, sortDirection, showRawStats, selectedYear]);

  // Unique list of owners across history
  const allOwners = useMemo(() => {
    const set = new Set();
    rawFinishes.forEach(r => {
      const o = normalizeOwner(r.owner);
      if (o) set.add(o);
    });
    return Array.from(set).sort();
  }, [rawFinishes]);

  // Handle table header sorting
  const handleSort = (field) => {
    if (sortField === field) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      const cat = CATEGORIES.find(c => c.key === field);
      const isRawSort = showRawStats || selectedYear === 'ALL';
      const preferAsc = field === 'place' || (isRawSort && cat && !cat.higherIsBetter);
      setSortDirection(preferAsc ? 'asc' : 'desc');
    }
  };

  // Champion for selected single season (hero card)
  const selectedSeasonChampion = useMemo(() => {
    if (selectedYear === 'ALL' || selectedYear === '2026') return null;
    const yr = parseInt(selectedYear, 10);
    const seasonRows = augmentedFinishes.filter(r => r.year === yr).sort((a, b) => a.place - b.place);
    if (!seasonRows.length) return null;
    const champ = seasonRows[0];
    const runnerUp = seasonRows[1] || null;
    const thirdPlace = seasonRows[2] || null;
    const margin = runnerUp ? (champ.points - runnerUp.points).toFixed(1) : null;
    return {
      year: yr,
      champ,
      runnerUp,
      thirdPlace,
      margin,
      totalTeams: seasonRows.length
    };
  }, [selectedYear, augmentedFinishes]);

  // Highlights and Outliers data calculations
  const highlightsData = useMemo(() => {
    return computeHighlightsData(augmentedFinishes, adjustRoster, adjustMlb, exclude2020Lowlights);
  }, [augmentedFinishes, adjustRoster, adjustMlb, exclude2020Lowlights]);

  // Franchise records calculations
  const franchiseRecords = useMemo(() => {
    return computeFranchiseRecords(augmentedFinishes);
  }, [augmentedFinishes]);

  // Sorting handler for Franchise Hall leaderboard table
  const handleHallSort = (field) => {
    if (hallSortField === field) {
      setHallSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setHallSortField(field);
      const preferAsc = field === 'owner' || field === 'avgPlace' || field === 'bestPlace';
      setHallSortDirection(preferAsc ? 'asc' : 'desc');
    }
  };

  // Sorted list for Franchise Career Leaderboard
  const sortedFranchiseRecords = useMemo(() => {
    const list = [...franchiseRecords];
    list.sort((a, b) => {
      let valA, valB;
      if (hallSortField === 'owner') {
        valA = a.owner.toLowerCase();
        valB = b.owner.toLowerCase();
        return hallSortDirection === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
      }
      if (hallSortField === 'titles') {
        valA = a.titles;
        valB = b.titles;
        if (valA === valB) {
          if (b.podiums !== a.podiums) return b.podiums - a.podiums;
          return b.totalPoints - a.totalPoints;
        }
      } else if (hallSortField === 'podiums') {
        valA = a.podiums;
        valB = b.podiums;
        if (valA === valB) return b.titles - a.titles;
      } else if (hallSortField === 'seasons') {
        valA = a.seasons;
        valB = b.seasons;
      } else if (hallSortField === 'avgPlace') {
        valA = a.avgPlaceNum;
        valB = b.avgPlaceNum;
        if (valA === valB) return b.titles - a.titles;
      } else if (hallSortField === 'bestPlace') {
        valA = a.bestPlace;
        valB = b.bestPlace;
        if (valA === valB) return a.avgPlaceNum - b.avgPlaceNum;
      } else if (hallSortField === 'worstPlace') {
        valA = a.worstPlace;
        valB = b.worstPlace;
        if (valA === valB) return a.avgPlaceNum - b.avgPlaceNum;
      } else if (hallSortField === 'totalPoints') {
        valA = a.totalPoints;
        valB = b.totalPoints;
      } else if (hallSortField === 'ptsPerSeason') {
        valA = a.ptsPerSeasonNum;
        valB = b.ptsPerSeasonNum;
      } else {
        valA = a[hallSortField];
        valB = b[hallSortField];
      }

      if (valA < valB) return hallSortDirection === 'asc' ? -1 : 1;
      if (valA > valB) return hallSortDirection === 'asc' ? 1 : -1;
      return 0;
    });
    return list;
  }, [franchiseRecords, hallSortField, hallSortDirection]);

  // Label for current Franchise Hall sort
  const hallSortLabel = useMemo(() => {
    const labels = {
      titles: 'Championships',
      podiums: 'Podiums',
      seasons: 'Seasons',
      avgPlace: 'Average Finish',
      bestPlace: 'Best Finish',
      worstPlace: 'Worst Finish',
      totalPoints: 'Total Points',
      ptsPerSeason: 'Points / Season',
      owner: 'Owner Name',
    };
    return labels[hallSortField] || hallSortField;
  }, [hallSortField]);

  // Find team object for onOwnerClick
  const getOwnerTeamObj = (ownerName) => {
    const norm = normalizeOwner(ownerName).toLowerCase();
    return Object.values(TEAMS).find(t => {
      const tNorm = normalizeOwner(t.owner).toLowerCase();
      return tNorm === norm;
    }) || { owner: normalizeOwner(ownerName), name: normalizeOwner(ownerName) };
  };

  // Filtered categories for stat records
  const displayedStatCategories = useMemo(() => {
    if (selectedStatCategory === 'ALL') return CATEGORIES;
    const cat = CATEGORIES.find(c => c.key === selectedStatCategory);
    if (cat) return [cat];
    const leg = LEGACY_CATEGORIES.find(c => c.key === selectedStatCategory);
    if (leg) return [leg];
    return CATEGORIES;
  }, [selectedStatCategory]);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-8 animate-fade-in">
      {/* HEADER & BRANDING */}
      <HistoryHeaderNav
        activeSubTab={activeSubTab}
        setActiveSubTab={handleSubTabChange}
        augmentedFinishesCount={augmentedFinishes.length}
        dataSource={dataSource}
        loading={loading}
        onOpenMethodologyModal={() => setShowMethodologyModal(true)}
      />

      {loading && augmentedFinishes.length === 0 && (
        <div className="bg-slate-900 border border-slate-800 rounded-3xl p-12 text-center shadow-xl space-y-4">
          <div className="inline-block animate-spin rounded-full h-10 w-10 border-4 border-blue-500 border-t-transparent mb-2"></div>
          <div className="font-extrabold text-white text-lg">Loading Historical Finishes...</div>
          <p className="text-slate-400 text-xs">
            Querying Supabase database archive and compiling multi-era records...
          </p>
        </div>
      )}

      {/* SUB-TAB 1: ANNUAL STANDINGS & YEAR-BY-YEAR DATABASE */}
      {activeSubTab === 'standings' && (
        <HistoryStandingsTab
          selectedYear={selectedYear}
          setSelectedYear={setSelectedYear}
          selectedOwner={selectedOwner}
          setSelectedOwner={setSelectedOwner}
          allOwners={allOwners}
          currentCategoriesOnly={currentCategoriesOnly}
          setCurrentCategoriesOnly={setCurrentCategoriesOnly}
          showRawStats={showRawStats}
          setShowRawStats={setShowRawStats}
          adjustRoster={adjustRoster}
          setAdjustRoster={setAdjustRoster}
          adjustMlb={adjustMlb}
          setAdjustMlb={setAdjustMlb}
          selectedSeasonChampion={selectedSeasonChampion}
          sortedFinishes={sortedFinishes}
          handleSort={handleSort}
          onOwnerClick={onOwnerClick}
          getOwnerTeamObj={getOwnerTeamObj}
        />
      )}

      {/* SUB-TAB 2: HIGHLIGHTS & LOWLIGHTS OF FINISHES (ERA ADJUSTED) */}
      {activeSubTab === 'highlights' && (
        <HistoryHighlightsTab
          highlightsData={highlightsData}
          adjustRoster={adjustRoster}
          setAdjustRoster={setAdjustRoster}
          adjustMlb={adjustMlb}
          setAdjustMlb={setAdjustMlb}
          exclude2020Lowlights={exclude2020Lowlights}
          setExclude2020Lowlights={setExclude2020Lowlights}
          highlightFilter={highlightFilter}
          setHighlightFilter={setHighlightFilter}
          highlightViewMode={highlightViewMode}
          setHighlightViewMode={setHighlightViewMode}
          marginsGapType={marginsGapType}
          setMarginsGapType={setMarginsGapType}
          selectedStatCategory={selectedStatCategory}
          setSelectedStatCategory={setSelectedStatCategory}
          displayedStatCategories={displayedStatCategories}
        />
      )}

      {/* SUB-TAB 3: ALL-TIME FRANCHISE HALL & PLACEMENT MATRIX */}
      {activeSubTab === 'records' && (
        <FranchiseHallOfFameTab
          sortedFranchiseRecords={sortedFranchiseRecords}
          franchiseRecords={franchiseRecords}
          hallSortField={hallSortField}
          hallSortDirection={hallSortDirection}
          hallSortLabel={hallSortLabel}
          handleHallSort={handleHallSort}
          onOwnerClick={onOwnerClick}
          getOwnerTeamObj={getOwnerTeamObj}
        />
      )}

      {/* METHODOLOGY EXPLAINER MODAL */}
      <EraAdjustmentModal
        isOpen={showMethodologyModal}
        onClose={() => setShowMethodologyModal(false)}
      />
    </div>
  );
}
