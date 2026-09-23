// src/views/FullRosterView.jsx
import { useMemo, useState } from 'react';
import { TEAMS, getDateFromPeriodId } from '../schedule';
import { SCORING_CATS, LINEUP_SLOTS, aggregateStats } from '../utils/scoring';
import { getPlayerAcquisition } from '../utils/acquisition';
import TeamAvatar from '../components/TeamAvatar';

const BATTER_SLOT_ORDER = {
  0: 0,   // C
  1: 1,   // 1B
  2: 2,   // 2B
  3: 3,   // 3B
  4: 4,   // SS
  6: 5,   // 2B/SS
  7: 6,   // 1B/3B
  19: 7,  // IF
  5: 8,   // OF
  11: 9,  // DH
  12: 10, // UTIL
};

const PITCHER_SLOT_ORDER = {
  14: 0,  // SP
  15: 1,  // RP
  13: 2,  // P
};

function formatOBP(val) {
  if (val == null) return '.0000';
  const n = typeof val === 'number' ? val : parseFloat(val);
  return !isNaN(n) ? n.toFixed(4).replace(/^0/, '') : String(val);
}

function formatRate(val, decimals = 2) {
  if (val == null) return (0).toFixed(decimals);
  const n = typeof val === 'number' ? val : parseFloat(val);
  return !isNaN(n) ? n.toFixed(decimals) : String(val);
}

// Calculate fantasy point / game score contribution for a daily batter record
function getBatterDailyScore(stats) {
  if (!stats) return 0;
  const r = parseFloat(stats.R ?? stats['20'] ?? 0);
  const hr = parseFloat(stats.HR ?? stats['5'] ?? 0);
  const rbi = parseFloat(stats.RBI ?? stats['21'] ?? 0);
  const sb = parseFloat(stats.SB ?? stats['23'] ?? 0);
  const h = parseFloat(stats.H ?? stats['1'] ?? 0);
  const bb = parseFloat(stats.BB ?? stats['10'] ?? 0);
  const so = parseFloat(stats.SO ?? stats['27'] ?? 0);
  const pa = parseFloat(stats.PA ?? stats['16'] ?? 0);

  if (pa === 0 && h === 0 && r === 0 && rbi === 0 && bb === 0) return 0;
  // Standard weighted game score: R*2 + HR*4 + RBI*2 + SB*2 + (H-HR)*1 + BB*1 - SO*0.5
  return parseFloat((r * 2 + hr * 4 + rbi * 2 + sb * 2 + Math.max(0, h - hr) * 1 + bb * 1 - so * 0.5).toFixed(1));
}

// Calculate fantasy score for an SP outing / start
function getSPStartScore(stats) {
  if (!stats) return 0;
  const ipRaw = parseFloat(stats.IP_raw ?? stats.IP ?? stats['34'] ?? 0);
  const ip = ipRaw / 3; // outs vs innings
  const er = parseFloat(stats.ER ?? stats['45'] ?? 0);
  const k = parseFloat(stats.K ?? stats['48'] ?? 0);
  const h = parseFloat(stats.H_Allowed ?? stats['37'] ?? 0);
  const bb = parseFloat(stats.BB_Allowed ?? stats['39'] ?? 0);
  const qs = parseFloat(stats.QS ?? stats['63'] ?? 0);

  if (ip === 0 && k === 0 && er === 0) return 0;
  // Pitcher Game Score style point metric: IP*3 + K*1 - ER*2 - H*0.5 - BB*0.5 + QS*4
  return parseFloat((ip * 3 + k * 1 - er * 2 - h * 0.5 - bb * 0.5 + (qs > 0 ? 4 : 0)).toFixed(1));
}

// Calculate fantasy score for an RP appearance
function getRPDailyScore(stats) {
  if (!stats) return 0;
  const ipRaw = parseFloat(stats.IP_raw ?? stats.IP ?? stats['34'] ?? 0);
  const ip = ipRaw / 3;
  const er = parseFloat(stats.ER ?? stats['45'] ?? 0);
  const k = parseFloat(stats.K ?? stats['48'] ?? 0);
  const sv = parseFloat(stats.SV ?? stats['57'] ?? 0);
  const hd = parseFloat(stats.HD ?? stats['60'] ?? 0);

  if (ip === 0 && k === 0 && sv === 0 && hd === 0) return 0;
  return parseFloat((ip * 3 + k * 1 - er * 2 + sv * 5 + hd * 3).toFixed(1));
}

// Render an interactive SVG Sparkline Trendline
function PlayerSparkline({ dataPoints, isSP = false, width = 130, height = 30 }) {
  const [hoveredPoint, setHoveredPoint] = useState(null);

  if (!dataPoints || dataPoints.length === 0) {
    return (
      <div className="flex items-center gap-1.5 text-xs text-gray-400 italic">
        <span className="inline-block w-2 h-2 rounded-full bg-gray-300"></span>
        <span>No recent outings</span>
      </div>
    );
  }

  // If only 1 data point, show single badge
  if (dataPoints.length === 1) {
    const pt = dataPoints[0];
    return (
      <div className="flex items-center gap-2">
        <span className="text-xs font-mono font-bold px-2 py-0.5 rounded bg-blue-50 text-blue-700 border border-blue-200">
          {pt.date}: {pt.val} pts
        </span>
      </div>
    );
  }

  const values = dataPoints.map(p => p.val);
  const minVal = Math.min(...values);
  const maxVal = Math.max(...values);
  const range = maxVal - minVal || 1;

  const padY = 5;
  const usableH = height - padY * 2;

  const coords = dataPoints.map((p, i) => {
    const x = (i / (dataPoints.length - 1)) * (width - 12) + 6;
    const y = height - padY - ((p.val - minVal) / range) * usableH;
    return { ...p, x, y };
  });

  const pathD = coords.reduce((acc, p, i) => `${acc} ${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)},${p.y.toFixed(1)}`, '');
  const areaD = `${pathD} L ${coords[coords.length - 1].x.toFixed(1)},${height} L ${coords[0].x.toFixed(1)},${height} Z`;

  const lastVal = values[values.length - 1];
  const firstVal = values[0];
  const isUp = lastVal >= firstVal;
  const strokeColor = isUp ? '#10b981' : '#f43f5e';
  const fillColor = isUp ? 'rgba(16, 185, 129, 0.12)' : 'rgba(244, 63, 94, 0.12)';

  return (
    <div className="relative inline-flex items-center gap-2 group select-none">
      <svg width={width} height={height} className="overflow-visible">
        <defs>
          <linearGradient id={`grad-${dataPoints[0].date}-${isUp}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={strokeColor} stopOpacity="0.25" />
            <stop offset="100%" stopColor={strokeColor} stopOpacity="0.0" />
          </linearGradient>
        </defs>

        {/* Baseline guide */}
        <line
          x1={coords[0].x}
          y1={height - padY - ((0 - minVal) / range) * usableH}
          x2={coords[coords.length - 1].x}
          y2={height - padY - ((0 - minVal) / range) * usableH}
          stroke="#e2e8f0"
          strokeDasharray="2 2"
          strokeWidth="1"
        />

        {/* Filled Area */}
        <path d={areaD} fill={fillColor} />

        {/* Polyline Path */}
        <path d={pathD} fill="none" stroke={strokeColor} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />

        {/* Points */}
        {coords.map((c, idx) => (
          <circle
            key={idx}
            cx={c.x}
            cy={c.y}
            r={isSP ? (c.isQS ? 4 : 2.5) : 2.5}
            fill={c.isQS ? '#eab308' : strokeColor}
            stroke={c.isQS ? '#854d0e' : '#ffffff'}
            strokeWidth={c.isQS ? 1.5 : 1}
            className="cursor-pointer transition-transform hover:scale-150"
            onMouseEnter={() => setHoveredPoint(c)}
            onMouseLeave={() => setHoveredPoint(null)}
          />
        ))}
      </svg>

      {/* Floating Hover Tooltip */}
      {hoveredPoint && (
        <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 z-30 pointer-events-none bg-slate-900 text-white text-[11px] rounded-lg shadow-xl px-2.5 py-1.5 whitespace-nowrap border border-slate-700 animate-fade-in">
          <div className="font-bold flex items-center gap-1.5 text-blue-300">
            <span>{hoveredPoint.date}</span>
            {hoveredPoint.isQS && <span className="bg-amber-400 text-slate-950 font-black px-1 rounded text-[9px]">QS</span>}
            <span className="text-gray-300 font-mono">({hoveredPoint.val} pts)</span>
          </div>
          {hoveredPoint.summary && <div className="text-gray-200 mt-0.5">{hoveredPoint.summary}</div>}
        </div>
      )}

      {/* Mini Trend Pill */}
      <span className={`text-[10px] font-black font-mono px-1.5 py-0.5 rounded ${
        isUp ? 'bg-emerald-50 text-emerald-700 border border-emerald-200' : 'bg-rose-50 text-rose-700 border border-rose-200'
      }`}>
        {isUp ? '↑' : '↓'} {dataPoints.length}{isSP ? ' GS' : ' G'}
      </span>
    </div>
  );
}

// Acquisition Chip Badge
function AcquisitionChip({ acqInfo }) {
  if (!acqInfo) return null;

  const typeConfig = {
    DRAFT: { bg: 'bg-indigo-50', text: 'text-indigo-700', border: 'border-indigo-200', icon: '🎯' },
    KEEPER: { bg: 'bg-purple-50', text: 'text-purple-700', border: 'border-purple-200', icon: '💎' },
    TRADE: { bg: 'bg-emerald-50', text: 'text-emerald-700', border: 'border-emerald-200', icon: '🤝' },
    WAIVER: { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-200', icon: '⚡' },
    FREEAGENT: { bg: 'bg-blue-50', text: 'text-blue-700', border: 'border-blue-200', icon: '📋' },
    ORIGINAL_ROSTER: { bg: 'bg-gray-100', text: 'text-gray-700', border: 'border-gray-300', icon: '🛡️' }
  };

  const conf = typeConfig[acqInfo.type] || typeConfig.ORIGINAL_ROSTER;

  return (
    <div
      className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold border ${conf.bg} ${conf.text} ${conf.border} shadow-2xs`}
      title={acqInfo.detailText}
    >
      <span>{conf.icon}</span>
      <span className="font-medium tracking-tight">{acqInfo.badgeText}</span>
    </div>
  );
}

export default function FullRosterView({ allStats = [], onOwnerClick, onPlayerClick, selectedSeason = 2026 }) {
  // Default to team 5 (Dan) or first team
  const [selectedTeamId, setSelectedTeamId] = useState(5);
  const [timeframe, setTimeframe] = useState('14d'); // '14d' | '30d' | 'season'
  const [positionFilter, setPositionFilter] = useState('ALL'); // 'ALL' | 'HITTERS' | 'PITCHERS' | 'STARTERS' | 'BENCH' | 'IL'
  const [searchQuery, setSearchQuery] = useState('');

  // 1. Identify latest scoring period with actual active gameplay in the loaded season
  const latestPeriod = useMemo(() => {
    if (!allStats.length) return 174;
    return allStats.reduce((max, r) => {
      const s = r.stats || {};
      const hasPlay = (
        (parseFloat(s.PA ?? s['16'] ?? s.AB ?? s['0'] ?? 0) > 0) ||
        (parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0) > 0) ||
        (parseFloat(s.H ?? s['1'] ?? 0) > 0) ||
        (parseFloat(s.K ?? s['48'] ?? 0) > 0)
      );
      return hasPlay ? Math.max(max, r.scoring_period_id || 0) : max;
    }, 0) || 174;
  }, [allStats]);

  // 2. Identify active roster for the selected team
  // We locate every player whose latest scoring period on this team represents current ownership
  const teamRoster = useMemo(() => {
    const tid = parseInt(selectedTeamId, 10);
    const playerMap = new Map();

    // Map all records for this team to get player entries
    allStats.forEach(r => {
      const pid = r.player_id;
      if (!pid) return;

      if (!playerMap.has(pid)) {
        playerMap.set(pid, {
          id: pid,
          name: r.full_name,
          records: [],
          latestPeriod: 0,
          latestSlotId: 16,
          isOnTeam: false
        });
      }

      const pEntry = playerMap.get(pid);
      pEntry.records.push(r);

      if (r.scoring_period_id > pEntry.latestPeriod) {
        pEntry.latestPeriod = r.scoring_period_id;
        pEntry.name = r.full_name;
        pEntry.isOnTeam = (r.team_id === tid);
        pEntry.latestSlotId = r.lineup_slot_id;
      }
    });

    // Filter to players whose latest record is on this team (or within last 3 periods)
    const rosterPlayers = [];
    playerMap.forEach(p => {
      if (p.isOnTeam && p.latestPeriod >= Math.max(1, latestPeriod - 3)) {
        rosterPlayers.push(p);
      }
    });

    return rosterPlayers;
  }, [allStats, selectedTeamId, latestPeriod]);

  // 3. Process each player with Season Totals, Recent Outings, Trendline & Acquisition
  const enrichedPlayers = useMemo(() => {
    const tid = parseInt(selectedTeamId, 10);

    return teamRoster.map(p => {
      const records = p.records.sort((a, b) => a.scoring_period_id - b.scoring_period_id);
      
      // Pitcher detection
      const hasPitchingStats = records.some(r => {
        const s = r.stats || {};
        return (
          parseFloat(s.IP ?? s['34'] ?? 0) > 0 ||
          parseFloat(s.K ?? s['48'] ?? 0) > 0 ||
          parseFloat(s.SV ?? s['57'] ?? 0) > 0 ||
          parseFloat(s.HD ?? s['60'] ?? 0) ||
          parseFloat(s.ER ?? s['45'] ?? 0) > 0
        );
      });
      const isPitcher = (p.latestSlotId >= 13 && p.latestSlotId <= 15) || hasPitchingStats;

      // Starting pitcher detection (specifically accounts for user's request)
      const isSP = isPitcher && (
        p.latestSlotId === 14 ||
        records.some(r => {
          const s = r.stats || {};
          const ipRaw = parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0);
          const gs = parseFloat(s.GS ?? s['33'] ?? 0);
          const qs = parseFloat(s.QS ?? s['63'] ?? 0);
          return gs > 0 || qs > 0 || ipRaw >= 9; // 9 outs = 3.0 IP
        })
      );

      // Season stats aggregated across all appearances
      const seasonStats = aggregateStats(records, { includeAll: true });

      // Build recent performance points & summary according to player role
      let sparklineData = [];
      let recentStats = null;
      let recentSummaryText = '';

      if (isSP) {
        // --- STARTING PITCHER CADENCE HANDLING ---
        // SPs pitch 1-2 times per week. Plot per-start / outing points rather than daily flat zeros!
        const starts = records.filter(r => {
          const s = r.stats || {};
          const ipRaw = parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0);
          const gs = parseFloat(s.GS ?? s['33'] ?? 0);
          const qs = parseFloat(s.QS ?? s['63'] ?? 0);
          return gs > 0 || qs > 0 || ipRaw >= 9 || r.lineup_slot_id === 14;
        });

        // Determine slice based on timeframe
        let filteredStarts = starts;
        if (timeframe === '14d') {
          // Last 3-4 starts
          filteredStarts = starts.slice(-4);
        } else if (timeframe === '30d') {
          // Last 6-8 starts
          filteredStarts = starts.slice(-7);
        }

        sparklineData = filteredStarts.map(st => {
          const s = st.stats || {};
          const dateStr = getDateFromPeriodId(st.scoring_period_id, selectedSeason);
          const shortDate = dateStr ? dateStr.slice(5) : `P${st.scoring_period_id}`;
          const val = getSPStartScore(s);
          const ipRaw = parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0);
          const ip = ipRaw / 3;
          const er = parseFloat(s.ER ?? s['45'] ?? 0);
          const k = parseFloat(s.K ?? s['48'] ?? 0);
          const isQS = (parseFloat(s.QS ?? s['63'] ?? 0) > 0) || (ip >= 6 && er <= 3);

          return {
            date: shortDate,
            val,
            isQS,
            summary: `${formatRate(ip, 1)} IP · ${er} ER · ${k} K${isQS ? ' (QS)' : ''}`
          };
        });

        recentStats = aggregateStats(filteredStarts, { includeAll: true });
        const qsCount = filteredStarts.filter(st => {
          const s = st.stats || {};
          const ipRaw = parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0);
          const ip = ipRaw / 3;
          const er = parseFloat(s.ER ?? s['45'] ?? 0);
          return (parseFloat(s.QS ?? s['63'] ?? 0) > 0) || (ip >= 6 && er <= 3);
        }).length;

        recentSummaryText = `${filteredStarts.length} GS · ${formatRate(recentStats.IP, 1)} IP · ${formatRate(recentStats.ERA, 2)} ERA · ${recentStats.K || 0} K (${qsCount} QS)`;

      } else if (isPitcher) {
        // --- RELIEF PITCHER CADENCE ---
        const minPeriod = timeframe === '14d' ? Math.max(1, latestPeriod - 14) : timeframe === '30d' ? Math.max(1, latestPeriod - 30) : 1;
        let recentRecords = records.filter(r => r.scoring_period_id >= minPeriod);

        // Active appearances with work
        let appRecords = recentRecords.filter(r => {
          const s = r.stats || {};
          return (parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0) > 0) || (parseFloat(s.K ?? s['48'] ?? 0) > 0) || (parseFloat(s.SV ?? s['57'] ?? 0) > 0);
        });

        // Fallback: If no work in calendar window (e.g. IL), take player's last active relief appearances
        if (appRecords.length === 0 && timeframe !== 'season') {
          const allActive = records.filter(r => {
            const s = r.stats || {};
            return (parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0) > 0) || (parseFloat(s.K ?? s['48'] ?? 0) > 0) || (parseFloat(s.SV ?? s['57'] ?? 0) > 0);
          });
          allActive.sort((a, b) => a.scoring_period_id - b.scoring_period_id);
          const sliceCount = timeframe === '14d' ? 5 : 10;
          appRecords = allActive.slice(-sliceCount);
          recentRecords = appRecords;
        }

        sparklineData = appRecords.map(r => {
          const s = r.stats || {};
          const dateStr = getDateFromPeriodId(r.scoring_period_id, selectedSeason);
          const shortDate = dateStr ? dateStr.slice(5) : `P${r.scoring_period_id}`;
          const val = getRPDailyScore(s);
          const ipRaw = parseFloat(s.IP_raw ?? s.IP ?? s['34'] ?? 0);
          const ip = ipRaw / 3;
          const k = parseFloat(s.K ?? s['48'] ?? 0);
          const sv = parseFloat(s.SV ?? s['57'] ?? 0);
          const hd = parseFloat(s.HD ?? s['60'] ?? 0);

          return {
            date: shortDate,
            val,
            isQS: false,
            summary: `${formatRate(ip, 1)} IP · ${k} K · ${(sv + hd)} SV+HD`
          };
        });

        recentStats = aggregateStats(recentRecords, { includeAll: true });
        recentSummaryText = `${appRecords.length} App · ${recentStats['SV+HDs'] || 0} SV+HD · ${recentStats.K || 0} K · ${formatRate(recentStats.ERA, 2)} ERA`;

      } else {
        // --- BATTER CADENCE ---
        const minPeriod = timeframe === '14d' ? Math.max(1, latestPeriod - 14) : timeframe === '30d' ? Math.max(1, latestPeriod - 30) : 1;
        let recentRecords = records.filter(r => r.scoring_period_id >= minPeriod);

        // Days with actual plate appearances / appearances
        let appRecords = recentRecords.filter(r => {
          const s = r.stats || {};
          return (parseFloat(s.PA ?? s['16'] ?? 0) > 0) || (parseFloat(s.AB ?? s['0'] ?? 0) > 0) || (parseFloat(s.H ?? s['1'] ?? 0) > 0);
        });

        // Fallback: If no games in exact calendar window (e.g. recent IL return), show player's last active games
        if (appRecords.length === 0 && timeframe !== 'season') {
          const allActive = records.filter(r => {
            const s = r.stats || {};
            return (parseFloat(s.PA ?? s['16'] ?? 0) > 0) || (parseFloat(s.AB ?? s['0'] ?? 0) > 0) || (parseFloat(s.H ?? s['1'] ?? 0) > 0);
          });
          allActive.sort((a, b) => a.scoring_period_id - b.scoring_period_id);
          const sliceCount = timeframe === '14d' ? 10 : 20;
          appRecords = allActive.slice(-sliceCount);
          recentRecords = appRecords;
        }

        sparklineData = appRecords.map(r => {
          const s = r.stats || {};
          const dateStr = getDateFromPeriodId(r.scoring_period_id, selectedSeason);
          const shortDate = dateStr ? dateStr.slice(5) : `P${r.scoring_period_id}`;
          const val = getBatterDailyScore(s);
          const h = parseFloat(s.H ?? s['1'] ?? 0);
          const hr = parseFloat(s.HR ?? s['5'] ?? 0);
          const rbi = parseFloat(s.RBI ?? s['21'] ?? 0);
          const rRuns = parseFloat(s.R ?? s['20'] ?? 0);

          return {
            date: shortDate,
            val,
            isQS: false,
            summary: `${h} H · ${hr} HR · ${rbi} RBI · ${rRuns} R`
          };
        });

        recentStats = aggregateStats(recentRecords, { includeAll: true });
        const obpStr = formatOBP(recentStats.OBP);
        recentSummaryText = `${appRecords.length} G · ${recentStats.HR || 0} HR · ${recentStats.RBI || 0} RBI · ${recentStats.SB || 0} SB · ${obpStr} OBP`;
      }

      // Acquisition info
      const acquisition = getPlayerAcquisition(p.id, tid);

      return {
        ...p,
        isPitcher,
        isSP,
        slotName: LINEUP_SLOTS[p.latestSlotId] || 'BN',
        slotId: p.latestSlotId,
        seasonStats,
        recentStats,
        recentSummaryText,
        sparklineData,
        acquisition
      };
    });
  }, [teamRoster, selectedTeamId, timeframe, latestPeriod, selectedSeason]);

  // 4. Group into Starters, Bench, IL
  const groupedRoster = useMemo(() => {
    let list = enrichedPlayers;

    // Search query filter
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      list = list.filter(p => p.name.toLowerCase().includes(q));
    }

    // Position filter
    if (positionFilter === 'HITTERS') {
      list = list.filter(p => !p.isPitcher);
    } else if (positionFilter === 'PITCHERS') {
      list = list.filter(p => p.isPitcher);
    } else if (positionFilter === 'STARTERS') {
      list = list.filter(p => p.slotId !== 16 && p.slotId !== 17);
    } else if (positionFilter === 'BENCH') {
      list = list.filter(p => p.slotId === 16);
    } else if (positionFilter === 'IL') {
      list = list.filter(p => p.slotId === 17);
    }

    // Split by role
    const startingBatters = list
      .filter(p => !p.isPitcher && p.slotId !== 16 && p.slotId !== 17)
      .sort((a, b) => (BATTER_SLOT_ORDER[a.slotId] ?? 99) - (BATTER_SLOT_ORDER[b.slotId] ?? 99));

    const startingPitchers = list
      .filter(p => p.isPitcher && p.slotId !== 16 && p.slotId !== 17)
      .sort((a, b) => (PITCHER_SLOT_ORDER[a.slotId] ?? 99) - (PITCHER_SLOT_ORDER[b.slotId] ?? 99));

    const bench = list.filter(p => p.slotId === 16);
    const il = list.filter(p => p.slotId === 17);

    return {
      startingBatters,
      startingPitchers,
      bench,
      il,
      totalCount: list.length
    };
  }, [enrichedPlayers, searchQuery, positionFilter]);

  const activeTeam = TEAMS[selectedTeamId] || { name: `Team ${selectedTeamId}` };

  return (
    <div className="space-y-6 animate-fade-in">
      {/* --- HERO HEADER --- */}
      <div className="bg-slate-900 border border-slate-800 text-white p-6 rounded-3xl shadow-xl backdrop-blur-xl relative overflow-hidden">
        <div className="absolute -right-10 -bottom-10 w-80 h-80 bg-blue-600/10 rounded-full blur-3xl pointer-events-none" />
        <div className="absolute -left-10 -top-10 w-80 h-80 bg-indigo-600/10 rounded-full blur-3xl pointer-events-none" />

        <div className="flex flex-col md:flex-row md:items-center justify-between gap-6 relative z-10">
          <div>
            <div className="flex items-center gap-2 mb-1.5">
              <span className="text-xl">📋</span>
              <span className="text-xs font-black uppercase tracking-wider text-blue-400 bg-blue-950/60 border border-blue-800/60 px-2.5 py-0.5 rounded-full">
                Depth Charts & Roster Cadence
              </span>
            </div>
            <h1 className="text-2xl md:text-3xl font-black tracking-tight text-white flex items-center gap-3">
              Full Lineups & Trendlines
            </h1>
            <div className="flex items-center gap-2 mt-2">
              <button
                type="button"
                onClick={() => onOwnerClick && onOwnerClick({ id: selectedTeamId, name: activeTeam.name, owner: activeTeam.owner })}
                className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-slate-800/80 hover:bg-slate-700 text-xs font-bold text-blue-300 border border-slate-700 transition cursor-pointer"
                title="View Owner Profile"
              >
                <TeamAvatar team={{ id: selectedTeamId, name: activeTeam.name }} size="xs" />
                <span>{activeTeam.name}</span>
                <span className="text-[10px] text-slate-400 font-normal">({activeTeam.owner || 'Owner'} &rarr;)</span>
              </button>
            </div>
            <p className="text-sm text-slate-300 mt-2 max-w-2xl">
              Inspect starting lineups, bench reserves, and injured lists with inline trendlines tailored for starting pitchers (per-start outing curve) and position players (daily scoring form).
            </p>
          </div>

          {/* Timeframe selector */}
          <div className="bg-slate-950/80 p-1.5 rounded-2xl border border-slate-800 flex items-center gap-1 shadow-inner self-start md:self-auto">
            <button
              type="button"
              onClick={() => setTimeframe('14d')}
              className={`px-3 py-1.5 rounded-xl text-xs font-black transition cursor-pointer ${
                timeframe === '14d' ? 'bg-blue-600 text-white shadow-md' : 'text-slate-400 hover:text-white'
              }`}
            >
              Last 14d / 3 Starts
            </button>
            <button
              type="button"
              onClick={() => setTimeframe('30d')}
              className={`px-3 py-1.5 rounded-xl text-xs font-black transition cursor-pointer ${
                timeframe === '30d' ? 'bg-blue-600 text-white shadow-md' : 'text-slate-400 hover:text-white'
              }`}
            >
              Last 30d / 6 Starts
            </button>
            <button
              type="button"
              onClick={() => setTimeframe('season')}
              className={`px-3 py-1.5 rounded-xl text-xs font-black transition cursor-pointer ${
                timeframe === 'season' ? 'bg-blue-600 text-white shadow-md' : 'text-slate-400 hover:text-white'
              }`}
            >
              Full Season
            </button>
          </div>
        </div>

        {/* --- TEAM SELECTOR TABS --- */}
        <div className="mt-6 pt-5 border-t border-slate-800/80">
          <div className="flex items-center gap-2 overflow-x-auto pb-2 scrollbar-none">
            {Object.entries(TEAMS)
              .filter(([id]) => parseInt(id, 10) !== 99)
              .map(([id, team]) => {
                const isSelected = parseInt(id, 10) === parseInt(selectedTeamId, 10);
                return (
                  <button
                    key={id}
                    type="button"
                    onClick={() => setSelectedTeamId(parseInt(id, 10))}
                    className={`flex items-center gap-2.5 px-3.5 py-2 rounded-2xl font-bold text-xs shrink-0 transition-all cursor-pointer border ${
                      isSelected
                        ? 'bg-blue-600 text-white border-blue-400 shadow-md ring-2 ring-blue-400/30'
                        : 'bg-slate-800/80 text-slate-300 border-slate-700/60 hover:bg-slate-700/80 hover:text-white'
                    }`}
                  >
                    <TeamAvatar team={{ id: parseInt(id, 10), name: team.name }} size="xs" />
                    <span>{team.name}</span>
                  </button>
                );
              })}
          </div>
        </div>
      </div>

      {/* --- FILTER BAR & SUMMARY BAR --- */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 bg-white p-4 rounded-2xl border border-gray-200 shadow-xs">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs font-bold text-gray-500 uppercase tracking-wider">Filter:</span>
          {['ALL', 'HITTERS', 'PITCHERS', 'STARTERS', 'BENCH', 'IL'].map(f => (
            <button
              key={f}
              type="button"
              onClick={() => setPositionFilter(f)}
              className={`px-3 py-1 rounded-xl text-xs font-bold transition cursor-pointer ${
                positionFilter === f
                  ? 'bg-slate-900 text-white shadow-xs'
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200 hover:text-gray-900'
              }`}
            >
              {f}
            </button>
          ))}
        </div>

        <div className="relative w-full sm:w-64">
          <input
            type="text"
            placeholder="Search players..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full text-xs font-medium pl-8 pr-3 py-2 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 focus:bg-white transition"
          />
          <span className="absolute left-2.5 top-2.5 text-gray-400 text-xs">🔍</span>
          {searchQuery && (
            <button
              type="button"
              onClick={() => setSearchQuery('')}
              className="absolute right-2.5 top-2 text-gray-400 hover:text-gray-600 text-xs font-bold cursor-pointer"
            >
              &times;
            </button>
          )}
        </div>
      </div>

      {/* --- ROSTER SECTION RENDERER --- */}
      {groupedRoster.totalCount === 0 ? (
        <div className="p-12 text-center bg-white rounded-2xl border border-gray-200 text-gray-400">
          No players match your search or filter.
        </div>
      ) : (
        <div className="space-y-6">
          {/* 1. STARTING BATTERS */}
          {groupedRoster.startingBatters.length > 0 && (
            <RosterTableSection
              title="⚾ Starting Lineup — Batters"
              badge="Active Hitters"
              players={groupedRoster.startingBatters}
              onPlayerClick={onPlayerClick}
              selectedTeamId={selectedTeamId}
            />
          )}

          {/* 2. STARTING PITCHERS & BULLPEN */}
          {groupedRoster.startingPitchers.length > 0 && (
            <RosterTableSection
              title="🔥 Starting Pitchers & Bullpen"
              badge="Active Pitchers"
              players={groupedRoster.startingPitchers}
              onPlayerClick={onPlayerClick}
              selectedTeamId={selectedTeamId}
            />
          )}

          {/* 3. BENCH RESERVES */}
          {groupedRoster.bench.length > 0 && (
            <RosterTableSection
              title="🪑 Bench Reserves"
              badge={`${groupedRoster.bench.length} Reserves`}
              players={groupedRoster.bench}
              onPlayerClick={onPlayerClick}
              selectedTeamId={selectedTeamId}
            />
          )}

          {/* 4. INJURED LIST */}
          {groupedRoster.il.length > 0 && (
            <RosterTableSection
              title="🩹 Injured List (IL)"
              badge={`${groupedRoster.il.length} Inactive`}
              players={groupedRoster.il}
              onPlayerClick={onPlayerClick}
              selectedTeamId={selectedTeamId}
            />
          )}
        </div>
      )}
    </div>
  );
}

// Sub-component: Clean Roster Table Section
function RosterTableSection({ title, badge, players, onPlayerClick, selectedTeamId }) {
  return (
    <div className="bg-white rounded-3xl border border-gray-200 shadow-sm overflow-hidden">
      <div className="bg-gray-50/80 px-6 py-4 border-b border-gray-200 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h2 className="text-base font-black text-gray-900">{title}</h2>
          <span className="text-[11px] font-black uppercase px-2.5 py-0.5 rounded-full bg-blue-100 text-blue-800">
            {badge}
          </span>
        </div>
        <span className="text-xs text-gray-400 font-semibold">
          {players.length} Player{players.length !== 1 ? 's' : ''}
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm border-collapse">
          <thead>
            <tr className="bg-gray-100/70 text-[11px] font-black uppercase text-gray-500 tracking-wider border-b border-gray-200">
              <th className="py-3 px-4 w-16 text-center">Slot</th>
              <th className="py-3 px-4">Player & Acquisition</th>
              <th className="py-3 px-4 text-center">Season Line</th>
              <th className="py-3 px-4">Recent Performance</th>
              <th className="py-3 px-4 text-center">Trendline</th>
              <th className="py-3 px-4 text-right w-24">Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {players.map(p => {
              const stats = p.seasonStats || {};
              const slot = p.slotName;

              return (
                <tr
                  key={p.id}
                  onClick={() => onPlayerClick && onPlayerClick(p.id, p.name, selectedTeamId)}
                  className="hover:bg-blue-50/60 transition-colors cursor-pointer group"
                >
                  {/* SLOT BADGE */}
                  <td className="py-3.5 px-4 text-center">
                    <span className={`inline-block px-2.5 py-1 rounded-lg text-xs font-black uppercase ${
                      slot === 'IL'
                        ? 'bg-rose-100 text-rose-800'
                        : slot === 'Bench' || slot === 'BN'
                        ? 'bg-amber-100 text-amber-800'
                        : slot === 'SP'
                        ? 'bg-emerald-100 text-emerald-800'
                        : slot === 'RP'
                        ? 'bg-teal-100 text-teal-800'
                        : 'bg-blue-100 text-blue-800'
                    }`}>
                      {slot}
                    </span>
                  </td>

                  {/* PLAYER & ACQUISITION */}
                  <td className="py-3.5 px-4">
                    <div className="flex flex-col gap-1">
                      <span className="font-bold text-gray-900 group-hover:text-blue-600 transition-colors flex items-center gap-2">
                        {p.name}
                        {p.isSP && (
                          <span className="text-[10px] font-black uppercase px-1.5 py-0.2 rounded bg-emerald-50 text-emerald-700 border border-emerald-200">
                            Starter
                          </span>
                        )}
                      </span>
                      <div>
                        <AcquisitionChip acqInfo={p.acquisition} />
                      </div>
                    </div>
                  </td>

                  {/* SEASON TOTALS */}
                  <td className="py-3.5 px-4 text-center">
                    <div className="text-xs font-mono font-medium text-gray-700">
                      {p.isPitcher ? (
                        <>
                          <div className="font-bold text-gray-900">
                            {stats.IP ? `${Math.floor(parseFloat(stats.IP))}.${Math.round((parseFloat(stats.IP) % 1) * 3)} IP` : '0 IP'} · {stats.K || 0} K
                          </div>
                          <div className="text-[11px] text-gray-500 mt-0.5">
                            {formatRate(stats.ERA, 2)} ERA · {formatRate(stats.WHIP, 2)} WHIP · {p.isSP ? `${stats.QS || 0} QS${parseFloat(stats.GS ?? stats['33'] ?? 0) > 0 ? ` (${((parseFloat(stats.QS ?? stats['63'] ?? 0) / parseFloat(stats.GS ?? stats['33'] ?? 0)) * 100).toFixed(0)}%)` : ''}` : `${stats['SV+HDs'] || 0} SV+HD`}
                          </div>
                        </>
                      ) : (
                        <>
                          <div className="font-bold text-gray-900">
                            {stats.PA || 0} PA · {stats.HR || 0} HR · {stats.RBI || 0} RBI
                          </div>
                          <div className="text-[11px] text-gray-500 mt-0.5">
                            {stats.R || 0} R · {stats.SB || 0} SB · {formatOBP(stats.OBP)} OBP
                          </div>
                        </>
                      )}
                    </div>
                  </td>

                  {/* RECENT PERFORMANCE SUMMARY */}
                  <td className="py-3.5 px-4">
                    <div className="text-xs text-gray-800 font-medium max-w-xs">
                      {p.recentSummaryText || '-'}
                    </div>
                    {p.isSP && (
                      <div className="text-[10px] text-gray-400 mt-0.5 italic">
                        *Tracked per start (1-2 outings/week)
                      </div>
                    )}
                  </td>

                  {/* SPARKLINE TRENDLINE */}
                  <td className="py-3.5 px-4 text-center">
                    <PlayerSparkline dataPoints={p.sparklineData} isSP={p.isSP} width={130} height={28} />
                  </td>

                  {/* ACTION ARROW */}
                  <td className="py-3.5 px-4 text-right">
                    <span className="text-xs font-bold text-blue-600 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-end gap-1">
                      Details &rarr;
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
