import { supabase } from '../../../supabaseClient.js';
import {
  getEffectiveRank,
  getEffectivePrice,
  getEffectiveQS,
  formatPlayerStats,
  getFriendlyOwnerName,
  mappedDraft2026History
} from '../utils/draftCalculations.js';
import { findMlbTeam } from '../../../utils/mlbTeams.js';

// --- SPOKEN PRONUNCIATION & ABBREVIATION DICTIONARIES ---
export function formatPositionForSpeech(pos) {
  if (!pos || typeof pos !== 'string') return 'player';
  const mapping = {
    '1B': 'first base',
    '2B': 'second base',
    '3B': 'third base',
    'SS': 'shortstop',
    'C': 'catcher',
    'OF': 'outfield',
    'LF': 'left field',
    'CF': 'center field',
    'RF': 'right field',
    'DH': 'designated hitter',
    'SP': 'starting pitcher',
    'RP': 'relief pitcher',
    'CP': 'closer',
    'CL': 'closer',
    'P': 'pitcher',
    'UTIL': 'utility',
    'IF': 'infield',
    'INF': 'infield',
    'CI': 'corner infield',
    'MI': 'middle infield'
  };

  // Handle slash, comma, or whitespace delimited positions like "2B/SS", "1B, 3B", "SP/RP"
  const rawTokens = pos.split(/[/,\s+]+/).map(p => p.trim()).filter(Boolean);
  if (rawTokens.length === 0) return 'player';
  const spokenTokens = rawTokens.map(tok => mapping[tok.toUpperCase()] || tok);
  if (spokenTokens.length === 1) return spokenTokens[0];
  if (spokenTokens.length === 2) return `${spokenTokens[0]} and ${spokenTokens[1]}`;
  return `${spokenTokens.slice(0, -1).join(', ')}, and ${spokenTokens[spokenTokens.length - 1]}`;
}

export function formatTeamForSpeech(team) {
  if (!team || typeof team !== 'string') return '';
  const clean = team.trim();
  const upper = clean.toUpperCase();
  if (upper === 'FA' || upper === 'FREE AGENT') return 'Free Agent';
  if (upper === 'MLB' || upper === 'NONE' || upper === 'N/A') return '';
  const matched = findMlbTeam(clean);
  if (matched?.name) return matched.name;
  return clean;
}

export function generatePickAnnouncement({ pickNumber, owner, playerName, position, team, nextOwner }) {
  const spokenPos = formatPositionForSpeech(position);
  const spokenTeam = formatTeamForSpeech(team);
  const teamPart = spokenTeam ? `, ${spokenTeam}` : '';
  const nextOwnerPart = nextOwner ? ` ${nextOwner} is on the clock.` : '';
  return `With pick ${pickNumber}, ${owner} selects ${playerName}, ${spokenPos}${teamPart}.${nextOwnerPart}`;
}

// --- CONFIGURATION ---
const GEMINI_API_KEY = import.meta.env.VITE_GEMINI_API_KEY || 'AIzaSyDQ0eRBz6jSsORZrnG19jR5mzmd0QE0DWg';
const GEMINI_MODELS = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-1.5-flash'];

export async function callGemini(prompt) {
  // 1. Try Supabase Edge Function 'gemini-proxy' first (secure backend proxy with Google Gemini access)
  try {
    const { data, error } = await supabase.functions.invoke('gemini-proxy', {
      body: { mode: 'generate', prompt, model: 'gemini-2.5-flash' }
    });
    if (!error && data?.candidates?.[0]?.content?.parts?.[0]?.text) {
      return data.candidates[0].content.parts[0].text;
    }
  } catch (err) {
    console.warn('Supabase gemini-proxy edge function invocation failed, trying direct Gemini API if configured:', err);
  }

  // 2. Direct Gemini API call if key is configured
  const apiKey = import.meta.env.VITE_GEMINI_API_KEY || (typeof GEMINI_API_KEY !== 'undefined' ? GEMINI_API_KEY : '');
  if (!apiKey || apiKey.startsWith('AIzaSyDQ0eRBz6jSsORZrnG19jR5mzmd0QE0DWg')) {
    return null;
  }

  for (const model of GEMINI_MODELS) {
    try {
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{
            parts: [{ text: prompt }]
          }]
        })
      });
      if (response.ok) {
        const data = await response.json();
        if (data.candidates && data.candidates[0]?.content?.parts?.[0]?.text) {
          return data.candidates[0].content.parts[0].text;
        }
      }
    } catch (err) {
      console.warn(`Gemini fetch error for ${model}:`, err);
    }
  }
  return null;
}

export function determinePickAngle(player, pickNum, rank, surplus, teamContext) {
  const isPitcher = (player.Position || '').includes('SP') || (player.Position || '').includes('RP') || (player.Position || '').includes('P');
  const isReliever = (player.Position || '').includes('RP') || (player.Position || '').includes('CL');
  const posCounts = teamContext?.positionCounts || {};
  const spCount = posCounts['SP'] || 0;
  const sb = parseFloat(player.ZIPSSB || player.ESPNSB || 0);
  const hr = parseFloat(player.ZIPSHR || player.ESPNHR || 0);
  const k = parseFloat(player.ZIPSK || player.ESPNK || 0);
  const sv = parseFloat(player['ZIPSSV+HDs'] || player['ESPNSV+HDs'] || 0);

  // 1. Extreme board displacement
  if (surplus !== null && surplus >= 12) return 'SURPLUS_STEAL';
  if (surplus !== null && surplus <= -14) return 'AGGRESSIVE_REACH';

  // 2. Specialized role or skill profile
  if (isReliever || sv >= 16) return 'BULLPEN_CLOSER';
  if (isPitcher && spCount >= 2) return 'ROTATION_HEAVY';
  if (sb >= 28) return 'SPEED_SPECIALIST';
  if (hr >= 30) return 'POWER_SLUGGER';
  if (isPitcher && k >= 190) return 'K_MACHINE';

  // 3. Draft stages
  if (pickNum <= 18) return 'CORNERSTONE_FOUNDATION';
  if (pickNum >= 140) return 'LATE_ROUND_FLYER';

  // 4. Moderate surplus/deficit
  if (surplus !== null && surplus >= 6) return 'VALUE_PICK';
  if (surplus !== null && surplus <= -6) return 'PRIORITY_TARGET';

  return 'BALANCED_FIT';
}

export function calculatePickGrade(pickNum, rank, surplus) {
  // If no rank is available or placeholder rank (>= 1000):
  if (rank === null || rank === undefined || rank >= 1000) {
    if (pickNum >= 140) {
      return {
        grade: 'B+',
        label: 'Late-Round Upside Flier',
        color: '#69f0ae',
        bg: 'rgba(105, 240, 174, 0.15)'
      };
    }
    if (pickNum >= 70) {
      return {
        grade: 'B',
        label: 'Depth Speculation',
        color: '#ffd54f',
        bg: 'rgba(255, 213, 79, 0.15)'
      };
    }
    return {
      grade: 'C+',
      label: 'Off-Board Target',
      color: '#ff9800',
      bg: 'rgba(255, 152, 0, 0.15)'
    };
  }

  // Elite top of the draft buffer (Picks 1-15, Round 1 superstars)
  if (pickNum <= 15 && surplus >= -5) {
    if (surplus >= 3) {
      return {
        grade: 'A+',
        label: `Elite Anchor (+${surplus})`,
        color: '#00e676',
        bg: 'rgba(0, 230, 118, 0.18)'
      };
    }
    return {
      grade: 'A',
      label: 'Franchise Cornerstone',
      color: '#00e676',
      bg: 'rgba(0, 230, 118, 0.18)'
    };
  }

  // Value tiers based on surplus:
  if (surplus >= 15) {
    return {
      grade: 'A+',
      label: `Massive Steal (+${surplus})`,
      color: '#00e676',
      bg: 'rgba(0, 230, 118, 0.18)'
    };
  }
  if (surplus >= 8) {
    return {
      grade: 'A',
      label: `Strong Value (+${surplus})`,
      color: '#00e676',
      bg: 'rgba(0, 230, 118, 0.18)'
    };
  }
  if (surplus >= 4) {
    return {
      grade: 'A-',
      label: `Positive Surplus (+${surplus})`,
      color: '#69f0ae',
      bg: 'rgba(105, 240, 174, 0.15)'
    };
  }
  if (surplus >= 0) {
    return {
      grade: 'B+',
      label: surplus === 0 ? 'On-Market Valuation' : `Solid Value (+${surplus})`,
      color: '#4caf50',
      bg: 'rgba(76, 175, 80, 0.15)'
    };
  }
  if (surplus >= -4) {
    return {
      grade: 'B',
      label: `Targeted Selection (${surplus})`,
      color: '#ffd54f',
      bg: 'rgba(255, 213, 79, 0.15)'
    };
  }
  if (surplus >= -8) {
    return {
      grade: 'B-',
      label: `Modest Reach (${surplus})`,
      color: '#ffb74d',
      bg: 'rgba(255, 183, 77, 0.15)'
    };
  }
  if (surplus >= -14) {
    return {
      grade: 'C+',
      label: `Reach (${surplus} Spots)`,
      color: '#ff9800',
      bg: 'rgba(255, 152, 0, 0.15)'
    };
  }
  if (surplus >= -20) {
    return {
      grade: 'C',
      label: `Heavy Premium (${surplus})`,
      color: '#ff7043',
      bg: 'rgba(255, 112, 67, 0.15)'
    };
  }
  if (surplus >= -28) {
    return {
      grade: 'C-',
      label: `Significant Overdraft (${surplus})`,
      color: '#f44336',
      bg: 'rgba(244, 67, 54, 0.15)'
    };
  }
  return {
    grade: 'D',
    label: `Extreme Reach (${surplus})`,
    color: '#d32f2f',
    bg: 'rgba(211, 47, 47, 0.2)'
  };
}

export function getPlayerDraftHistory(player, allDraftHistory = []) {
  if (!player) return [];
  let hist = Array.isArray(allDraftHistory) ? [...allDraftHistory] : [];
  if (!hist.some(d => String(d.Year || d.year) === '2026') && mappedDraft2026History?.length > 0) {
    hist = [...mappedDraft2026History, ...hist];
  }

  const pId = String(player['ESPN PlayerID'] || player.player_id || player.id || '').trim();
  const mlbId = String(player.MLBAMID || player.mlbamid || '').trim();
  const pName = String(player.Player || player.player_name || '').trim().toLowerCase();

  return hist
    .filter(h => {
      const hPid = String(h.player_id || h['ESPN PlayerID'] || h.espn_player_id || '').trim();
      const hMlb = String(h.MLBAMID || h.mlbamid || '').trim();
      const hName = String(h.Player_Name || h.Player || h.player_name || '').trim().toLowerCase();
      if (pId && hPid && pId === hPid) return true;
      if (mlbId && hMlb && mlbId === hMlb) return true;
      if (pName && hName && pName === hName) return true;
      return false;
    })
    .sort((a, b) => (parseInt(b.Year || b.year, 10) || 0) - (parseInt(a.Year || a.year, 10) || 0));
}

export function formatDraftHistorySummary(playerDraftHistory = []) {
  if (!playerDraftHistory || playerDraftHistory.length === 0) {
    return '<span style="color: #9e9e9e; font-style: italic;">League Draft Debut (First time selected)</span>';
  }

  const recent = playerDraftHistory.slice(0, 3);
  return recent.map(entry => {
    const rawOwner = entry.Owner || entry.owner || entry.Team_ID || entry.team_id || 'Unknown';
    const ownerName = getFriendlyOwnerName(rawOwner);
    const yr = entry.Year || entry.year || '?';
    const isKeeper = String(entry.Keeper).toLowerCase() === 'true' || entry.Keeper === true;
    const round = entry.Round || entry.round;
    const overall = entry['Overall Pick'] || entry.overall_pick || entry.Pick_Overall || entry.Pick || entry.pick;
    const cost = entry.cost || entry.Bid_Amount || entry.bid_amount;
    const costStr = cost && String(cost) !== '0' ? ` ($${cost})` : '';

    let slotStr = '';
    if (isKeeper) {
      slotStr = 'Keeper';
    } else if (round && overall) {
      slotStr = `R${round} (#${overall})`;
    } else if (round) {
      slotStr = `R${round}`;
    } else if (overall) {
      slotStr = `#${overall}`;
    } else {
      slotStr = 'Drafted';
    }

    return `<span style="display: inline-flex; align-items: center; gap: 4px; background: rgba(255, 255, 255, 0.06); border: 1px solid rgba(255, 255, 255, 0.12); padding: 2px 7px; border-radius: 4px; font-size: 11px; color: #ddd;"><b>${yr}</b>: ${slotStr}${costStr} <span style="color: #ffd54f; font-weight: 600;">(${ownerName})</span></span>`;
  }).join(' ');
}

export function formatPlayerStatsSummary(player) {
  if (!player) return '';
  const isPitcher = (player.Position || '').includes('SP') || (player.Position || '').includes('RP') || (player.Position || '').includes('P');

  if (isPitcher) {
    const k = player.ZIPSK || player.ESPNK;
    const qs = getEffectiveQS(player) || player.ZIPSQS || player.ESPNQS;
    const era = player.ZIPSERA || player.ESPNERA;
    const whip = player.ZIPSWHIP || player.ESPNWHIP;
    const sv = player['ZIPSSV+HDs'] || player['ESPNSV+HDs'];
    const ip = player.ZIPSIP || player.ESPNIP;

    const stats = [
      { label: 'K', val: k ? Math.round(Number(k)) : '—' },
      { label: 'QS', val: qs ? Math.round(Number(qs)) : '—' },
      { label: 'ERA', val: era ? Number(era).toFixed(2) : '—' },
      { label: 'WHIP', val: whip ? Number(whip).toFixed(2) : '—' },
      { label: 'SV+HD', val: sv ? Math.round(Number(sv)) : '0' }
    ];
    if (ip) {
      stats.unshift({ label: 'IP', val: Math.round(Number(ip)) });
    }

    return stats.map(s => 
      `<span style="display: inline-flex; align-items: center; background: rgba(255, 255, 255, 0.05); padding: 2px 7px; border-radius: 4px; border: 1px solid rgba(255, 255, 255, 0.08); font-size: 11px;"><span style="color: #888; margin-right: 4px;">${s.label}:</span><b style="color: #fff;">${s.val}</b></span>`
    ).join(' ');
  }

  // Hitters: HR, RBI, R, SB, OBP
  const hr = player.ZIPSHR || player.ESPNHR;
  const rbi = player.ZIPSRBI || player.ESPNRBI;
  const r = player.ZIPSR || player.ESPNR;
  const sb = player.ZIPSSB || player.ESPNSB;
  const obp = player.ZIPSOBP || player.ESPNOBP;
  const avg = player.ZIPSAVG || player.ESPNAVG;

  const obpStr = obp ? `.${String(obp).replace('0.', '').slice(0, 3)}` : '—';
  const avgStr = avg ? `.${String(avg).replace('0.', '').slice(0, 3)}` : null;

  const stats = [
    { label: 'HR', val: hr ? Math.round(Number(hr)) : '—' },
    { label: 'RBI', val: rbi ? Math.round(Number(rbi)) : '—' },
    { label: 'R', val: r ? Math.round(Number(r)) : '—' },
    { label: 'SB', val: sb ? Math.round(Number(sb)) : '—' },
    { label: 'OBP', val: obpStr }
  ];
  if (avgStr) {
    stats.push({ label: 'AVG', val: avgStr });
  }

  return stats.map(s => 
    `<span style="display: inline-flex; align-items: center; background: rgba(255, 255, 255, 0.05); padding: 2px 7px; border-radius: 4px; border: 1px solid rgba(255, 255, 255, 0.08); font-size: 11px;"><span style="color: #888; margin-right: 4px;">${s.label}:</span><b style="color: #fff;">${s.val}</b></span>`
  ).join(' ');
}

export function renderPickSummaryCard({ player, pickNum, rank, price, surplus, playerDraftHistory }) {
  const gradeInfo = calculatePickGrade(pickNum, rank, surplus);
  const statsHtml = formatPlayerStatsSummary(player);
  const historyHtml = formatDraftHistorySummary(playerDraftHistory);
  const teamObj = findMlbTeam(player.Team);
  const teamDisplay = teamObj?.name || player.Team || 'MLB';

  const hasValidRank = rank !== null && rank !== undefined && rank < 1000;
  const displayRank = hasValidRank ? `#${rank}` : '—';
  const surplusColor = hasValidRank && surplus !== null
    ? (surplus >= 0 ? '#00e676' : '#ff9800')
    : '#aaa';
  const surplusStr = hasValidRank && surplus !== null
    ? (surplus > 0 ? `+${surplus}` : (surplus === 0 ? 'Even' : `${surplus}`))
    : '—';

  return `
<!-- PICK_SUMMARY_CARD -->
<div style="margin-top: 14px; padding: 12px 14px; background: rgba(18, 18, 28, 0.85); border: 1px solid rgba(255, 255, 255, 0.12); border-radius: 8px; display: flex; flex-direction: column; gap: 10px; font-style: normal; text-align: left;">
  <!-- Grade Header & Board Valuation -->
  <div style="display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px; padding-bottom: 8px; border-bottom: 1px solid rgba(255, 255, 255, 0.08);">
    <div style="display: flex; align-items: center; gap: 10px;">
      <span style="display: inline-flex; align-items: center; justify-content: center; width: 36px; height: 36px; border-radius: 6px; background: ${gradeInfo.bg}; color: ${gradeInfo.color}; font-weight: 800; font-size: 19px; border: 1.5px solid ${gradeInfo.color}; box-shadow: 0 0 10px ${gradeInfo.bg}; letter-spacing: -0.5px;">${gradeInfo.grade}</span>
      <div>
        <div style="font-size: 10px; text-transform: uppercase; letter-spacing: 0.6px; color: #888; font-weight: 700;">PICK GRADE</div>
        <div style="font-size: 13px; font-weight: 700; color: ${gradeInfo.color};">${gradeInfo.label}</div>
      </div>
    </div>
    <div style="display: flex; align-items: center; gap: 12px; font-size: 12px; color: #bbb; flex-wrap: wrap;">
      <span>Rank: <b style="color: #fff;">${displayRank}</b></span>
      <span>Value: <b style="color: #4caf50;">$${price !== null && price !== undefined ? price : '—'}</b></span>
      <span>Surplus: <b style="color: ${surplusColor};">${surplusStr}</b></span>
      <span style="color: #666;">•</span>
      <span style="color: #aaa;">${teamDisplay}</span>
    </div>
  </div>

  <!-- Projected Stats Grid -->
  <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
    <span style="color: #888; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px;">Projections:</span>
    <div style="display: flex; align-items: center; gap: 6px; flex-wrap: wrap;">
      ${statsHtml}
    </div>
  </div>

  <!-- League Draft History Row -->
  <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap; padding-top: 4px; border-top: 1px dashed rgba(255, 255, 255, 0.06);">
    <span style="color: #888; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px;">League History:</span>
    <div style="display: flex; align-items: center; gap: 6px; flex-wrap: wrap;">
      ${historyHtml}
    </div>
  </div>
</div>`;
}

export function generateLocalDraftCommentary(player, owner, pickNum, teamContext = {}, draftHistory = []) {
  const rank = getEffectiveRank(player);
  const price = getEffectivePrice(player);
  const surplus = rank ? (pickNum - rank) : null;
  const isPitcher = (player.Position || '').includes('SP') || (player.Position || '').includes('RP') || (player.Position || '').includes('P');
  const angle = determinePickAngle(player, pickNum, rank, surplus, teamContext);
  const playerDraftHistory = getPlayerDraftHistory(player, draftHistory);

  const hr = player.ZIPSHR || player.ESPNHR;
  const rbi = player.ZIPSRBI || player.ESPNRBI;
  const sb = player.ZIPSSB || player.ESPNSB;
  const obp = player.ZIPSOBP || player.ESPNOBP;
  const obpStr = obp ? `.${String(obp).replace('0.', '').slice(0, 3)}` : null;
  const k = player.ZIPSK || player.ESPNK;
  const era = player.ZIPSERA || player.ESPNERA;
  const whip = player.ZIPSWHIP || player.ESPNWHIP;
  const qs = player.ZIPSQS || player.ESPNQS;
  const sv = player['ZIPSSV+HDs'] || player['ESPNSV+HDs'];

  const mlbTeamObj = findMlbTeam(player.Team);
  const mlbTeamName = mlbTeamObj?.name || player.Team || 'MLB';
  const roleDesc = isPitcher
    ? ((player.Position || '').includes('RP') || (player.Position || '').includes('CL') ? `a high-leverage bullpen stopper for the ${mlbTeamName}` : `a frontline starter for the ${mlbTeamName}`)
    : `a key bat in the ${mlbTeamName} order`;

  let historySentence = '';
  if (playerDraftHistory && playerDraftHistory.length > 0) {
    const recent = playerDraftHistory[0];
    const prevYear = recent.Year || recent.year || '2026';
    const rawOwner = recent.Owner || recent.owner || recent.Team_ID || recent.team_id || 'Unknown';
    const prevOwner = getFriendlyOwnerName(rawOwner);
    const prevRound = recent.Round || recent.round;
    const prevPick = recent['Overall Pick'] || recent.overall_pick || recent.Pick_Overall || recent.Pick || recent.pick;
    const isKeeper = String(recent.Keeper).toLowerCase() === 'true' || recent.Keeper === true;

    if (prevOwner.toLowerCase() === owner.toLowerCase()) {
      historySentence = `<b>${owner}</b> stays loyal to their ${prevYear} ${isKeeper ? 'keeper' : `Round ${prevRound || 'roster'}`} piece, keeping his production firmly in-house.`;
    } else if (isKeeper) {
      historySentence = `After being kept by <b>${prevOwner}</b> in ${prevYear}, he changes allegiances to anchor <b>${owner}</b>'s lineup.`;
    } else if (prevRound && prevPick) {
      historySentence = `Drafted by <b>${prevOwner}</b> in Round ${prevRound} (#${prevPick}) back in ${prevYear}, he shifts to <b>${owner}</b>'s roster for the upcoming season.`;
    } else {
      historySentence = `Previously rostered by <b>${prevOwner}</b> in ${prevYear}, he enters a new chapter on <b>${owner}</b>'s board.`;
    }
  } else {
    historySentence = `Making his official Heftystrong draft debut, he enters the league as a high-upside newcomer for <b>${owner}</b>.`;
  }

  let headline = '';
  let narrative = '';
  const v = pickNum % 3;

  switch (angle) {
    case 'SURPLUS_STEAL': {
      headline = `🔥 <b>Draft Steal (+${surplus} Surplus)</b>`;
      const variants = [
        `Tremendous board patience pays off for <b>${owner}</b> at pick #${pickNum}. Securing consensus #${rank} talent <b>${player.Player}</b> lands ${roleDesc} at a massive +${surplus} surplus discount. ${historySentence}`,
        `The room let <b>${player.Player}</b> slide way past market valuation. <b>${owner}</b> scoops up the ${mlbTeamName} ${player.Position} and his projected ${isPitcher ? `${k || 170} K / ${era || '3.50'} ERA profile` : `${hr || 25} HR / ${obpStr || '.350'} OBP engine`} for pure draft profit. ${historySentence}`,
        `Pure surplus equity for <b>${owner}</b> here at #${pickNum}. Adding a $${price || 20} projected asset from the ${mlbTeamName} this late represents one of the cleanest value captures of the draft. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'AGGRESSIVE_REACH': {
      const reach = Math.abs(surplus);
      headline = `⚡ <b>Aggressive Target (-${reach} Reach)</b>`;
      const variants = [
        `<b>${owner}</b> refuses to wait at pick #${pickNum}, leaping ${reach} spots ahead of consensus rank to lock down <b>${player.Player}</b>. Clearly prioritizing ${roleDesc} before this tier evaporates. ${historySentence}`,
        `Flag planted with conviction. <b>${owner}</b> pulls the trigger on <b>${player.Player}</b> well above his #${rank || 'board'} projection, prioritizing the ${mlbTeamName} ${player.Position}'s upside over market consensus. ${historySentence}`,
        `Bold maneuvering by <b>${owner}</b>. Paying a ${reach}-pick premium ensures ${player.Position} coverage with ${player.Player}'s projected ${isPitcher ? `${k || 150}+ Ks` : `${hr || 20}+ HRs`} for the upcoming campaign. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'BULLPEN_CLOSER': {
      headline = `🎯 <b>High-Leverage Bullpen</b>`;
      const variants = [
        `<b>${owner}</b> bolsters the bullpen with <b>${player.Player}</b>, acquiring a premier late-inning weapon for the ${mlbTeamName}. Projected for ${sv || '20+'} SV+HDs with swing-and-miss stuff (${k || '70'} Ks). ${historySentence}`,
        `Relief leverage secured at #${pickNum}. <b>${player.Player}</b> provides <b>${owner}</b> with a shutdown arm in the ${mlbTeamName} pen, pacing weekly SV+HD counts alongside a stingy ${era || '3.10'} ERA. ${historySentence}`,
        `Targeted bullpen reinforcement for <b>${owner}</b>. Locking down <b>${player.Player}</b> brings ${sv ? `${sv} projected SV+HDs` : 'high-leverage relief'} to keep ratios and holds protected. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'ROTATION_HEAVY': {
      headline = `⚾ <b>Rotation Anchor</b>`;
      const variants = [
        `<b>${owner}</b> invests heavily in mound volume, landing <b>${player.Player}</b> to bolster the rotation. Projected for ${qs || 14} quality starts and a ${era || '3.60'} ERA for the ${mlbTeamName}. ${historySentence}`,
        `Starting pitching depth prioritised at pick #${pickNum}. Adding <b>${player.Player}</b> gives <b>${owner}</b> a tested arm capable of chewing innings and delivering ${k || 160} Ks for ${mlbTeamName}. ${historySentence}`,
        `Mound dominance remains the blueprint for <b>${owner}</b>. <b>${player.Player}</b> steps in as a dependable starter to carry weekly QS and strikeout tallies. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'SPEED_SPECIALIST': {
      headline = `💨 <b>Elite Speed Impact</b>`;
      const variants = [
        `Game-breaking speed comes off the board at #${pickNum}. <b>${owner}</b> secures <b>${player.Player}</b>, whose projected ${sb || 30} steals for the ${mlbTeamName} can single-handedly flip weekly category matchups. ${historySentence}`,
        `A dedicated category weapon for <b>${owner}</b>. <b>${player.Player}</b> brings blazing wheels to the basepaths alongside a solid ${obpStr || '.340'} OBP profile in the ${mlbTeamName} lineup. ${historySentence}`,
        `Speed premium paid and delivered. <b>${owner}</b> targets <b>${player.Player}</b> at #${pickNum} to lock down ${sb || 25}+ stolen base upside at ${player.Position}. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'POWER_SLUGGER': {
      headline = `💥 <b>Middle-of-Order Power</b>`;
      const variants = [
        `Heavyweight thump for <b>${owner}</b>. <b>${player.Player}</b> is forecasted for ${hr || 28} home runs and ${rbi || 90} RBIs, anchoring run production in the middle of the ${mlbTeamName} order. ${historySentence}`,
        `Raw power locked down at pick #${pickNum}. <b>${owner}</b> adds <b>${player.Player}</b> to pace the power columns with ${hr || 25}+ homers and heavy extra-base damage for ${mlbTeamName}. ${historySentence}`,
        `<b>${owner}</b> dials up serious extra-base juice. <b>${player.Player}</b> brings an imposing bat projected for ${hr || 26} HR and strong RBI totals to this roster. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'K_MACHINE': {
      headline = `🔥 <b>Elite Strikeout Punch</b>`;
      const variants = [
        `Whiff-rate premium secured for <b>${owner}</b>. <b>${player.Player}</b> generates elite swing-and-miss stuff for the ${mlbTeamName}, projected for ${k || 190} strikeouts across ${qs || 15} quality starts. ${historySentence}`,
        `Missing bats at an elite clip. <b>${owner}</b> nabs <b>${player.Player}</b> at pick #${pickNum} to supercharge strikeout totals (${k || 180}+ K proj) while protecting WHIP (${whip || '1.15'}). ${historySentence}`,
        `Strikeout firepower added to <b>${owner}</b>'s staff. <b>${player.Player}</b> brings explosive mound upside that elevates weekly pitching categories immediately. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'CORNERSTONE_FOUNDATION': {
      headline = `👑 <b>Franchise Cornerstone</b>`;
      const variants = [
        `<b>${owner}</b> lays a premier cornerstone in round ${Math.ceil(pickNum / 10)}. <b>${player.Player}</b> brings superstar pedigree to the ${mlbTeamName} and delivers an elite baseline of ${isPitcher ? `${k || 190} Ks and ${era || '3.20'} ERA` : `${hr || 28} HR and .${String(obp || '360').replace('0.', '')} OBP`}. ${historySentence}`,
        `A marquee cornerstone selection for <b>${owner}</b> at pick #${pickNum}. <b>${player.Player}</b> provides high-floor excellence and multi-category production to build this roster around. ${historySentence}`,
        `Starting from strength: <b>${owner}</b> secures <b>${player.Player}</b> as their premier anchor, locking in top-tier performance for ${mlbTeamName}. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'LATE_ROUND_FLYER': {
      headline = `🎲 <b>Late-Round Upside</b>`;
      const variants = [
        `Calculated upside play at pick #${pickNum}. <b>${owner}</b> takes a flyer on <b>${player.Player}</b>, who offers intriguing ceiling in the ${mlbTeamName} organization if playing time breaks right. ${historySentence}`,
        `Late-round ceiling hunting for <b>${owner}</b>. At this stage of the draft, <b>${player.Player}</b>'s skill profile (${isPitcher ? `${k || 120} K punch` : `${hr || 18} HR pop`}) is well worth the gamble. ${historySentence}`,
        `High-upside selection to round out the board. <b>${owner}</b> adds <b>${player.Player}</b> at #${pickNum} for bench flexibility and category insurance. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'VALUE_PICK': {
      headline = `📈 <b>Above-Average Value (+${surplus})</b>`;
      const variants = [
        `Smart draft board reading by <b>${owner}</b>. <b>${player.Player}</b> slips past his #${rank} projection to deliver a favorable ${surplus}-pick surplus at #${pickNum} for the ${mlbTeamName} ${player.Position}. ${historySentence}`,
        `Consensus value in the bag. <b>${owner}</b> capitalizes on <b>${player.Player}</b> sliding ${surplus} spots past expected rank for a steady addition from ${mlbTeamName}. ${historySentence}`,
        `Efficient acquisition at #${pickNum}. <b>${owner}</b> captures positive equity on <b>${player.Player}</b> without having to force the issue. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'PRIORITY_TARGET': {
      headline = `⚡ <b>Targeted Priority (-${Math.abs(surplus)})</b>`;
      const variants = [
        `<b>${owner}</b> didn't want to risk losing <b>${player.Player}</b>, pulling him ${Math.abs(surplus)} spots early to ensure ${player.Position} coverage with the ${mlbTeamName} standout. ${historySentence}`,
        `Target locked. <b>${owner}</b> steps ahead of the curve at pick #${pickNum} to bring in <b>${player.Player}</b> ahead of market consensus. ${historySentence}`,
        `Proactive roster construction. <b>${owner}</b> pays a modest reach price to secure <b>${player.Player}</b>'s specific statistical profile for the upcoming matchups. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }

    case 'BALANCED_FIT':
    default: {
      headline = `🎯 <b>On-Market Selection</b>`;
      const variants = [
        `Right on script at pick #${pickNum}. <b>${owner}</b> selects <b>${player.Player}</b> directly in line with board valuation, addressing ${player.Position} with projected ${isPitcher ? `${era || '3.70'} ERA / ${k || 140} K` : `${hr || 20} HR / ${rbi || 70} RBI`} for ${mlbTeamName}. ${historySentence}`,
        `Disciplined, on-schedule pick by <b>${owner}</b>. <b>${player.Player}</b> comes off the board at fair market value to provide steady ${player.Position} production. ${historySentence}`,
        `<b>${owner}</b> stays balanced at #${pickNum}, picking up <b>${player.Player}</b> to keep weekly counting categories and ratios on pace. ${historySentence}`
      ];
      narrative = variants[v];
      break;
    }
  }

  const summaryCard = renderPickSummaryCard({
    player,
    pickNum,
    rank,
    price,
    surplus,
    playerDraftHistory
  });

  return `${headline}<br><br>${narrative}${summaryCard}`;
}

export async function generateDraftCommentary(player, owner, pickNum, teamContext = {}, season = 2027, draftHistory = []) {
  const rank = getEffectiveRank(player);
  const price = getEffectivePrice(player);
  const surplus = rank ? (pickNum - rank) : null;
  const angle = determinePickAngle(player, pickNum, rank, surplus, teamContext);
  const statLine = formatPlayerStats(player);
  const playerDraftHistory = getPlayerDraftHistory(player, draftHistory);

  const mlbTeamObj = findMlbTeam(player.Team);
  const mlbTeamName = mlbTeamObj?.name || player.Team || 'MLB';

  const posBreakdown = typeof teamContext === 'object' && teamContext?.positionCounts
    ? Object.entries(teamContext.positionCounts).map(([pos, c]) => `${pos}: ${c}`).join(', ')
    : (typeof teamContext === 'string' ? teamContext : 'None yet');

  const recentPicksStr = typeof teamContext === 'object' && teamContext?.recentPicks?.length
    ? teamContext.recentPicks.join(' ➔ ')
    : 'None yet';

  const surplusDesc = surplus !== null
    ? (surplus >= 0 ? `+${surplus} spots past consensus rank (value slide)` : `${surplus} spots ahead of consensus rank (reach)`)
    : 'In line with consensus';

  const historyPromptSummary = playerDraftHistory.length > 0
    ? playerDraftHistory.slice(0, 3).map(h => {
        const o = getFriendlyOwnerName(h.Owner || h.owner || h.Team_ID || h.team_id || 'Unknown');
        const yr = h.Year || h.year || '?';
        const isK = String(h.Keeper).toLowerCase() === 'true' || h.Keeper === true;
        const r = h.Round || h.round;
        const p = h['Overall Pick'] || h.overall_pick || h.Pick_Overall;
        return `${yr}: ${isK ? 'Keeper' : `Round ${r || '?'}${p ? ` (Pick ${p})` : ''}`} drafted by ${o}`;
      }).join('; ')
    : 'First-time draft debut in league history (never drafted previously)';

  const prompt = `You are a sharp, analytical fantasy baseball war-room commentator providing instant reaction for the ${season} draft.
League Format: Head-to-Head Each Category (R, HR, RBI, SB, OBP | K, QS, ERA, WHIP, SV+HD).

SELECTION:
- Manager: ${owner}
- Player: ${player.Player} (${player.Position} - ${mlbTeamName})
- Pick: #${pickNum}
- Consensus Board Rank: ${rank ? `#${rank}` : 'Unranked'}
- Projected Value: ${price ? `$${price}` : 'N/A'}
- Surplus vs Pick: ${surplusDesc}
- Player Projections: ${statLine}

HISTORICAL LEAGUE DRAFT RECORD:
${historyPromptSummary}

MANAGER ROSTER CONTEXT:
- Picks so far: ${teamContext?.rosterCount || 0}
- Current positions rostered: ${posBreakdown}
- Recent picks: ${recentPicksStr}

ANALYTICAL ANGLE: [${angle}]

STRICT NEGATIVE CONSTRAINTS - BANNED CLICHÉS (DO NOT USE ANY OF THESE UNDER ANY CIRCUMSTANCES):
- DO NOT say "solidifies their depth" or "adds depth"
- DO NOT say "immediate category firepower" or "firepower"
- DO NOT say "championship push" or "title aspirations" or "championship contention"
- DO NOT say "anchors the staff" or "anchors the rotation"
- DO NOT say "locks in [Player] at Pick [X]"
- DO NOT say "makes a statement" or "puts the league on notice"
- DO NOT say "only time will tell" or "time will tell"
- DO NOT say "high-risk, high-reward"
- DO NOT say "fills a crucial void/need"
- DO NOT say "fitting their [archetype] identity"
- DO NOT say "potent bat" or "live arm"

INSTRUCTIONS:
1. Write 2 to 3 concise, punchy sentences (strictly under 75 words).
2. Incorporate REAL-WORLD player information (MLB team context for ${mlbTeamName}, role, skill set, and rotation/lineup context).
3. Analyze the draft VALUE (consensus board rank vs pick #${pickNum}, surplus discount or reach, dollar value equity).
4. Reference the player's past LEAGUE DRAFT HISTORY if available (who drafted them in prior seasons, or if this is their league debut).
5. Format with clean HTML: Use <b> tags for manager names, player names, teams, and key metrics. Do NOT include markdown code blocks.
6. Return ONLY the narrative prose. Do NOT include any summary cards, grade badges, or tables (those will be appended automatically).`;

  const summaryCard = renderPickSummaryCard({
    player,
    pickNum,
    rank,
    price,
    surplus,
    playerDraftHistory
  });

  try {
    const aiResponse = await callGemini(prompt);
    if (aiResponse && aiResponse !== 'Analysis unavailable') {
      let narrative = aiResponse.replace(/^```html\s*/i, '').replace(/```\s*$/i, '').trim();
      // Strip any hallucinated card, footers, or trailing artifacts
      narrative = narrative.replace(/<!-- PICK_SUMMARY_CARD -->[\s\S]*/i, '').trim();
      narrative = narrative.replace(/<span style="color:#888[^>]*>.*?<\/span>/gi, '').trim();
      return `${narrative}${summaryCard}`;
    }
  } catch (err) {
    console.warn('Gemini draft commentary error, using local fallback:', err);
  }

  return generateLocalDraftCommentary(player, owner, pickNum, teamContext, draftHistory);
}

// --- AUDIO SYSTEM ---
let currentTtsAudio = null;

export function prepareSpokenScript(htmlText) {
  if (!htmlText) return '';
  // Cut off any summary card or footer metadata BEFORE stripping tags
  let narrative = htmlText;
  if (narrative.includes('<!-- PICK_SUMMARY_CARD -->')) {
    narrative = narrative.split('<!-- PICK_SUMMARY_CARD -->')[0];
  } else if (narrative.includes('PICK GRADE')) {
    narrative = narrative.split(/PICK GRADE/i)[0];
  }
  // Remove old-style metadata footers (Rank, Proj Value, Pos, Team, etc.)
  narrative = narrative.replace(/<span style="color:#888[^>]*>.*?<\/span>/gi, ' ');
  narrative = narrative.replace(/Rank:\s*#?\d*.*?Team:\s*[\w\s]*/gi, ' ');
  narrative = narrative.replace(/Board:\s*#?\d*.*?MLB:\s*[\w\s]*/gi, ' ');

  // Strip HTML tags
  let text = narrative.replace(/<[^>]*>/g, ' ');
  // Remove emojis
  text = text.replace(/[\u{1F300}-\u{1FAFF}]|[\u{2600}-\u{27BF}]/gu, ' ');
  // Expand fantasy and baseball abbreviations so pronunciation sounds like an athletic sports broadcaster
  text = text
    .replace(/\b1B\b/g, 'first base')
    .replace(/\b2B\b/g, 'second base')
    .replace(/\b3B\b/g, 'third base')
    .replace(/\bSS\b/g, 'shortstop')
    .replace(/\bSP\b/g, 'starting pitcher')
    .replace(/\bRP\b/g, 'relief pitcher')
    .replace(/\bDH\b/g, 'designated hitter')
    .replace(/\bOF\b/g, 'outfield')
    .replace(/\bLF\b/g, 'left field')
    .replace(/\bCF\b/g, 'center field')
    .replace(/\bRF\b/g, 'right field')
    .replace(/\bHRs?\b/g, 'home runs')
    .replace(/\bRBIs?\b/g, 'RBIs')
    .replace(/\bSB\b/g, 'stolen bases')
    .replace(/\bOBP\b/g, 'on-base percentage')
    .replace(/\bERA\b/g, 'E-R-A')
    .replace(/\bWHIP\b/g, 'whip')
    .replace(/\bKs\b/g, 'strikeouts')
    .replace(/\bQS\b/g, 'quality starts')
    .replace(/\bSV\+HDs?\b/g, 'saves plus holds')
    .replace(/#(\d+)/g, 'number $1')
    .replace(/\$(\d+)/g, '$1 dollars')
    .replace(/\s+/g, ' ')
    .trim();
  return text;
}

// --- 2-PERSON PODCAST DIALOGUE & GOOGLE CLOUD HD VOICE ENGINE ---
let currentPodcastAbortController = null;
let currentPodcastAudios = [];

export function stopPodcastAudio() {
  if (currentPodcastAbortController) {
    try {
      currentPodcastAbortController.abort();
    } catch {
      // ignore
    }
    currentPodcastAbortController = null;
  }
  if (currentPodcastAudios.length > 0) {
    currentPodcastAudios.forEach(a => {
      try {
        a.pause();
        a.currentTime = 0;
      } catch {
        // ignore
      }
    });
    currentPodcastAudios = [];
  }
  if (currentTtsAudio) {
    try {
      currentTtsAudio.pause();
      currentTtsAudio.currentTime = 0;
    } catch {
      // ignore
    }
    currentTtsAudio = null;
  }
  if (typeof window !== 'undefined' && 'speechSynthesis' in window) {
    try {
      window.speechSynthesis.cancel();
    } catch {
      // ignore
    }
  }
}

export function generateLocalPodcastDialogue(player, owner, pickNum, teamContext = {}, season = 2027, nextOwner = null) {
  const rank = getEffectiveRank(player);
  const surplus = rank ? (pickNum - rank) : 0;
  const isPitcher = (player.Position || '').includes('SP') || (player.Position || '').includes('RP') || (player.Position || '').includes('P');
  const effQS = getEffectiveQS(player);
  const k = player.ZIPSK || player.ESPNK || 160;
  const hr = player.ZIPSHR || player.ESPNHR || 22;
  const sb = parseFloat(player.ZIPSSB || player.ESPNSB || 4);
  const prevPicks = teamContext?.recentPicks || [];

  // Official Pick Announcement as opening line
  const announcementText = generatePickAnnouncement({
    pickNumber: pickNum,
    owner,
    playerName: player.Player,
    position: player.Position,
    team: player.Team,
    nextOwner
  });

  let skipLine1 = "";
  let chipLine1 = "";
  let skipLine2 = "";
  let chipLine2 = "";

  if (surplus <= -12) {
    skipLine1 = `I don't know Chip, ${player.Player} at number ${pickNum}? That is an aggressive reach when his consensus rank is sitting at number ${rank || pickNum}.`;
    chipLine1 = `Skip, you're looking at the board, but ${owner} is looking at the ceiling! In this league, you don't wait around if he's your target.`;
    skipLine2 = isPitcher
      ? `He still has to prove the health and workload over a full summer. You left proven top-tier bats on the table.`
      : `He has real strikeout volatility. If that on-base percentage slips, this pick is going to sting.`;
    chipLine2 = `Flags fly forever Skip. If ${player.Player} hits his projected ceiling, this is the bold move that wins the entire ${season} championship.`;
  } else if (surplus >= 10) {
    skipLine1 = `${player.Player} fell all the way to pick ${pickNum}? Why did eight other managers pass on a consensus top-${rank} stud?`;
    chipLine1 = `Don't overthink it Skip! Sometimes the draft room gifts you pure gold. ${owner} just smiled and scooped the easiest value of the draft.`;
    skipLine2 = isPitcher
      ? `There's always a reason an ace slides. Keep a close eye on the workload limits down the stretch.`
      : `Just make sure he gets the counting stats around that lineup.`;
    chipLine2 = `Come on, ${isPitcher ? `${k} strikeouts and ${effQS} quality starts` : `${hr} home runs with elite pedigree`}! That is an absolute highway robbery for ${owner}.`;
  } else if (isPitcher) {
    skipLine1 = `Taking ${player.Player} right here feels like a big gamble on pitching stability. Ratios can blow up fast in this category format.`;
    chipLine1 = `Are you kidding me Skip? The strikeout stuff is filthy. We're talking ${k} punchouts and an anchor for ${effQS} quality starts!`;
    skipLine2 = `Sure, when he's on the mound, but starters break hearts in fantasy. One bad week can torpedo your WHIP.`;
    chipLine2 = `${owner} knows you win this league with top-end strikeout leverage. This is an elite rotation cornerstone for ${season}.`;
  } else if (sb >= 20) {
    skipLine1 = `Are we chasing stolen bases too early here? ${player.Player} gives you speed, but where's the home run thump?`;
    chipLine1 = `Skip, in this format, 25-plus steals are practically impossible to find on waivers! That speed wins matchups all on its own.`;
    skipLine2 = `If he hits down in the order, those run totals are going to dry up quickly.`;
    chipLine2 = `His wheels create run opportunities out of thin air. ${owner} just locked down one of the fastest players on the board.`;
  } else {
    skipLine1 = prevPicks.length > 0
      ? `${player.Player} at pick ${pickNum} is dependable, but with ${prevPicks[prevPicks.length - 1]} already rostered, does ${owner} have enough ceiling here?`
      : `${player.Player} at pick ${pickNum} feels pretty fair, but I wonder if ${owner} missed a chance to grab positional scarcity elsewhere.`;
    chipLine1 = `Look at the profile Skip! That bat delivers ${hr} home runs and dependable counting stats every single week.`;
    skipLine2 = `Good player, but this feels like taking the safe floor instead of swinging for league-winning upside.`;
    chipLine2 = `Championship rosters are built on dependable category anchors. ${owner} executed this pick to perfection.`;
  }

  return [
    { speaker: 'ANNOUNCER', text: announcementText },
    { speaker: 'SKIP', text: skipLine1 },
    { speaker: 'CHIP', text: chipLine1 },
    { speaker: 'SKIP', text: skipLine2 },
    { speaker: 'CHIP', text: chipLine2 }
  ];
}

export async function generatePodcastDialogue(player, owner, pickNum, teamContext = {}, season = 2027, nextOwner = null) {
  const rank = getEffectiveRank(player);
  const price = getEffectivePrice(player);
  const surplus = rank ? (pickNum - rank) : null;
  const statLine = formatPlayerStats(player);
  const isPitcher = (player.Position || '').includes('SP') || (player.Position || '').includes('RP') || (player.Position || '').includes('P');
  const effQS = getEffectiveQS(player);

  // Official Pick Announcement as opening line
  const announcementText = generatePickAnnouncement({
    pickNumber: pickNum,
    owner,
    playerName: player.Player,
    position: player.Position,
    team: player.Team,
    nextOwner
  });
  const announcementLine = { speaker: 'ANNOUNCER', text: announcementText };

  const prompt = `You are producing a 30-second rapid-fire sports talk podcast reaction to a fantasy baseball draft selection.
Format: Head-to-Head Each Category (R, HR, RBI, SB, OBP | K, QS, ERA, WHIP, SV+HD).

SELECTION:
- Manager: ${owner}
- Player: ${player.Player} (${player.Position} - ${player.Team || 'MLB'})
- Pick: #${pickNum} (Consensus Rank: #${rank || 'N/A'}, Price: $${price || 'N/A'}, Surplus: ${surplus !== null ? (surplus > 0 ? `+${surplus} spots slide` : `${surplus} spots reach`) : 'Fair market'})
- Projections: ${statLine} ${isPitcher && effQS ? `| Projected QS: ${effQS}` : ''}

Generate 4 lines of energetic back-and-forth debate between two podcast hosts:
- SKIP: Skeptical analyst. Questions reaching, spots injury/workload flaws, challenges the pick.
- CHIP: Enthusiastic hype man. Defends the upside, loves the category juice, hypes up the manager.

Return ONLY a valid JSON array of 4 objects with "speaker" and "text" fields:
[
  { "speaker": "SKIP", "text": "..." },
  { "speaker": "CHIP", "text": "..." },
  { "speaker": "SKIP", "text": "..." },
  { "speaker": "CHIP", "text": "..." }
]`;

  try {
    const aiResponse = await callGemini(prompt);
    if (aiResponse) {
      const jsonMatch = aiResponse.match(/\[\s*\{[\s\S]*\}\s*\]/);
      if (jsonMatch) {
        const parsed = JSON.parse(jsonMatch[0]);
        if (Array.isArray(parsed) && parsed.length >= 2) {
          const debateLines = parsed.map(item => ({
            speaker: (item.speaker || 'SKIP').toUpperCase().includes('CHIP') ? 'CHIP' : 'SKIP',
            text: prepareSpokenScript(item.text || '')
          }));
          return [announcementLine, ...debateLines];
        }
      }
    }
  } catch (err) {
    console.warn('Podcast script generation failed, falling back to local debate:', err);
  }

  return generateLocalPodcastDialogue(player, owner, pickNum, teamContext, season, nextOwner);
}

export async function playDualSpeakerPodcast(dialogueScript, callbacks = {}) {
  const { onLineStart, onComplete, onError } = callbacks;
  if (!dialogueScript || !Array.isArray(dialogueScript) || dialogueScript.length === 0) {
    if (onComplete) onComplete();
    return;
  }

  stopPodcastAudio();
  const abortController = new AbortController();
  currentPodcastAbortController = abortController;

  try {
    // Pre-fetch audio blobs via Supabase gemini-proxy edge function
    // ANNOUNCER uses 'en-US-Journey-D', SKIP uses 'en-US-Chirp3-HD-Puck', CHIP uses 'en-US-Chirp3-HD-Charon'
    const audioItems = await Promise.all(
      dialogueScript.map(async (line) => {
        const speaker = (line.speaker || '').toUpperCase().trim();
        const isAnnouncer = speaker === 'ANNOUNCER' || speaker === 'COMMISSIONER';
        const isChip = speaker === 'CHIP';
        const voiceName = isAnnouncer 
          ? 'en-US-Journey-D'
          : (isChip ? 'en-US-Chirp3-HD-Charon' : 'en-US-Chirp3-HD-Puck');
        const cleanText = prepareSpokenScript(line.text);
        if (!cleanText || cleanText.length < 2) return null;

        try {
          const { data, error } = await supabase.functions.invoke('gemini-proxy', {
            body: { mode: 'tts', text: cleanText, voice: voiceName }
          });
          if (!error && data?.audioContent) {
            return {
              audioUrl: `data:audio/mp3;base64,${data.audioContent.trim()}`,
              speaker: isAnnouncer ? 'ANNOUNCER' : (isChip ? 'CHIP' : 'SKIP'),
              text: cleanText
            };
          }
        } catch (err) {
          console.warn(`Edge function TTS failed for line "${cleanText.slice(0, 20)}...":`, err);
        }
        return {
          fallbackText: cleanText,
          speaker: isAnnouncer ? 'ANNOUNCER' : (isChip ? 'CHIP' : 'SKIP')
        };
      })
    );

    if (abortController.signal.aborted) return;

    // Sequential playback line by line
    for (let idx = 0; idx < audioItems.length; idx++) {
      if (abortController.signal.aborted) break;
      const item = audioItems[idx];
      if (!item) continue;

      if (onLineStart) onLineStart(idx, item.speaker);

      if (item.audioUrl) {
        await new Promise((resolve) => {
          const audio = new Audio(item.audioUrl);
          currentPodcastAudios.push(audio);

          const cleanup = () => {
            audio.onended = null;
            audio.onerror = null;
            resolve();
          };

          audio.onended = cleanup;
          audio.onerror = () => {
            console.warn('Audio playback error, advancing to next line:', idx);
            cleanup();
          };

          if (abortController.signal.aborted) {
            cleanup();
            return;
          }

          audio.play().catch(e => {
            console.warn('Audio play blocked or interrupted:', e);
            cleanup();
          });
        });
      } else if (item.fallbackText && typeof window !== 'undefined' && 'speechSynthesis' in window) {
        // High fidelity web speech fallback with differentiated pitch/voice
        await new Promise((resolve) => {
          const speaker = (item.speaker || '').toUpperCase().trim();
          const isAnnouncer = speaker === 'ANNOUNCER';
          const isChip = speaker === 'CHIP';
          const utterance = new SpeechSynthesisUtterance(item.fallbackText);
          
          if (isAnnouncer) {
            utterance.rate = 1.0;
            utterance.pitch = 0.95;
          } else if (isChip) {
            utterance.rate = 1.08;
            utterance.pitch = 1.15;
          } else {
            utterance.rate = 0.98;
            utterance.pitch = 0.92;
          }

          const voices = window.speechSynthesis.getVoices();
          let preferredVoice = null;
          if (isAnnouncer) {
            preferredVoice = voices.find(v => 
              v.lang.startsWith('en') && (v.name.includes('David') || v.name.includes('Google') || v.name.includes('Natural') || v.name.includes('Guy'))
            );
          } else if (isChip) {
            preferredVoice = voices.find(v => 
              v.lang.startsWith('en') && (v.name.includes('Natural') || v.name.includes('Female') || v.name.includes('Samantha') || v.name.includes('Zira'))
            );
          } else {
            preferredVoice = voices.find(v => 
              v.lang.startsWith('en') && (v.name.includes('Google') || v.name.includes('Daniel') || v.name.includes('Guy') || v.name.includes('David'))
            );
          }

          if (preferredVoice) {
            utterance.voice = preferredVoice;
          } else {
            const defaultEn = voices.find(v => v.lang.startsWith('en'));
            if (defaultEn) utterance.voice = defaultEn;
          }

          utterance.onend = resolve;
          utterance.onerror = resolve;
          window.speechSynthesis.speak(utterance);
        });
      }

      // Natural pause: slightly longer breath after official pick announcement (420ms), conversational between debate hosts (160ms)
      const pauseDuration = item.speaker === 'ANNOUNCER' ? 420 : 160;
      if (idx < audioItems.length - 1 && !abortController.signal.aborted) {
        await new Promise(r => setTimeout(r, pauseDuration));
      }
    }
  } catch (err) {
    console.warn('Podcast playback engine error:', err);
    if (onError) onError(err);
  } finally {
    if (!abortController.signal.aborted && onComplete) {
      onComplete();
    }
  }
}

export async function playPodcastTTS(htmlText, voiceName = 'en-US-Chirp3-HD-Puck') {
  const spokenText = prepareSpokenScript(htmlText);
  if (!spokenText || spokenText.length < 5) return;

  // Stop any currently playing TTS audio or speech synthesis
  stopPodcastAudio();

  // 1. Google Cloud Chirp 3 HD via Supabase gemini-proxy edge function
  try {
    const { data, error } = await supabase.functions.invoke('gemini-proxy', {
      body: { mode: 'tts', text: spokenText, voice: voiceName }
    });
    if (!error && data?.audioContent) {
      const audio = new Audio(`data:audio/mp3;base64,${data.audioContent}`);
      currentTtsAudio = audio;
      await audio.play();
      return;
    }
  } catch (err) {
    console.warn('Edge function TTS error, falling back:', err);
  }

  // 2. High-fidelity Web Speech fallback
  if (typeof window !== 'undefined' && 'speechSynthesis' in window) {
    try {
      const utterance = new SpeechSynthesisUtterance(spokenText);
      utterance.rate = 1.02;
      utterance.pitch = 1.0;
      
      const voices = window.speechSynthesis.getVoices();
      const preferredVoice = voices.find(v => 
        v.lang.startsWith('en') && (v.name.includes('Google') || v.name.includes('Natural') || v.name.includes('Samantha') || v.name.includes('Daniel'))
      ) || voices.find(v => v.lang.startsWith('en'));

      if (preferredVoice) {
        utterance.voice = preferredVoice;
      }
      window.speechSynthesis.speak(utterance);
    } catch (speechErr) {
      console.warn('Web speech synthesis failed:', speechErr);
    }
  }
}

export function playOwnerSound(ownerName) {
  if (!ownerName) return;
  try {
    const rawBase = import.meta.env.BASE_URL || '/';
    const base = rawBase.endsWith('/') ? rawBase : `${rawBase}/`;
    const audioPath = `${base}audio/owners/${ownerName.toLowerCase()}.mp3`;
    const audio = new Audio(audioPath);
    audio.play().catch(err => {
      console.log('Audio playback blocked or file not found:', err);
    });
  } catch (error) {
    console.error('Error playing sound:', error);
  }
}
