import { supabase } from '../../../supabaseClient.js';
import {
  getEffectiveRank,
  getEffectivePrice,
  getEffectiveQS,
  formatPlayerStats
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

export function generateLocalDraftCommentary(player, owner, pickNum, teamContext = {}) {
  const rank = getEffectiveRank(player);
  const price = getEffectivePrice(player);
  const surplus = rank ? (pickNum - rank) : null;
  const isPitcher = (player.Position || '').includes('SP') || (player.Position || '').includes('RP') || (player.Position || '').includes('P');
  const angle = determinePickAngle(player, pickNum, rank, surplus, teamContext);

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

  let headline = '';
  let narrative = '';
  const v = pickNum % 3;

  switch (angle) {
    case 'SURPLUS_STEAL': {
      headline = `🔥 <b>Draft Steal (+${surplus} Surplus)</b>`;
      const variants = [
        `Tremendous board patience pays off for <b>${owner}</b>. Securing a consensus #${rank} talent at pick #${pickNum} captures massive value for their roster.`,
        `The room let <b>${player.Player}</b> slide too far. <b>${owner}</b> scoops up a projected ${isPitcher ? `${k || 170} K / ${era || '3.50'} ERA profile` : `${hr || 25} HR / ${obpStr || '.350'} OBP engine`} at a +${surplus} pick discount.`,
        `Pure profit for <b>${owner}</b> here at #${pickNum}. Getting a $${price || 20} projected asset this late represents one of the cleanest value plays of the draft so far.`
      ];
      narrative = variants[v];
      break;
    }

    case 'AGGRESSIVE_REACH': {
      const reach = Math.abs(surplus);
      headline = `⚡ <b>Aggressive Target (-${reach} Reach)</b>`;
      const variants = [
        `<b>${owner}</b> refuses to wait, leaping ${reach} spots ahead of consensus board rank to secure <b>${player.Player}</b>. Clearly prioritizing ${isPitcher ? `arm talent (${k || 150}+ Ks)` : `offensive punch (${hr || 20}+ HR)`} before this tier evaporates.`,
        `Flag planted early. <b>${owner}</b> pulls the trigger on <b>${player.Player}</b> well above his #${rank || 'late'} ranking, prioritizing roster fit over market consensus.`,
        `Bold maneuvering at pick #${pickNum}. <b>${owner}</b> pays a premium to lock down ${player.Position}, betting the projections underestimate his upside.`
      ];
      narrative = variants[v];
      break;
    }

    case 'BULLPEN_CLOSER': {
      headline = `🎯 <b>High-Leverage Bullpen</b>`;
      const variants = [
        `<b>${owner}</b> tackles high-leverage relief, picking up <b>${player.Player}</b> for late-inning lockdown. Projected for ${sv || '20+'} SV+HDs with swing-and-miss stuff (${k || '70'} Ks).`,
        `Relief scarcity strikes and <b>${owner}</b> responds. <b>${player.Player}</b> gives this bullpen a proven weapon for the SV+HD and ratio categories (${era || '3.10'} ERA).`,
        `A targeted bullpen acquisition at #${pickNum}. <b>${owner}</b> secures ${sv ? `${sv} projected SV+HDs` : 'late-inning leverage'} to stay competitive in weekly relief counts.`
      ];
      narrative = variants[v];
      break;
    }

    case 'ROTATION_HEAVY': {
      headline = `⚾ <b>Rotation Stacking</b>`;
      const variants = [
        `<b>${owner}</b> continues stacking starting pitching. <b>${player.Player}</b> brings ${qs || 14} projected QS and a ${era || '3.60'} ERA to an already formidable mound group.`,
        `Mound dominance remains the clear blueprint for <b>${owner}</b>. Adding <b>${player.Player}</b> gives them another high-volume starter projected for ${k || 160} Ks.`,
        `Another starting arm in the fold for <b>${owner}</b>. Betting heavily on starting pitching volume to carry the QS and strikeout categories week in and week out.`
      ];
      narrative = variants[v];
      break;
    }

    case 'SPEED_SPECIALIST': {
      headline = `💨 <b>Elite Speed Impact</b>`;
      const variants = [
        `Sprint speed comes off the board. <b>${owner}</b> secures a game-breaker on the basepaths in <b>${player.Player}</b>, whose projected ${sb || 30} steals can swing weekly matchups alone.`,
        `A dedicated category play for <b>${owner}</b>. Adding ${sb || 25}+ stolen base potential gives this roster immense flexibility in weekly matchup planning.`,
        `Speed is scarce and <b>${owner}</b> wasn't going to get left behind. <b>${player.Player}</b> pairs ${sb || 25} SB upside with a steady ${obpStr || '.340'} OBP.`
      ];
      narrative = variants[v];
      break;
    }

    case 'POWER_SLUGGER': {
      headline = `💥 <b>Middle-of-Order Power</b>`;
      const variants = [
        `Pure offensive thumping for <b>${owner}</b>. <b>${player.Player}</b> is forecasted for ${hr || 28} homers and ${rbi || 90} RBIs, giving this lineup a formidable run-production base.`,
        `Run production dialed in. <b>${owner}</b> adds <b>${player.Player}</b> to pace the power columns, projecting for ${hr || 25}+ HRs and heavy extra-base hit damage.`,
        `<b>${owner}</b> adds serious pop at pick #${pickNum}. <b>${player.Player}</b> projects to deliver ${hr || 26} HR with strong run totals in the middle of a productive lineup.`
      ];
      narrative = variants[v];
      break;
    }

    case 'K_MACHINE': {
      headline = `🔥 <b>Strikeout Punch</b>`;
      const variants = [
        `Whiff-rate premium for <b>${owner}</b>. <b>${player.Player}</b> generates elite swing-and-miss stuff, projected for ${k || 190} strikeouts across ${qs || 15} quality starts.`,
        `<b>${owner}</b> dials up the strikeout punch with <b>${player.Player}</b>. High-ceiling stuff on the mound that elevates weekly K totals immediately.`,
        `Missing bats at a high clip. <b>${owner}</b> nabs <b>${player.Player}</b> to fortify strikeouts (${k || 180}+ K proj) and keep WHIP in check (${whip || '1.15'}).`
      ];
      narrative = variants[v];
      break;
    }

    case 'CORNERSTONE_FOUNDATION': {
      headline = `👑 <b>Early Anchor Pick</b>`;
      const variants = [
        `<b>${owner}</b> lays a premier cornerstone in round ${Math.ceil(pickNum / 10)}. <b>${player.Player}</b> provides an elite baseline of ${isPitcher ? `${k || 190} Ks and ${era || '3.20'} ERA` : `${hr || 28} HR and .${String(obp || '360').replace('0.', '')} OBP`} to build around.`,
        `A foundational piece for <b>${owner}</b> at pick #${pickNum}. <b>${player.Player}</b> delivers high-floor excellence and marquee category contribution right out of the gate.`,
        `Starting strong: <b>${owner}</b> secures <b>${player.Player}</b> to anchor the top of their draft board with elite projected production.`
      ];
      narrative = variants[v];
      break;
    }

    case 'LATE_ROUND_FLYER': {
      headline = `🎲 <b>Late-Round Upside</b>`;
      const variants = [
        `Calculated risk at pick #${pickNum}. <b>${owner}</b> takes a flyer on <b>${player.Player}</b>, who offers intriguing upside if playing time breaks his way.`,
        `Late-draft upside hunting for <b>${owner}</b>. At this stage of the draft, <b>${player.Player}</b>'s skill profile (${isPitcher ? `${k || 120} K upside` : `${hr || 18} HR power`}) is well worth the roster spot.`,
        `High-ceiling play late. <b>${owner}</b> adds <b>${player.Player}</b> at #${pickNum} to round out roster flexibility before the final rounds conclude.`
      ];
      narrative = variants[v];
      break;
    }

    case 'VALUE_PICK': {
      headline = `📈 <b>Above-Average Value (+${surplus})</b>`;
      const variants = [
        `Smart draft board reading by <b>${owner}</b>. <b>${player.Player}</b> slips past his #${rank} projection to deliver a favorable ${surplus}-pick surplus at #${pickNum}.`,
        `Consensus value in the bag. <b>${owner}</b> capitalizes on <b>${player.Player}</b> sliding ${surplus} spots past expected board rank for a steady ${player.Position} addition.`,
        `Efficient acquisition at #${pickNum}. <b>${owner}</b> collects positive equity on <b>${player.Player}</b> without having to force the issue.`
      ];
      narrative = variants[v];
      break;
    }

    case 'PRIORITY_TARGET': {
      headline = `⚡ <b>Targeted Priority (-${Math.abs(surplus)})</b>`;
      const variants = [
        `<b>${owner}</b> didn't want to risk losing <b>${player.Player}</b>, pulling him ${Math.abs(surplus)} spots early to ensure ${player.Position} coverage.`,
        `Target locked. <b>${owner}</b> steps ahead of the curve at pick #${pickNum} to bring in <b>${player.Player}</b> ahead of market consensus.`,
        `Proactive roster construction. <b>${owner}</b> pays a modest reach price to secure <b>${player.Player}</b>'s specific statistical profile.`
      ];
      narrative = variants[v];
      break;
    }

    case 'BALANCED_FIT':
    default: {
      headline = `🎯 <b>On-Market Selection</b>`;
      const variants = [
        `Right on script at pick #${pickNum}. <b>${owner}</b> selects <b>${player.Player}</b> directly in line with board valuation, addressing ${player.Position} with projected ${isPitcher ? `${era || '3.70'} ERA / ${k || 140} K` : `${hr || 20} HR / ${rbi || 70} RBI`}.`,
        `Clean, disciplined pick by <b>${owner}</b>. <b>${player.Player}</b> comes off the board at fair market value to provide steady ${player.Position} production.`,
        `<b>${owner}</b> stays balanced at #${pickNum}, picking up <b>${player.Player}</b> to keep weekly counting categories and ratios on pace.`
      ];
      narrative = variants[v];
      break;
    }
  }

  const footer = `<br><br><span style="color:#888; font-size:11px;">Rank: #${rank || '—'} • Proj Value: $${price || '—'} • Pos: ${player.Position} • Team: ${player.Team || 'MLB'}</span>`;

  return `${headline}<br><br>${narrative}${footer}`;
}

export async function generateDraftCommentary(player, owner, pickNum, teamContext = {}, season = 2027) {
  const rank = getEffectiveRank(player);
  const price = getEffectivePrice(player);
  const surplus = rank ? (pickNum - rank) : null;
  const angle = determinePickAngle(player, pickNum, rank, surplus, teamContext);
  const statLine = formatPlayerStats(player);

  const posBreakdown = typeof teamContext === 'object' && teamContext?.positionCounts
    ? Object.entries(teamContext.positionCounts).map(([pos, c]) => `${pos}: ${c}`).join(', ')
    : (typeof teamContext === 'string' ? teamContext : 'None yet');

  const recentPicksStr = typeof teamContext === 'object' && teamContext?.recentPicks?.length
    ? teamContext.recentPicks.join(' ➔ ')
    : 'None yet';

  const surplusDesc = surplus !== null
    ? (surplus >= 0 ? `+${surplus} spots past consensus rank (value slide)` : `${surplus} spots ahead of consensus rank (reach)`)
    : 'In line with consensus';

  const prompt = `You are a sharp, analytical fantasy baseball war-room commentator providing instant reaction for the ${season} draft.
League Format: Head-to-Head Each Category (R, HR, RBI, SB, OBP | K, QS, ERA, WHIP, SV+HD).

SELECTION:
- Manager: ${owner}
- Player: ${player.Player} (${player.Position} - ${player.Team || 'MLB'})
- Pick: #${pickNum}
- Board Rank: ${rank ? `#${rank}` : 'Unranked'}
- Projected Value: ${price ? `$${price}` : 'N/A'}
- Surplus vs Pick: ${surplusDesc}
- Player Projections: ${statLine}

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
1. Write 2 to 3 concise, punchy sentences (strictly under 65 words).
2. Focus on specific numbers (e.g. 30+ HR, sub-3.30 ERA, 25 SB, or +15 pick surplus) and tactical roster construction.
3. Sound like a knowledgeable, sharp fantasy analyst in an athletic war room.
4. Format with clean HTML: Use <b> tags for emphasis. Do NOT include markdown code blocks.
5. End with:
<br><br><span style="color:#888; font-size:11px;">Rank: #${rank || '—'} • Proj Value: $${price || '—'} • Pos: ${player.Position} • MLB: ${player.Team || 'MLB'}</span>`;

  const aiResponse = await callGemini(prompt);
  if (aiResponse && aiResponse !== 'Analysis unavailable') {
    const cleaned = aiResponse.replace(/^```html\s*/i, '').replace(/```\s*$/i, '').trim();
    return cleaned;
  }
  return generateLocalDraftCommentary(player, owner, pickNum, teamContext);
}

// --- AUDIO SYSTEM ---
let currentTtsAudio = null;

export function prepareSpokenScript(htmlText) {
  if (!htmlText) return '';
  // Strip HTML tags
  let text = htmlText.replace(/<[^>]*>/g, ' ');
  // Remove metadata footers (Rank, Proj Value, Pos, Team, etc.)
  text = text.replace(/Rank:\s*#?\d*.*?Team:\s*[\w\s]*/gi, ' ');
  text = text.replace(/Board:\s*#?\d*.*?MLB:\s*[\w\s]*/gi, ' ');
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
