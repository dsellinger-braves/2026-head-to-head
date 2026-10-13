import { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { supabase } from '../supabaseClient';
import defaultDraftAssetTrades from '../data/draftAssetTrades2026.json';
import defaultTeamBudgets from '../data/teamBudgets2026.json';
import defaultCompPicks from '../data/compensationPicks2026.json';
import defaultKeepers from '../data/keeperInput2026.json';
import defaultKeepers2027 from '../data/keeperInput2027.json';
import defaultDraft2026 from '../data/draft2026.json';
import defaultCalculations from '../data/keeperCalculations.json';
import HistoricalDraftView from './HistoricalDraftView';
import { getPlayerHeadshotUrl, handleHeadshotError, updateGlobalPlayerLookup } from '../utils/headshotUtils';
import {
  normalizeManager,
  computeLiveDraftRoto,
  detectPositionalRun,
  getSmartDraftRecommendations
} from '../utils/draftRotoEngine';
import styles from './draft/styles/draftRoomStyles';
import {
  mappedDraft2026History,
  normalizeName,
  enrichPlayerWithCalculations,
  mergeEspnData,
  mergeZipsData,
  DRAFT_OWNERS,
  generateDefaultDraftOrder,
  getInjuryIndicator,
  generate2027DraftOrder,
  getEffectiveRank,
  getEffectivePrice
} from './draft/utils/draftCalculations';
import {
  fetchFromGCS,
  fetchFanGraphs,
  fetchAllEspnPlayers,
  draftDbGet,
  draftDbSet
} from './draft/services/draftDataService';
import {
  generateDraftCommentary,
  generateLocalDraftCommentary,
  renderPickSummaryCard,
  getPlayerDraftHistory,
  stopPodcastAudio,
  generateLocalPodcastDialogue,
  generatePodcastDialogue,
  playDualSpeakerPodcast,
  playPodcastTTS
} from './draft/services/draftAudio';

export { DRAFT_OWNERS };

// --- SUB-COMPONENTS & VIEWS (MODULARIZATION) ---
import HostBroadcastView from './draft/views/HostBroadcastView';
import MobileDraftView from './draft/views/MobileDraftView';
import useMockDraftEngine from './draft/hooks/useMockDraftEngine';
import DesktopHeader from './draft/components/DesktopHeader';
import WarRoomDrawer from './draft/components/WarRoomDrawer';
import MockDraftController from './draft/components/MockDraftController';
import PlayerModal from './draft/components/PlayerModal';
import PlayerNameButton from './draft/components/PlayerNameButton';
import ModeSelectionModal from './draft/components/ModeSelectionModal';
import LoginModal from './draft/components/LoginModal';
import {
  Ticker,
  OnDeckSidebar,
  TeamNeedsSummary,
  RecentActivityWidget,
  QueuePreviewWidget,
  PositionalRunRadar,
  CategoryDeficitHUD
} from './draft/components/DraftWidgets';

export default function DraftRoomView({ 
  onOpenPlayerModal, 
  onSwitchView, 
  seasonYear = 2027, 
  onSeasonYearChange 
}) {
  const [roomSeason, setRoomSeason] = useState(seasonYear || 2027);
  const [draftMode, setDraftMode] = useState(() => localStorage.getItem('draftMode') || null);
  const [currentUser, setCurrentUser] = useState(() => localStorage.getItem('draftUser') || null);
  const [players, setPlayers] = useState([]);
  const [picks, setPicks] = useState([]);
  const [queue, setQueue] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem('draft_queue') || '[]');
    } catch {
      return [];
    }
  });
  const [pickStartTime, setPickStartTime] = useState(() => Date.now());
  const [activeTab, setActiveTab] = useState('Pool');
  const [draftTrades, setDraftTrades] = useState(defaultDraftAssetTrades || []);
  const draftTradesRef = useRef(draftTrades);
  useEffect(() => {
    draftTradesRef.current = draftTrades;
  }, [draftTrades]);
  const [_teamBudgets, setTeamBudgets] = useState(defaultTeamBudgets?.budgets || []);
  const [_compPicks, setCompPicks] = useState(defaultCompPicks?.comp_picks || []);
  const [_keepers, setKeepers] = useState(seasonYear === 2027 ? (defaultKeepers2027?.keepers || defaultKeepers2027 || []) : (defaultKeepers?.keepers || []));
  const [showDashboard, setShowDashboard] = useState(false);
  
  // Test mode state
  const [testModePicks, setTestModePicks] = useState([]);
  
  // AI Commentary state
  const [lastPickCommentary, setLastPickCommentary] = useState("Draft has not started.");
  const [generatingCommentary, setGeneratingCommentary] = useState(false);
  
  // 2-Person Podcast Debate state (Skip & Chip Google Cloud Chirp 3 HD)
  const [lastPodcastScript, setLastPodcastScript] = useState(null);
  const [generatingPodcast, setGeneratingPodcast] = useState(false);
  const [isPlayingPodcast, setIsPlayingPodcast] = useState(false);
  const [activePodcastLine, setActivePodcastLine] = useState(null);

  // Audio control callbacks
  const handlePlayPodcast = useCallback((customScript = null) => {
    const scriptToPlay = customScript || lastPodcastScript;
    if (!scriptToPlay || !scriptToPlay.length) return;
    if (isPlayingPodcast) {
      stopPodcastAudio();
      setIsPlayingPodcast(false);
      setActivePodcastLine(null);
      return;
    }
    setIsPlayingPodcast(true);
    playDualSpeakerPodcast(scriptToPlay, {
      onLineStart: (idx) => setActivePodcastLine(idx),
      onComplete: () => {
        setIsPlayingPodcast(false);
        setActivePodcastLine(null);
      },
      onError: () => {
        setIsPlayingPodcast(false);
        setActivePodcastLine(null);
      }
    });
  }, [isPlayingPodcast, lastPodcastScript]);

  const handleStopPodcast = useCallback(() => {
    stopPodcastAudio();
    setIsPlayingPodcast(false);
    setActivePodcastLine(null);
  }, []);
  
  // Analysis history storage
  const [analysisHistory, setAnalysisHistory] = useState([]);
  
  // Player modal state
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [playerInfo, setPlayerInfo] = useState([]);

  // Static and merged draft data states
  const [_endingRoster, setEndingRoster] = useState([]);
  const [draftHistory, setDraftHistory] = useState([]);
  const draftHistoryRef = useRef(draftHistory);
  useEffect(() => {
    draftHistoryRef.current = draftHistory;
  }, [draftHistory]);
  const [historicalFinish, setHistoricalFinish] = useState([]);
  const [battersZips, setBattersZips] = useState([]);
  const [pitchersZips, setPitchersZips] = useState([]);
  const [savantBatters, setSavantBatters] = useState([]);
  const [savantPitchers, setSavantPitchers] = useState([]);
  const [_espnPlayers, setEspnPlayers] = useState([]);

  // Audio state
  const [audioEnabled, setAudioEnabled] = useState(false);
  const audioEnabledRef = useRef(audioEnabled);
  useEffect(() => {
    audioEnabledRef.current = audioEnabled;
  }, [audioEnabled]);

  const [resetting, setResetting] = useState(false);

  // Sync roomSeason when parent seasonYear prop changes
  useEffect(() => {
    if (seasonYear && seasonYear !== roomSeason) {
      setRoomSeason(seasonYear);
      setTestModePicks([]);
      setAnalysisHistory([]);
      setLastPickCommentary(seasonYear === 2026 ? "Viewing 2026 completed draft archive." : "Draft has not started.");
      setLastPodcastScript(null);
      setIsPlayingPodcast(false);
      setActivePodcastLine(null);
      stopPodcastAudio();
    }
  }, [seasonYear, roomSeason]);

  const handleSeasonChange = (newYear) => {
    setRoomSeason(newYear);
    setTestModePicks([]);
    setAnalysisHistory([]);
    setLastPickCommentary(newYear === 2026 ? "Viewing 2026 completed draft archive." : "Draft has not started.");
    setLastPodcastScript(null);
    setIsPlayingPodcast(false);
    setActivePodcastLine(null);
    stopPodcastAudio();
    if (onSeasonYearChange) onSeasonYearChange(newYear);
  };

  const displayPicks = useMemo(() => {
    if (draftMode === 'test' || draftMode === 'mockdraft') {
      if (testModePicks.length > 0) return testModePicks;
      const liveCutoff = roomSeason === 2027 ? 55 : 46;
      return picks.map(p => p['Overall Pick'] >= liveCutoff ? {
        ...p,
        'ESPN PlayerID': null,
        Selection: null
      } : p);
    }
    return picks;
  }, [draftMode, testModePicks, picks, roomSeason]);

  const displayPlayers = useMemo(() => {
    const keeperMap = new Map();
    if (roomSeason === 2027) {
      // For 2027 draft, the ONLY players that should show as currently rostered
      // are the presumed 2027 keeper picks. All other players must show as Available.
      (_keepers || []).forEach(k => {
        const rawOwner = k.owner || k.owner_name || k.team_owner || '';
        const owner = (rawOwner === 'Dan' || rawOwner === 'dsellinger') ? 'Daniel' : rawOwner;
        const pid = String(k.espn_player_id || k.player_id || '');
        if (pid) keeperMap.set(pid, { owner, slot: k.keeper_slot });
      });
    }

    const baseList = (!players || players.length === 0)
      ? (defaultCalculations?.players || []).map(cp => ({
          'ESPN PlayerID': String(cp.espn_player_id || cp.player_id || ''),
          id: cp.player_id,
          Player: cp.player_name,
          Position: cp.position || (cp.is_pitcher ? (cp.pitcher_role || 'SP') : 'DH'),
          Team: cp.team || 'MLB',
          Availability: 'Available'
        }))
      : players;

    return baseList.map(p => {
      const enriched = enrichPlayerWithCalculations(p, roomSeason);
      if (roomSeason === 2027) {
        const pid = String(enriched['ESPN PlayerID'] || enriched.espn_player_id || enriched.id || '');
        const kInfo = keeperMap.get(pid);
        return {
          ...enriched,
          Availability: kInfo ? kInfo.owner : 'Available',
          isKeeper: !!kInfo,
          keeperOwner: kInfo ? kInfo.owner : null,
          keeperSlot: kInfo ? kInfo.slot : null
        };
      }
      return enriched;
    });
  }, [players, roomSeason, _keepers]);

  const playersRef = useRef(displayPlayers);
  useEffect(() => {
    playersRef.current = displayPlayers;
    updateGlobalPlayerLookup(displayPlayers);
  }, [displayPlayers]);

  const picksRef = useRef(displayPicks);
  useEffect(() => {
    picksRef.current = displayPicks;
  }, [displayPicks]);

  const currentPick = useMemo(() => {
    if (roomSeason === 2027) {
      // In 2027 draft prep, live drafting begins at pick 55 (picks 1-54 are 6 keeper rounds)
      return displayPicks.find(p => p['Overall Pick'] >= 55 && !p['ESPN PlayerID']);
    }
    return displayPicks.find(p => !p['ESPN PlayerID']);
  }, [displayPicks, roomSeason]);

  // Commentary callback - stable reference using playersRef
  const handleNewPick = useCallback(async (pick) => {
    const player = playersRef.current.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
    if (!player) return;

    const currentPicks = picksRef.current || [];
    const currentPlayers = playersRef.current || [];
    const currentOverall = Number(pick['Overall Pick']);

    // Find the very next pick in the serpentine schedule to announce who is on the clock
    const nextPick = currentPicks.find(p => Number(p['Overall Pick']) === currentOverall + 1) ||
                     currentPicks.find(p => Number(p['Overall Pick']) > currentOverall && !p['ESPN PlayerID']);
    const nextOwner = nextPick?.Owner || null;

    // If pick already comes with commentary, display immediately without regenerating (unless containing placeholder #9999 artifact)
    if (pick.ai_commentary && !pick.ai_commentary.includes('-9944') && !pick.ai_commentary.includes('#9999')) {
      let commentary = pick.ai_commentary;
      if (!commentary.includes('PICK_SUMMARY_CARD') && !commentary.includes('PICK GRADE')) {
        const rank = getEffectiveRank(player);
        const price = getEffectivePrice(player);
        const surplus = rank ? (Number(pick['Overall Pick']) - rank) : null;
        const playerHistory = getPlayerDraftHistory(player, draftHistoryRef.current);
        commentary = `${commentary}${renderPickSummaryCard({
          player,
          pickNum: Number(pick['Overall Pick']),
          rank,
          price,
          surplus,
          playerDraftHistory: playerHistory
        })}`;
      }
      setLastPickCommentary(commentary);
      const script = pick.podcast_script || generateLocalPodcastDialogue(player, pick.Owner, pick['Overall Pick'], {}, roomSeason, nextOwner);
      setLastPodcastScript(script);
      const historyEntry = {
        pickNumber: pick['Overall Pick'],
        round: pick.Round,
        owner: pick.Owner,
        playerName: player.Player,
        position: player.Position,
        team: player.Team,
        commentary,
        podcastScript: script,
        nextOwner,
        timestamp: new Date().toLocaleTimeString()
      };
      setAnalysisHistory(prev => {
        if (prev.some(h => h.pickNumber === historyEntry.pickNumber)) return prev;
        return [...prev, historyEntry];
      });
      return;
    }
    
    setGeneratingCommentary(true);
    setGeneratingPodcast(true);

    // Find all previous picks by this owner
    const ownerPriorPicks = currentPicks.filter(p => 
      p.Owner === pick.Owner && 
      p['ESPN PlayerID'] && 
      p['Overall Pick'] !== pick['Overall Pick'] &&
      p['Overall Pick'] < (pick['Overall Pick'] || 999)
    );

    const posCounts = {};
    const recentPicks = [];
    ownerPriorPicks.forEach(p => {
      const pl = currentPlayers.find(x => String(x['ESPN PlayerID']) === String(p['ESPN PlayerID']));
      const pos = pl?.Position || p.Position || 'UTIL';
      const mainPos = pos.split(/[/,]/)[0].trim();
      posCounts[mainPos] = (posCounts[mainPos] || 0) + 1;
      const name = pl?.Player || p.Player;
      if (name) {
        recentPicks.push(`${name} (R${p.Round || '?'})`);
      }
    });

    const teamContext = {
      rosterCount: ownerPriorPicks.length,
      positionCounts: posCounts,
      recentPicks: recentPicks.slice(-3),
      totalTeamPicks: ownerPriorPicks.length + 1
    };
    
    const [commentary, podcastDialogue] = await Promise.all([
      generateDraftCommentary(
        player,
        pick.Owner,
        pick['Overall Pick'],
        teamContext,
        roomSeason,
        draftHistoryRef.current
      ),
      generatePodcastDialogue(
        player,
        pick.Owner,
        pick['Overall Pick'],
        teamContext,
        roomSeason,
        nextOwner
      )
    ]);
    
    setLastPickCommentary(commentary);
    setGeneratingCommentary(false);
    setLastPodcastScript(podcastDialogue);
    setGeneratingPodcast(false);

    // Official pick announcement + Skip & Chip dual-voice debate plays if audio is enabled
    if (audioEnabledRef.current && podcastDialogue && podcastDialogue.length > 0) {
      setTimeout(() => {
        setIsPlayingPodcast(true);
        playDualSpeakerPodcast(podcastDialogue, {
          onLineStart: (idx) => setActivePodcastLine(idx),
          onComplete: () => {
            setIsPlayingPodcast(false);
            setActivePodcastLine(null);
          },
          onError: () => {
            setIsPlayingPodcast(false);
            setActivePodcastLine(null);
          }
        });
      }, 500);
    }
    
    const historyEntry = {
      pickNumber: pick['Overall Pick'],
      round: pick.Round,
      owner: pick.Owner,
      playerName: player.Player,
      position: player.Position,
      team: player.Team,
      commentary: commentary,
      podcastScript: podcastDialogue,
      nextOwner,
      timestamp: new Date().toLocaleTimeString()
    };
    setAnalysisHistory(prev => {
      if (prev.some(h => h.pickNumber === historyEntry.pickNumber)) return prev;
      return [...prev, historyEntry];
    });

    // Persist to Supabase draft_picks if in 2027 live draft
    if (roomSeason === 2027 && pick['Overall Pick']) {
      try {
        await supabase
          .from('draft_picks')
          .update({ 
            ai_commentary: commentary,
            podcast_script: podcastDialogue 
          })
          .eq('season_year', 2027)
          .eq('overall_pick', pick['Overall Pick']);
      } catch (err) {
        console.warn('Could not persist AI commentary to draft_picks:', err);
      }
    }
  }, [roomSeason]);

  const fetchDraftOrder = useCallback(async () => {
    try {
      if (roomSeason === 2027) {
        let trades = draftTradesRef.current;
        let keepers2027 = [];
        let compPicks2027 = [];
        let livePicks2027 = [];

        try {
          const [tradesRes, keepersRes, cpRes, liveRes] = await Promise.all([
            supabase.from('draft_asset_trades').select('*').order('trade_date', { ascending: true }),
            supabase.from('draft_keepers').select('*').eq('season_year', 2027).order('keeper_slot', { ascending: true }),
            supabase.from('draft_compensation_picks').select('*').eq('season_year', 2027).order('round_num', { ascending: true }),
            supabase.from('draft_picks').select('*').eq('season_year', 2027).order('overall_pick', { ascending: true })
          ]);
          if (tradesRes?.data && tradesRes.data.length > 0) {
            trades = tradesRes.data;
            setDraftTrades(prev => prev.length === tradesRes.data.length ? prev : tradesRes.data);
          }
          if (keepersRes?.data && keepersRes.data.length > 0) {
            keepers2027 = keepersRes.data;
            setKeepers(prev => prev.length === keepersRes.data.length ? prev : keepersRes.data);
          } else {
            const fallbackKeepers = defaultKeepers2027?.keepers || defaultKeepers2027 || [];
            keepers2027 = fallbackKeepers;
            setKeepers(prev => (prev.length === fallbackKeepers.length ? prev : fallbackKeepers));
          }
          if (cpRes?.data && cpRes.data.length > 0) {
            compPicks2027 = cpRes.data;
            setCompPicks(prev => prev.length === cpRes.data.length ? prev : cpRes.data);
          }
          if (liveRes?.data) {
            livePicks2027 = liveRes.data;
            const existingComments = liveRes.data
              .filter(lp => lp.player_id)
              .map(lp => {
                let commentary = lp.ai_commentary || '';
                const pl = (playersRef.current || []).find(x => String(x['ESPN PlayerID']) === String(lp.player_id));
                if (pl) {
                  if (!commentary || commentary.trim().length === 0 || commentary.includes('-9944') || commentary.includes('#9999')) {
                    commentary = generateLocalDraftCommentary(pl, lp.team_owner, Number(lp.overall_pick), {}, draftHistoryRef.current);
                  } else if (!commentary.includes('PICK_SUMMARY_CARD') && !commentary.includes('PICK GRADE')) {
                    const rank = getEffectiveRank(pl);
                    const price = getEffectivePrice(pl);
                    const surplus = rank ? (Number(lp.overall_pick) - rank) : null;
                    const playerHistory = getPlayerDraftHistory(pl, draftHistoryRef.current);
                    commentary = `${commentary}${renderPickSummaryCard({
                      player: pl,
                      pickNum: Number(lp.overall_pick),
                      rank,
                      price,
                      surplus,
                      playerDraftHistory: playerHistory
                    })}`;
                  }
                }
                return {
                  pickNumber: lp.overall_pick,
                  round: lp.round,
                  owner: lp.team_owner,
                  playerName: lp.player_name,
                  position: lp.player_position || '',
                  team: lp.player_team || '',
                  commentary,
                  timestamp: lp.picked_at ? new Date(lp.picked_at).toLocaleTimeString() : ''
                };
              });
            if (existingComments.length > 0) {
              setAnalysisHistory(prev => {
                const map = new Map(prev.map(item => [item.pickNumber, item]));
                existingComments.forEach(item => {
                  if (!map.has(item.pickNumber)) map.set(item.pickNumber, item);
                });
                return Array.from(map.values()).sort((a, b) => a.pickNumber - b.pickNumber);
              });
              setLastPickCommentary(existingComments[existingComments.length - 1].commentary);
            }
          }
        } catch (e) {
          console.warn('Supabase 2027 data fetch notice:', e);
        }

        const generated = generate2027DraftOrder(trades, keepers2027, compPicks2027);

        if (livePicks2027.length > 0) {
          const liveMap = new Map(livePicks2027.map(lp => [lp.overall_pick, lp]));
          const merged = generated.map(p => {
            const lp = liveMap.get(p['Overall Pick']);
            if (lp) {
              return {
                ...p,
                'ESPN PlayerID': lp.player_id,
                Selection: lp.player_name,
                ai_commentary: lp.ai_commentary
              };
            }
            return p;
          });
          setPicks(merged);
        } else {
          setPicks(generated);
        }
      } else {
        let loadedPicks = null;
        try {
          const { data, error } = await supabase
            .from('draft-order')
            .select('*')
            .order('Overall Pick', { ascending: true });
          if (!error && data && data.length > 0) {
            loadedPicks = data;
          }
        } catch (e) {
          console.warn('Supabase draft-order fetch error:', e);
        }

        if (loadedPicks && loadedPicks.length > 0) {
          setPicks(loadedPicks);
        } else if (roomSeason === 2026 && defaultDraft2026?.length > 0) {
          setPicks(defaultDraft2026.map(p => ({
            'Overall Pick': p.overall_pick,
            Round: p.round,
            Pick: p.pick,
            Owner: p.team_owner,
            'ESPN PlayerID': p.player_id,
            Selection: p.player_name,
            isKeeper: Boolean(p.is_keeper),
            is_keeper: Boolean(p.is_keeper)
          })));
        } else {
          setPicks(generateDefaultDraftOrder());
        }

        try {
          const [dtData, budgetsRes, cpRes, keepersRes] = await Promise.all([
            supabase.from('draft_asset_trades').select('*').order('trade_date', { ascending: true }),
            supabase.from('draft_team_budgets').select('*').order('finish_rank', { ascending: true }),
            supabase.from('draft_compensation_picks').select('*').order('round_num', { ascending: true }),
            supabase.from('draft_keepers').select('*').order('keeper_slot', { ascending: true })
          ]);
          if (dtData?.data && dtData.data.length > 0) {
            setDraftTrades(prev => prev.length === dtData.data.length ? prev : dtData.data);
          }
          if (budgetsRes?.data && budgetsRes.data.length > 0) {
            setTeamBudgets(prev => prev.length === budgetsRes.data.length ? prev : budgetsRes.data);
          }
          if (cpRes?.data && cpRes.data.length > 0) {
            setCompPicks(prev => prev.length === cpRes.data.length ? prev : cpRes.data);
          }
          if (keepersRes?.data && keepersRes.data.length > 0) {
            setKeepers(prev => prev.length === keepersRes.data.length ? prev : keepersRes.data);
          }
        } catch (e) {
          console.warn('Supabase supporting data fetch notice:', e);
        }
      }
    } catch (err) {
      console.warn('Draft order fetch error, fallback:', err);
      if (roomSeason === 2027) {
        setPicks(generate2027DraftOrder(draftTradesRef.current, [], []));
      } else if (roomSeason === 2026 && defaultDraft2026?.length > 0) {
        setPicks(defaultDraft2026.map(p => ({
          'Overall Pick': p.overall_pick,
          Round: p.round,
          Pick: p.pick,
          Owner: p.team_owner,
          'ESPN PlayerID': p.player_id,
          Selection: p.player_name,
          isKeeper: Boolean(p.is_keeper),
          is_keeper: Boolean(p.is_keeper)
        })));
      } else {
        setPicks(generateDefaultDraftOrder());
      }
    }
  }, [roomSeason]);

  const fetchStaticData = useCallback(async () => {
    console.log(`📦 Fetching static data...`);
    try {
      let pool = [];
      let ending = [];
      let history = [];
      let finishes = [];

      try {
        const [poolRes, endingRes, dhRes, hfRes, settingsRes] = await Promise.all([
          supabase.from('player-pool').select('*'),
          supabase.from('ending-roster').select('*'),
          supabase.from('draft_picks').select('*').order('season_year', { ascending: false }).order('overall_pick', { ascending: true }),
          supabase.from('historical_finishes').select('*').order('season_year', { ascending: false }),
          supabase.from('league_settings').select('*').limit(1)
        ]);

        pool = poolRes?.data || [];
        ending = endingRes?.data || [];
        if (dhRes?.data && dhRes.data.length > 0) {
          history = dhRes.data.map(p => ({
            Year: String(p.season_year),
            Round: String(p.round || ''),
            Pick_Overall: String(p.overall_pick),
            Player_Name: p.player_name,
            Team_ID: p.team_owner,
            Owner: p.team_owner,
            player_id: String(p.player_id || ''),
            Keeper: p.is_keeper ? 'True' : 'False'
          }));
        }
        if (hfRes?.data && hfRes.data.length > 0) {
          finishes = hfRes.data.map(f => ({
            Year: String(f.season_year),
            Owner: f.team_owner,
            "Final Rank": String(f.final_place || ''),
            Points: String(f.total_roto_points || ''),
            "Active Owner?": f.is_active_owner ? 'Y' : 'N',
            ...(f.category_ranks || {})
          }));
        }
        if (settingsRes?.data?.[0]?.current_season) {
          console.log(`⚾ Active League Season: ${settingsRes.data[0].current_season}`);
        }
      } catch (dbErr) {
        console.warn('Supabase warehouse query notice:', dbErr);
      }

      if (!pool.length) {
        pool = await fetchFromGCS('player-pool.json', 'gcs_player_pool') || [];
      }
      if (!history.length) {
        history = await fetchFromGCS('draft-history.json', 'gcs_draft_history_v2') || [];
      }
      const has2026 = history.some(d => String(d.Year || d.year) === '2026');
      if (!has2026 && mappedDraft2026History.length > 0) {
        history = [...mappedDraft2026History, ...history];
      }
      if (!finishes.length) {
        finishes = await fetchFromGCS('historical-finish.json', 'gcs_league_history') || [];
      }

      console.log(`✅ Loaded ${ending.length} ending roster entries`);
      console.log(`📦 Core data loaded: ${pool.length} players, ${history.length} draft history records, ${finishes.length} historical finish records`);

      const [battingZipsData, pitchingZipsData] = await Promise.all([
        fetchFanGraphs('batting'),
        fetchFanGraphs('pitching')
      ]);

      let savantBat = [];
      let savantPitch = [];
      const savantResults = await Promise.allSettled([
        fetchFromGCS('savant-batting.json', 'gcs_bat_savant_2025'),
        fetchFromGCS('savant-pitching.json', 'gcs_pitch_savant_2025')
      ]);
      if (savantResults[0].status === 'fulfilled') savantBat = savantResults[0].value || [];
      if (savantResults[1].status === 'fulfilled') savantPitch = savantResults[1].value || [];

      let espn = [];
      try {
        const cached = await draftDbGet('espn_player_info');
        if (cached) {
          const { data, timestamp } = cached;
          if (Date.now() - timestamp < 3600000) {
            espn = data;
            console.log(`📺 Using cached ESPN data`);
          }
        }
        if (espn.length === 0) {
          console.log(`📺 Fetching fresh ESPN data...`);
          espn = await fetchAllEspnPlayers();
          if (espn.length > 0) {
            await draftDbSet('espn_player_info', {
              data: espn,
              timestamp: Date.now()
            });
          }
        }
      } catch (err) {
        console.error('ESPN fetch error:', err);
      }

      let mergedPlayers = pool;
      if (espn.length > 0) {
        mergedPlayers = mergeEspnData(pool, espn);
      }
      if (battingZipsData.length > 0 || pitchingZipsData.length > 0) {
        console.log(`📊 Starting ZiPS merge: ${battingZipsData.length} batters, ${pitchingZipsData.length} pitchers`);
        mergedPlayers = mergeZipsData(mergedPlayers, battingZipsData, pitchingZipsData);
      }

      // Enrich all pool players with official keeper calculations
      mergedPlayers = mergedPlayers.map(p => enrichPlayerWithCalculations(p, roomSeason));

      // Append any players from keeperCalculations.json that were missing from pool
      const existingIds = new Set(mergedPlayers.map(p => String(p['ESPN PlayerID'] || p.espn_player_id || p.id || '')));
      const existingNames = new Set(mergedPlayers.map(p => normalizeName(p.Player || p.Name || p.player_name || '')));

      (defaultCalculations?.players || []).forEach(cp => {
        const cPid = String(cp.espn_player_id || cp.player_id || '');
        const cName = normalizeName(cp.player_name || '');
        if ((!cPid || !existingIds.has(cPid)) && (!cName || !existingNames.has(cName))) {
          mergedPlayers.push(enrichPlayerWithCalculations({
            'ESPN PlayerID': cPid,
            id: cp.player_id || parseInt(cPid, 10),
            Player: cp.player_name,
            Position: cp.position || (cp.is_pitcher ? (cp.pitcher_role || 'SP') : 'DH'),
            Team: cp.team || 'MLB',
            Availability: 'Available'
          }, roomSeason));
        }
      });

      const mapWithMlbam = list => Array.isArray(list) ? list.map(item => ({
        ...item,
        MLBAMID: item.mlbamid || item.MLBAMID || item.playerid
      })) : [];

      setPlayers(mergedPlayers);
      setEndingRoster(ending);
      setDraftHistory(history);
      setHistoricalFinish(finishes);
      setBattersZips(mapWithMlbam(battingZipsData));
      setPitchersZips(mapWithMlbam(pitchingZipsData));
      setSavantBatters(savantBat);
      setSavantPitchers(savantPitch);
      if (espn.length > 0) {
        setPlayerInfo(espn);
        setEspnPlayers(espn);
      }
      console.log(`✅ All data loaded. Batters: ${battingZipsData.length}, Pitchers: ${pitchingZipsData.length}`);
    } catch (err) {
      console.error(`❌ Error fetching static data:`, err);
    }
  }, [roomSeason]);

  const fetchStaticDataRef = useRef(fetchStaticData);
  const fetchDraftOrderRef = useRef(fetchDraftOrder);
  const handleNewPickRef = useRef(handleNewPick);
  useEffect(() => {
    fetchStaticDataRef.current = fetchStaticData;
    fetchDraftOrderRef.current = fetchDraftOrder;
    handleNewPickRef.current = handleNewPick;
  });

  useEffect(() => {
    if (!draftMode) return;

    fetchStaticDataRef.current();

    if (draftMode === 'live' || draftMode === 'multitest' || draftMode === 'mobile' || draftMode === 'host') {
      console.log(`🟢 Starting Polling for ${draftMode} mode (${roomSeason}) (Every 2s)...`);
      const poll = async () => {
        try {
          if (roomSeason === 2027) {
            const { data, error } = await supabase
              .from('draft_picks')
              .select('*')
              .eq('season_year', 2027)
              .order('overall_pick', { ascending: true });
            if (error) throw error;
            if (data) {
              setPicks(curr => {
                const liveMap = new Map(data.map(lp => [lp.overall_pick, lp]));
                const updated = curr.map(p => {
                  const lp = liveMap.get(p['Overall Pick']);
                  if (lp && !p['ESPN PlayerID']) {
                    return { ...p, 'ESPN PlayerID': lp.player_id, Selection: lp.player_name };
                  }
                  return p;
                });
                const currCount = curr.filter(p => p['ESPN PlayerID']).length;
                const newCount = updated.filter(p => p['ESPN PlayerID']).length;
                if (newCount > currCount) {
                  const latestNewPick = updated.filter(p => p['ESPN PlayerID']).slice(-1)[0];
                  if (latestNewPick) handleNewPickRef.current(latestNewPick);
                  return updated;
                }
                return curr;
              });
            }
          } else {
            const { data, error } = await supabase
              .from('draft-order')
              .select('*')
              .order('Overall Pick', { ascending: true });
            if (error) throw error;
            if (data && data.length > 0) {
              setPicks(curr => {
                const currPicked = curr.filter(p => p['ESPN PlayerID']).length;
                const newPicked = data.filter(p => p['ESPN PlayerID']).length;
                if (currPicked !== newPicked) {
                  const latestNewPick = data.filter(p => p['ESPN PlayerID']).slice(-1)[0];
                  if (latestNewPick) {
                    handleNewPickRef.current(latestNewPick);
                  }
                  return data;
                }
                return curr;
              });
            }
          }
        } catch (err) {
          console.error(`🔴 Polling Error:`, err.message);
        }
      };
      poll();
      const interval = setInterval(poll, 2000);
      return () => {
        console.log(`🛑 Stopping Polling`);
        clearInterval(interval);
      };
    } else if (draftMode === 'test' || draftMode === 'mockdraft') {
      fetchDraftOrderRef.current();
    }
  }, [draftMode, roomSeason]);

  useEffect(() => {
    if (picks.length > 0) {
      const liveCutoff = roomSeason === 2027 ? 55 : 46;
      if (draftMode === 'test' && testModePicks.length === 0) {
        setTestModePicks(picks.map(p => p['Overall Pick'] >= liveCutoff ? {
          ...p,
          'ESPN PlayerID': null,
          Selection: null
        } : { ...p }));
      } else if (draftMode === 'mockdraft' && testModePicks.length === 0) {
        setTestModePicks(picks.map(p => p['Overall Pick'] >= liveCutoff ? {
          ...p,
          'ESPN PlayerID': null,
          Selection: null
        } : { ...p }));
      }
    }
  }, [draftMode, picks, testModePicks.length, roomSeason]);

  const handleModeSelect = (mode) => {
    localStorage.setItem('draftMode', mode);
    setDraftMode(mode);
    const liveCutoff = roomSeason === 2027 ? 55 : 46;
    if ((mode === 'test' || mode === 'mockdraft') && testModePicks.length === 0) {
      setTestModePicks(picks.map(p => p['Overall Pick'] >= liveCutoff ? {
        ...p,
        'ESPN PlayerID': null,
        Selection: null
      } : { ...p }));
    }
  };

  const handleLogin = (user) => {
    localStorage.setItem('draftUser', user);
    setCurrentUser(user);
  };

  const handleResetMode = () => {
    localStorage.removeItem('draftMode');
    setDraftMode(null);
  };

  const handleResetUser = () => {
    localStorage.removeItem('draftUser');
    setCurrentUser(null);
  };

  const {
    isRunningMock,
    setIsRunningMock,
    mockSpeed,
    setMockSpeed,
    stepMockPick
  } = useMockDraftEngine({
    draftMode,
    currentPick,
    currentUser,
    displayPicks,
    playersRef,
    roomSeason,
    setPickStartTime,
    setTestModePicks,
    handleNewPick
  });

  const handleResetDraft = async () => {
    const liveCutoff = roomSeason === 2027 ? 55 : 46;
    if (!window.confirm(`Are you sure you want to reset all ${roomSeason} draft picks? This will clear selections from pick ${liveCutoff} onwards.`)) return;
    setResetting(true);
    try {
      if (roomSeason === 2027) {
        const { error } = await supabase
          .from('draft_picks')
          .delete()
          .eq('season_year', 2027)
          .gte('overall_pick', liveCutoff);
        if (error) throw error;
      } else {
        const { error } = await supabase
          .from('draft-order')
          .update({ 'ESPN PlayerID': null, 'Selection': null })
          .gte('Overall Pick', liveCutoff);
        if (error) throw error;
      }
      setTestModePicks(prev => prev.map(p => p['Overall Pick'] >= liveCutoff ? { ...p, 'ESPN PlayerID': null, Selection: null } : p));
      setPicks(prev => prev.map(p => p['Overall Pick'] >= liveCutoff ? { ...p, 'ESPN PlayerID': null, Selection: null } : p));
      setLastPickCommentary(roomSeason === 2026 ? "Viewing 2026 completed draft archive." : "Draft has not started.");
      setAnalysisHistory([]);
      setLastPodcastScript(null);
      setIsPlayingPodcast(false);
      setActivePodcastLine(null);
      stopPodcastAudio();
      alert("Draft reset successfully!");
    } catch (e) {
      console.error("Reset error:", e);
      alert("Reset error: " + e.message);
    } finally {
      setResetting(false);
    }
  };

  const lastPick = displayPicks.filter(p => p['ESPN PlayerID']).slice(-1)[0];
  const lastPickPlayer = lastPick ? displayPlayers.find(p => String(p['ESPN PlayerID']) === String(lastPick['ESPN PlayerID'])) : null;
  
  const isMyTurn = (currentPick && currentUser && currentPick.Owner === currentUser) || draftMode === 'test' || draftMode === 'multitest';

  // 1. Live Draft Roto Standings (Keepers + Draft Picks)
  const liveDraftRoto = useMemo(() => {
    return computeLiveDraftRoto({
      allPicks: displayPicks,
      keepers: _keepers,
      seasonYear: roomSeason
    });
  }, [displayPicks, _keepers, roomSeason]);

  // 2. Positional Run Radar
  const activePositionalRun = useMemo(() => {
    const drafted = displayPicks.filter(p => p['ESPN PlayerID']);
    return detectPositionalRun(drafted, 4);
  }, [displayPicks]);

  // 3. Current On-the-clock Owner Roto Profile
  const currentOwnerRoto = useMemo(() => {
    if (!currentPick?.Owner) return null;
    return liveDraftRoto.byOwner[currentPick.Owner] || liveDraftRoto.byOwner[normalizeManager(currentPick.Owner)] || null;
  }, [liveDraftRoto, currentPick]);

  // 4. Smart Draft Assistant Recommendations
  const smartRecommendations = useMemo(() => {
    const activePicks = (draftMode === 'test' || draftMode === 'mockdraft') && testModePicks?.length > 0
      ? testModePicks
      : (displayPicks || []);
    const takenIds = new Set(activePicks.map(p => String(p['ESPN PlayerID'])).filter(id => id && id !== 'null' && id !== 'undefined'));
    const available = (displayPlayers || []).filter(p => {
      const pid = String(p['ESPN PlayerID']);
      if (takenIds.has(pid)) return false;
      if (p.isKeeper) return false;
      return p.Availability === 'Available' || !p.Availability;
    });

    const targetOwner = (isMyTurn && currentUser) ? currentUser : (currentPick?.Owner || currentUser);
    const targetOwnerNormalized = normalizeManager(targetOwner);

    const ownerRoster = activePicks
      .filter(p => (p.Owner === targetOwner || normalizeManager(p.Owner) === targetOwnerNormalized) && p['ESPN PlayerID'])
      .map(p => displayPlayers.find(pl => String(pl['ESPN PlayerID']) === String(p['ESPN PlayerID'])))
      .filter(Boolean);

    const ownerDeficits = liveDraftRoto.byOwner[targetOwner]?.deficits ||
      liveDraftRoto.byOwner[targetOwnerNormalized]?.deficits || [];

    return getSmartDraftRecommendations({
      availablePlayers: available,
      myRoster: ownerRoster,
      userDeficits: ownerDeficits,
      currentPickNumber: currentPick?.['Overall Pick'] || 1
    });
  }, [displayPlayers, displayPicks, testModePicks, draftMode, isMyTurn, currentUser, currentPick, liveDraftRoto]);

  const upcomingPicks = useMemo(() => {
    const liveCutoff = roomSeason === 2027 ? 55 : 46;
    const currentIdx = displayPicks.findIndex(p => 
      roomSeason === 2027
        ? (p['Overall Pick'] >= liveCutoff && !p['ESPN PlayerID'])
        : !p['ESPN PlayerID']
    );
    if (currentIdx === -1) return [];
    return displayPicks.slice(currentIdx + 1, currentIdx + 21);
  }, [displayPicks, roomSeason]);

  const handleDraft = async (player) => {
    if (!isMyTurn && draftMode !== 'test' && draftMode !== 'mockdraft') return alert("Not your turn!");
    if (!window.confirm(`Draft ${player.Player}${draftMode === 'test' || draftMode === 'mockdraft' ? ' (Test Mode)' : ''}?`)) return;

    const newQueue = queue.filter(p => String(p['ESPN PlayerID']) !== String(player['ESPN PlayerID']));
    setQueue(newQueue);
    localStorage.setItem('draft_queue', JSON.stringify(newQueue));

    if (draftMode === 'test' || draftMode === 'mockdraft') {
      setPickStartTime(Date.now());
      const basePicks = testModePicks.length > 0 ? testModePicks : picks;
      
      setTestModePicks(basePicks.map(p => 
        p['Overall Pick'] === currentPick['Overall Pick'] 
          ? { ...p, 'ESPN PlayerID': player['ESPN PlayerID'], 'Selection': player.Player }
          : p
      ));
      
      setPlayers(curr => curr.map(p => 
        String(p['ESPN PlayerID']) === String(player['ESPN PlayerID'])
          ? { ...p, Availability: currentPick.Owner }
          : p
      ));
      
      await handleNewPick({ ...currentPick, 'ESPN PlayerID': player['ESPN PlayerID'], Round: currentPick.Round, Selection: player.Player });
    } else {
      if (roomSeason === 2027) {
        const { error } = await supabase
          .from('draft_picks')
          .upsert({
            season_year: 2027,
            round: currentPick.Round,
            pick: currentPick.Pick,
            overall_pick: currentPick['Overall Pick'],
            team_owner: currentPick.Owner,
            player_id: player['ESPN PlayerID'],
            player_name: player.Player,
            player_position: player.Position,
            player_team: player.Team,
            is_keeper: false,
            picked_at: new Date().toISOString()
          }, { onConflict: 'season_year,overall_pick' });
          
        if (error) {
          alert('Database Update Error: ' + error.message);
          return;
        }
        fetchDraftOrder();
        handleNewPick({
          ...currentPick,
          'ESPN PlayerID': player['ESPN PlayerID'],
          Round: currentPick.Round,
          Selection: player.Player
        });
      } else {
        const { error } = await supabase
          .from('draft-order')
          .update({ 
            'ESPN PlayerID': player['ESPN PlayerID'],
            'Selection': player.Player 
          })
          .eq('Overall Pick', currentPick['Overall Pick']);
          
        if (error) {
          alert('Database Update Error: ' + error.message);
          return;
        }
        
        fetchDraftOrder();
        handleNewPick({
          ...currentPick,
          'ESPN PlayerID': player['ESPN PlayerID'],
          Round: currentPick.Round,
          Selection: player.Player
        });
      }
    }
    
    setShowDashboard(false);
  };

  const addToQueue = (player) => {
    const newQueue = [...queue, player];
    setQueue(newQueue);
    localStorage.setItem('draft_queue', JSON.stringify(newQueue));
  };

  const removeFromQueue = (playerId) => {
    const newQueue = queue.filter(p => String(p['ESPN PlayerID']) !== String(playerId));
    setQueue(newQueue);
    localStorage.setItem('draft_queue', JSON.stringify(newQueue));
  };

  // --- HISTORICAL DRAFT SNAPSHOT (2026 ARCHIVE) ---
  if (roomSeason === 2026) {
    return (
      <>
        <HistoricalDraftView
          allPicks={displayPicks}
          players={displayPlayers}
          keepers={_keepers}
          compPicks={_compPicks}
          draftTrades={draftTrades}
          onSeasonChange={handleSeasonChange}
          onOpenPlayerModal={(id, name) => {
            const found = displayPlayers.find(p => String(p['ESPN PlayerID']) === String(id));
            if (found) setSelectedPlayer(found);
            else if (onOpenPlayerModal) onOpenPlayerModal(id, name);
          }}
          onSwitchView={onSwitchView}
          analysisHistory={analysisHistory}
        />
        {selectedPlayer && (
          <PlayerModal
            player={selectedPlayer}
            onClose={() => setSelectedPlayer(null)}
            onOpenInDepthModal={onOpenPlayerModal}
            players={displayPlayers}
            draftHistory={draftHistory}
            historicalFinish={historicalFinish}
            savantBatters={savantBatters}
            savantPitchers={savantPitchers}
            battersZips={battersZips}
            pitchersZips={pitchersZips}
          />
        )}
      </>
    );
  }

  if (!draftMode) {
    return (
      <ModeSelectionModal 
        onSelectMode={handleModeSelect} 
        roomSeason={roomSeason} 
        onSeasonChange={handleSeasonChange} 
      />
    );
  }

  if (!currentUser) {
    return <LoginModal owners={DRAFT_OWNERS} onLogin={handleLogin} roomSeason={roomSeason} />;
  }

  const myPicks = displayPicks.filter(p => p.Owner === currentUser && p['ESPN PlayerID']);
  const recentPicks = displayPicks.filter(p => p['ESPN PlayerID']);

  if (draftMode === 'host') {
    return (
      <HostBroadcastView
        roomSeason={roomSeason}
        pickStartTime={pickStartTime}
        currentPick={currentPick}
        upcomingPicks={upcomingPicks}
        lastPick={lastPick}
        lastPickPlayer={lastPickPlayer}
        lastPickCommentary={lastPickCommentary}
        generatingCommentary={generatingCommentary}
        playPodcastTTS={playPodcastTTS}
        lastPodcastScript={lastPodcastScript}
        generatingPodcast={generatingPodcast}
        isPlayingPodcast={isPlayingPodcast}
        activePodcastLine={activePodcastLine}
        handlePlayPodcast={handlePlayPodcast}
        handleStopPodcast={handleStopPodcast}
        recentPicks={recentPicks}
        displayPlayers={displayPlayers}
        selectedPlayer={selectedPlayer}
        setSelectedPlayer={setSelectedPlayer}
        playerInfo={playerInfo}
        handleResetMode={handleResetMode}
        onSwitchView={onSwitchView}
        onOpenPlayerModal={onOpenPlayerModal}
        draftHistory={draftHistory}
        historicalFinish={historicalFinish}
        savantBatters={savantBatters}
        savantPitchers={savantPitchers}
        battersZips={battersZips}
        pitchersZips={pitchersZips}
      />
    );
  }

  if (draftMode === 'mobile') {
    return (
      <MobileDraftView
        roomSeason={roomSeason}
        isMyTurn={isMyTurn}
        currentPick={currentPick}
        pickStartTime={pickStartTime}
        handleResetMode={handleResetMode}
        onSwitchView={onSwitchView}
        activePositionalRun={activePositionalRun}
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        displayPlayers={displayPlayers}
        handleDraft={handleDraft}
        queue={queue}
        addToQueue={addToQueue}
        removeFromQueue={removeFromQueue}
        draftMode={draftMode}
        testModePicks={testModePicks}
        displayPicks={displayPicks}
        selectedPlayer={selectedPlayer}
        setSelectedPlayer={setSelectedPlayer}
        playerInfo={playerInfo}
        currentUser={currentUser}
        smartRecommendations={smartRecommendations}
        recentPicks={recentPicks}
        onOpenPlayerModal={onOpenPlayerModal}
        draftHistory={draftHistory}
        historicalFinish={historicalFinish}
        savantBatters={savantBatters}
        savantPitchers={savantPitchers}
        battersZips={battersZips}
        pitchersZips={pitchersZips}
      />
    );
  }

  return (
    <>
      <style>{`
        @keyframes scroll {
          0% { transform: translate3d(0, 0, 0); }
          100% { transform: translate3d(-100%, 0, 0); }
        }
        @keyframes pulse {
          0% { opacity: 1; }
          50% { opacity: 0.5; }
          100% { opacity: 1; }
        }
        :root {
          --bg-dark: #121212;
          --bg-card: #1e1e1e;
          --accent: #bb86fc;
          --text-main: #e0e0e0;
          --highlight: #03dac6;
          --alert: #cf6679;
        }
      `}</style>
      
      <div style={styles.body}>
        {/* Header */}
        <DesktopHeader
          roomSeason={roomSeason}
          handleSeasonChange={handleSeasonChange}
          draftMode={draftMode}
          handleResetDraft={handleResetDraft}
          resetting={resetting}
          handleResetMode={handleResetMode}
          handleResetUser={handleResetUser}
          currentUser={currentUser}
          currentPick={currentPick}
          audioEnabled={audioEnabled}
          setAudioEnabled={setAudioEnabled}
          onTestAudio={() => playPodcastTTS(`Audio test for ${currentUser}. Live speech system is active.`)}
          pickStartTime={pickStartTime}
          onSwitchView={onSwitchView}
        />

        {/* War Room Drawer Toggle */}
        <button 
          style={styles.warRoomToggle}
          onClick={() => setShowDashboard(!showDashboard)}
        >
          {showDashboard ? '✕ CLOSE DRAWER' : '📋 OPEN WAR ROOM DASHBOARD'}
        </button>

        {/* Main Stage */}
        <div style={styles.mainStage}>
          {/* Positional Run Radar Alert */}
          <PositionalRunRadar run={activePositionalRun} />

          {/* On-The-Clock Category Deficit HUD */}
          <CategoryDeficitHUD
            currentPick={currentPick}
            ownerRoto={currentOwnerRoto}
            isMyTurn={isMyTurn}
            onSelectTab={(tab) => {
              setShowDashboard(true);
              setActiveTab(tab);
            }}
          />

          <div style={styles.pickCard}>
            <div style={styles.pickMeta}>
              {lastPick ? `Round ${lastPick.Round} • Pick #${lastPick['Overall Pick']}` : 'Waiting for first pick...'}
            </div>
            <div style={styles.pickPlayer}>
              {lastPickPlayer ? (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '20px', flexWrap: 'wrap' }}>
                  <div style={{
                    width: '84px',
                    height: '84px',
                    borderRadius: '50%',
                    border: '3px solid #bb86fc',
                    backgroundColor: '#222',
                    overflow: 'hidden',
                    flexShrink: 0,
                    boxShadow: '0 4px 18px rgba(187, 134, 252, 0.4)'
                  }}>
                    <img
                      src={getPlayerHeadshotUrl(lastPickPlayer)}
                      alt={lastPickPlayer.Player}
                      style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                      onError={(e) => handleHeadshotError(e, lastPickPlayer)}
                    />
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center' }}>
                    <PlayerNameButton 
                      player={lastPickPlayer} 
                      onClick={setSelectedPlayer}
                      style={{ color: '#fff', fontWeight: '700', fontSize: '52px' }}
                    />
                    {getInjuryIndicator(lastPickPlayer['ESPN PlayerID'], playerInfo) && (
                      <span style={{
                        marginLeft: '15px',
                        padding: '4px 10px',
                        borderRadius: '6px',
                        background: getInjuryIndicator(lastPickPlayer['ESPN PlayerID'], playerInfo).color,
                        color: '#000',
                        fontSize: '18px',
                        fontWeight: 'bold',
                        verticalAlign: 'middle'
                      }}>
                        {getInjuryIndicator(lastPickPlayer['ESPN PlayerID'], playerInfo).status}
                      </span>
                    )}
                  </div>
                </div>
              ) : (
                'WAITING ON CLOCK...'
              )}
            </div>
            <div style={styles.pickOwner}>
              {lastPick ? `Selected by ${lastPick.Owner}` : '--'}
            </div>
            <div style={styles.aiBox}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px', flexWrap: 'wrap', gap: '6px' }}>
                <span style={{ fontSize: '11px', color: '#bb86fc', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  🎙️ Instant Reaction & Grade
                </span>
                <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                  {lastPodcastScript && lastPodcastScript.length > 0 && (
                    <button
                      onClick={() => isPlayingPodcast ? handleStopPodcast() : handlePlayPodcast()}
                      style={{
                        background: isPlayingPodcast ? 'rgba(239, 68, 68, 0.2)' : 'rgba(3, 218, 198, 0.15)',
                        border: `1px solid ${isPlayingPodcast ? '#ef4444' : '#03dac6'}`,
                        color: isPlayingPodcast ? '#ef4444' : '#03dac6',
                        borderRadius: '4px',
                        padding: '2px 7px',
                        fontSize: '10px',
                        fontWeight: 'bold',
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '3px'
                      }}
                      title={isPlayingPodcast ? "Stop podcast debate" : "Play Skip & Chip podcast debate audio"}
                    >
                      {isPlayingPodcast ? '⏹️ Stop Debate' : '🎙️ Play Debate'}
                    </button>
                  )}
                  {lastPickCommentary && lastPickCommentary !== 'Draft has not started.' && !generatingCommentary && (
                    <button
                      onClick={() => playPodcastTTS(lastPickCommentary)}
                      style={{
                        background: 'rgba(187, 134, 252, 0.15)',
                        border: '1px solid #bb86fc',
                        color: '#bb86fc',
                        borderRadius: '4px',
                        padding: '2px 7px',
                        fontSize: '10px',
                        fontWeight: 'bold',
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '3px'
                      }}
                      title="Play podcast-style spoken analysis"
                    >
                      🔊 Listen
                    </button>
                  )}
                </div>
              </div>
              {generatingCommentary ? (
                <span>🤔 Gemini is analyzing this selection...</span>
              ) : (
                <span dangerouslySetInnerHTML={{ __html: lastPickCommentary }} />
              )}
            </div>
          </div>
        </div>

        {/* On Deck Sidebar */}
        <OnDeckSidebar upcomingPicks={upcomingPicks} currentUser={currentUser} />

        {/* Bottom Info Bar */}
        <div style={styles.infoBar}>
          <TeamNeedsSummary myPicks={myPicks} players={displayPlayers} />
          <RecentActivityWidget 
            recentPicks={recentPicks} 
            players={displayPlayers} 
            onPlayerClick={setSelectedPlayer}
            playerInfo={playerInfo}
          />
          <QueuePreviewWidget 
            queue={queue} 
            onPlayerClick={setSelectedPlayer} 
            playerInfo={playerInfo} 
          />
        </div>

        {/* Scrolling Ticker */}
        <Ticker recentPicks={recentPicks} players={displayPlayers} />

        {/* War Room Drawer / Panel */}
        <WarRoomDrawer
          isOpen={showDashboard}
          activeTab={activeTab}
          setActiveTab={setActiveTab}
          currentUser={currentUser}
          displayPlayers={displayPlayers}
          handleDraft={handleDraft}
          isMyTurn={isMyTurn}
          queue={queue}
          addToQueue={addToQueue}
          removeFromQueue={removeFromQueue}
          draftMode={draftMode}
          testModePicks={testModePicks}
          displayPicks={displayPicks}
          setSelectedPlayer={setSelectedPlayer}
          playerInfo={playerInfo}
          smartRecommendations={smartRecommendations}
          draftTrades={draftTrades}
          roomSeason={roomSeason}
          analysisHistory={analysisHistory}
          keepers={_keepers}
        />

        {/* Mock Draft Floating Controller */}
        {draftMode === 'mockdraft' && (
          <MockDraftController
            isRunningMock={isRunningMock}
            setIsRunningMock={setIsRunningMock}
            stepMockPick={stepMockPick}
            currentPick={currentPick}
            currentUser={currentUser}
            mockSpeed={mockSpeed}
            setMockSpeed={setMockSpeed}
          />
        )}
      </div>
      
      {/* Player Detail Modal */}
      {selectedPlayer && (
        <PlayerModal 
          player={selectedPlayer} 
          onClose={() => setSelectedPlayer(null)}
          onOpenInDepthModal={onOpenPlayerModal}
          players={displayPlayers}
          draftHistory={draftHistory}
          historicalFinish={historicalFinish}
          savantBatters={savantBatters}
          savantPitchers={savantPitchers}
          battersZips={battersZips}
          pitchersZips={pitchersZips}
        />
      )}
    </>
  );
}
