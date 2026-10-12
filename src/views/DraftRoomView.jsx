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
  OWNER_ARCHETYPES,
  normalizeManager,
  computeLiveDraftRoto,
  detectPositionalRun,
  getSmartDraftRecommendations,
  getPlayerProjectionStats
} from '../utils/draftRotoEngine';
import styles from './draft/styles/draftRoomStyles';
import {
  mappedDraft2026History,
  normalizeName,
  getEffectiveQS,
  enrichPlayerWithCalculations,
  mergeEspnData,
  mergeZipsData,
  DRAFT_OWNERS,
  OWNER_AVATARS,
  generateDefaultDraftOrder,
  getInjuryIndicator,
  generate2027DraftOrder
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
  stopPodcastAudio,
  generateLocalPodcastDialogue,
  generatePodcastDialogue,
  playDualSpeakerPodcast,
  playPodcastTTS,
  playOwnerSound
} from './draft/services/draftAudio';

export { DRAFT_OWNERS };

// --- SUB-COMPONENTS (PHASE 2 MODULARIZATION) ---
import PodcastBreakdownWidget from './draft/components/PodcastBreakdownWidget';
import PlayerModal from './draft/components/PlayerModal';
import PlayerNameButton from './draft/components/PlayerNameButton';
import ModeSelectionModal from './draft/components/ModeSelectionModal';
import LoginModal from './draft/components/LoginModal';
import {
  CountdownTimer,
  Ticker,
  OnDeckSidebar,
  TeamNeedsSummary,
  RecentActivityWidget,
  QueuePreviewWidget,
  PositionalRunRadar,
  CategoryDeficitHUD,
  DraftAssistantWidget
} from './draft/components/DraftWidgets';
import PlayerPoolPanel from './draft/components/PlayerPoolPanel';
import RosterManagerPanel from './draft/components/RosterManagerPanel';
import MyPicksPanel from './draft/components/MyPicksPanel';
import DraftCapitalPanel from './draft/components/DraftCapitalPanel';
import AnalysisHistoryPanel from './draft/components/AnalysisHistoryPanel';
import DraftLogPanel from './draft/components/DraftLogPanel';
import StandingsPanel from './draft/components/StandingsPanel';

const DEFAULT_OWNER_PROFILES = OWNER_ARCHETYPES;

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
  const lastAnnouncedPickRef = useRef(null);

  const [isRunningMock, setIsRunningMock] = useState(false);
  const [mockSpeed, setMockSpeed] = useState(1000);
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

  const currentPickId = currentPick?.['Overall Pick'];
  const currentPickOwner = currentPick?.Owner;

  // Sound effect when current pick changes
  useEffect(() => {
    if (currentPickOwner && audioEnabled && currentPickId) {
      if (currentPickId !== lastAnnouncedPickRef.current) {
        lastAnnouncedPickRef.current = currentPickId;
        playOwnerSound(currentPickOwner);
      }
    }
  }, [currentPickId, currentPickOwner, audioEnabled]);

  // Commentary callback - stable reference using playersRef
  const handleNewPick = useCallback(async (pick) => {
    const player = playersRef.current.find(p => String(p['ESPN PlayerID']) === String(pick['ESPN PlayerID']));
    if (!player) return;

    // If pick already comes with commentary, display immediately without regenerating
    if (pick.ai_commentary) {
      setLastPickCommentary(pick.ai_commentary);
      const script = pick.podcast_script || generateLocalPodcastDialogue(player, pick.Owner, pick['Overall Pick'], {}, roomSeason);
      setLastPodcastScript(script);
      const historyEntry = {
        pickNumber: pick['Overall Pick'],
        round: pick.Round,
        owner: pick.Owner,
        playerName: player.Player,
        position: player.Position,
        team: player.Team,
        commentary: pick.ai_commentary,
        podcastScript: script,
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

    const currentPicks = picksRef.current || [];
    const currentPlayers = playersRef.current || [];

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
        roomSeason
      ),
      generatePodcastDialogue(
        player,
        pick.Owner,
        pick['Overall Pick'],
        teamContext,
        roomSeason
      )
    ]);
    
    setLastPickCommentary(commentary);
    setGeneratingCommentary(false);
    setLastPodcastScript(podcastDialogue);
    setGeneratingPodcast(false);

    // Skip & Chip dual-voice podcast analysis plays if audio is enabled
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
      }, 700);
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
          .update({ ai_commentary: commentary })
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
              .filter(lp => lp.ai_commentary)
              .map(lp => ({
                pickNumber: lp.overall_pick,
                round: lp.round,
                owner: lp.team_owner,
                playerName: lp.player_name,
                position: lp.player_position || '',
                team: lp.player_team || '',
                commentary: lp.ai_commentary,
                timestamp: lp.picked_at ? new Date(lp.picked_at).toLocaleTimeString() : ''
              }));
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

  const stepMockPick = useCallback(() => {
    const current = currentPick;
    if (!current || current.Owner === currentUser) {
      setIsRunningMock(false);
      return;
    }

    const takenIds = new Set(displayPicks.map(p => String(p['ESPN PlayerID'])).filter(Boolean));
    const available = playersRef.current.filter(p => !takenIds.has(String(p['ESPN PlayerID'])));
    if (!available.length) return;

    const ownerRoster = displayPicks
      .filter(p => p.Owner === current.Owner && p['ESPN PlayerID'])
      .map(p => playersRef.current.find(pl => String(pl['ESPN PlayerID']) === String(p['ESPN PlayerID'])))
      .filter(Boolean);

    const pickNum = current['Overall Pick'];
    const windowOffset = pickNum <= 65 ? 20 : pickNum <= 120 ? 25 : pickNum <= 180 ? 35 : 60;

    const candidates = available.filter(p => {
      const adp = parseFloat(p.ADP);
      return isNaN(adp) || adp >= 900 ? pickNum > 150 : adp <= pickNum + windowOffset;
    });

    const poolToScore = candidates.length > 0 ? candidates : available.slice(0, 50);

    const slotLimits = { C: 1, '1B': 1, '2B': 1, '3B': 1, SS: 1, OF: 6, DH: 1, SP: 4, RP: 2 };
    const currentCounts = {};
    ownerRoster.forEach(rp => {
      Object.keys(slotLimits).forEach(slot => {
        if ((rp.Position || '').includes(slot)) {
          currentCounts[slot] = (currentCounts[slot] || 0) + 1;
        }
      });
    });

    const scored = poolToScore.map(player => {
      const pos = player.Position || '';
      const isSP = pos.includes('SP');
      const isRP = pos.includes('RP');
      const isP = isSP || isRP;

      const pr = parseFloat(player['Projected PR']) || 0;
      const valScore = Math.max(0, Math.min(100, (pr + 3) / 6 * 100));

      let needScore = 40;
      Object.entries(slotLimits).forEach(([slot, limit]) => {
        if (!pos.includes(slot)) return;
        const count = currentCounts[slot] || 0;
        const ratio = count / limit;
        const s = ratio === 0 ? 100 : ratio < 0.5 ? 85 : ratio < 1 ? 65 : ratio < 1.5 ? 35 : 10;
        needScore = Math.max(needScore, s);
      });

      const profile = OWNER_ARCHETYPES[current.Owner] || OWNER_ARCHETYPES[normalizeManager(current.Owner)] || DEFAULT_OWNER_PROFILES[current.Owner];
      let ownerScore = 50;
      if (profile) {
        const pitcherBias = profile.pitcherBias ?? 0.35;
        if (isP) {
          ownerScore = pitcherBias > 0.45 ? 76 : pitcherBias > 0.35 ? 64 : pitcherBias < 0.25 ? 30 : 44;
        } else {
          const hitBias = 1 - pitcherBias;
          ownerScore = hitBias > 0.75 ? 76 : hitBias > 0.6 ? 64 : hitBias < 0.5 ? 40 : 50;
        }

        // Positional bias
        if (profile.boostPositions && profile.boostPositions.some(bp => pos.includes(bp))) {
          ownerScore += 16;
        }
        if (profile.fadePositions && profile.fadePositions.some(fp => pos.includes(fp))) {
          ownerScore -= 18;
        }

        // Target stats bias
        const st = getPlayerProjectionStats(player, roomSeason);
        if (profile.targetStats) {
          if (profile.targetStats.includes('HR') && (parseFloat(st.HR) || 0) >= 28) ownerScore += 12;
          if (profile.targetStats.includes('SB') && (parseFloat(st.SB) || 0) >= 18) ownerScore += 14;
          if (profile.targetStats.includes('K') && (parseFloat(st.SO || st.K) || 0) >= 150) ownerScore += 14;
          if (profile.targetStats.includes('QS') && (parseFloat(st.QS) || 0) >= 12) ownerScore += 12;
          if (profile.targetStats.includes('SVHD') && (isRP || (parseFloat(st.SVHD || st.SV_HD) || 0) >= 15)) ownerScore += 16;
          if (profile.targetStats.includes('OBP') && (parseFloat(st.OBP) || 0) >= 0.360) ownerScore += 12;
          if (profile.targetStats.includes('ERA') && (parseFloat(st.ERA) || 9) <= 3.35 && (parseFloat(st.IP) || 0) >= 60) ownerScore += 12;
          if (profile.targetStats.includes('WHIP') && (parseFloat(st.WHIP) || 9) <= 1.12 && (parseFloat(st.IP) || 0) >= 60) ownerScore += 12;
        }

        // ADP Value hunter (Garrett / Adrian / etc.)
        const adp = parseFloat(player.ADP) || 999;
        if (adp < pickNum - 10) {
          ownerScore += 14;
        }
      }

      const totalScore = (valScore * 0.4 + needScore * 0.35 + ownerScore * 0.25) * (0.92 + Math.random() * 0.16);
      return { player, score: totalScore };
    }).sort((a, b) => b.score - a.score);

    const chosen = scored[0]?.player;
    if (!chosen) return;

    setPickStartTime(Date.now());
    setTestModePicks(prev => {
      const base = prev.length > 0 ? prev : displayPicks;
      return base.map(p => 
        p['Overall Pick'] === current['Overall Pick'] 
          ? { ...p, 'ESPN PlayerID': chosen['ESPN PlayerID'], Selection: chosen.Player }
          : p
      );
    });

    handleNewPick({
      ...current,
      'ESPN PlayerID': chosen['ESPN PlayerID'],
      Round: current.Round
    });
  }, [currentPick, displayPicks, currentUser, handleNewPick, roomSeason]);

  useEffect(() => {
    if (draftMode !== 'mockdraft' || !isRunningMock) return;

    if (!currentPick || currentPick.Owner === currentUser) {
      setIsRunningMock(false);
      return;
    }

    const timer = setTimeout(() => {
      stepMockPick();
    }, mockSpeed);

    return () => clearTimeout(timer);
  }, [draftMode, isRunningMock, mockSpeed, currentPick, currentUser, stepMockPick]);

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
      
      await handleNewPick({ ...currentPick, 'ESPN PlayerID': player['ESPN PlayerID'], Round: currentPick.Round });
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
    const elapsed = Math.floor((Date.now() - pickStartTime) / 1000);
    const secondsLeft = Math.max(0, 60 - elapsed);
    const isWarning = secondsLeft <= 30 && secondsLeft > 0;
    const isUrgent = secondsLeft <= 15 && secondsLeft > 0;
    const isExpired = secondsLeft === 0;

    const timerColor = isExpired ? '#f44336' : isUrgent ? '#ff5722' : isWarning ? '#ffc107' : '#03dac6';
    const timerBg = isExpired ? '#f44336' : isUrgent ? '#ff5722' : isWarning ? '#ffc107' : '#222';
    const timerTextColor = isExpired || isUrgent ? '#fff' : isWarning ? '#000' : '#03dac6';
    const timerClass = isExpired ? 'monospace timer-expired' : isUrgent ? 'monospace urgent-warning' : isWarning ? 'monospace timer-warning' : 'monospace';
    const headerBg = isExpired
      ? 'linear-gradient(180deg, #3d1a1a 0%, #2d1616 100%)'
      : isUrgent
      ? 'linear-gradient(180deg, #3d2a1a 0%, #2d1f16 100%)'
      : isWarning
      ? 'linear-gradient(180deg, #3d3a1a 0%, #2d2916 100%)'
      : 'linear-gradient(180deg, #1a1a2e 0%, #16213e 100%)';

    const isPitcher = lastPickPlayer ? (lastPickPlayer.Position || '').includes('SP') || (lastPickPlayer.Position || '').includes('RP') : false;

    return (
      <div className={`host-mode-container ${isExpired ? 'container-expired' : ''}`} style={{
        backgroundColor: '#000',
        color: '#e0e0e0',
        height: '100vh',
        overflow: 'hidden',
        display: 'grid',
        gridTemplateColumns: '60% 40%',
        gridTemplateRows: '90px 1fr 50px',
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
      }}>
        <style>{`
          .host-mode-container, .host-mode-container * {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
          }
          .host-mode-container .monospace {
            font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Fira Mono', monospace !important;
          }
          @keyframes timerWarning { 0%, 100% { transform: scale(1); } 50% { transform: scale(1.02); } }
          .timer-warning { animation: timerWarning 1s ease-in-out infinite !important; }
          @keyframes timerPulse { 0%, 100% { transform: scale(1); box-shadow: 0 0 20px rgba(244, 67, 54, 0.5); } 50% { transform: scale(1.05); box-shadow: 0 0 40px rgba(244, 67, 54, 0.8); } }
          @keyframes borderFlash { 0%, 100% { border-color: #f44336; } 50% { border-color: #ff8a80; } }
          @keyframes headerPulse { 0%, 100% { background: linear-gradient(180deg, #3d1a1a 0%, #2d1616 100%); } 50% { background: linear-gradient(180deg, #5d2a2a 0%, #3d1a1a 100%); } }
          @keyframes textFlash { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
          @keyframes urgentPulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.8; transform: scale(1.02); } }
          @keyframes redVignette { 0%, 100% { box-shadow: inset 0 0 150px 50px rgba(244, 67, 54, 0.3); } 50% { box-shadow: inset 0 0 200px 80px rgba(244, 67, 54, 0.5); } }
          @keyframes cardGlow { 0%, 100% { box-shadow: 0 0 30px rgba(244, 67, 54, 0.4), 0 0 60px rgba(244, 67, 54, 0.2); } 50% { box-shadow: 0 0 50px rgba(244, 67, 54, 0.6), 0 0 100px rgba(244, 67, 54, 0.3); } }
          @keyframes screenShake { 0%, 100% { transform: translateX(0); } 25% { transform: translateX(-2px); } 75% { transform: translateX(2px); } }
          .timer-expired { animation: timerPulse 0.8s ease-in-out infinite !important; }
          .header-expired { animation: headerPulse 1s ease-in-out infinite, borderFlash 0.5s ease-in-out infinite !important; }
          .times-up-flash { animation: textFlash 0.5s ease-in-out infinite !important; }
          .urgent-warning { animation: urgentPulse 0.5s ease-in-out infinite !important; }
          .container-expired { animation: screenShake 0.3s ease-in-out infinite !important; }
        `}</style>

        {/* Host Header */}
        <div className={isExpired ? 'header-expired' : ''} style={{
          gridColumn: '1 / -1',
          background: headerBg,
          borderBottom: `4px solid ${timerColor}`,
          display: 'flex',
          alignItems: 'center',
          padding: '0 25px',
          justifyContent: 'space-between',
          transition: 'all 0.3s ease'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
            <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#fff', letterSpacing: '2px' }}>
              ⚾ HEFTY WAR ROOM {roomSeason}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginLeft: '10px', paddingLeft: '15px', borderLeft: '2px solid #444', overflow: 'hidden' }}>
              {[currentPick, ...upcomingPicks].filter(Boolean).slice(0, 10).map((p, idx) => {
                const isCurrent = idx === 0;
                const avatar = OWNER_AVATARS[p.Owner] || OWNER_AVATARS.default;
                return (
                  <div key={p['Overall Pick']} style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    background: isCurrent ? 'rgba(3, 218, 198, 0.15)' : '#1a1a2e',
                    border: `1px solid ${isCurrent ? '#03dac6' : '#333'}`,
                    padding: '4px 10px',
                    borderRadius: '20px',
                    minWidth: '100px'
                  }}>
                    <div style={{ width: '24px', height: '24px', borderRadius: '50%', overflow: 'hidden', border: `2px solid ${isCurrent ? '#03dac6' : '#555'}` }}>
                      <img src={avatar} alt={p.Owner} style={{ width: '100%', height: '100%', objectFit: 'cover' }} onError={e => { e.target.style.display = 'none'; }} />
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column' }}>
                      <span style={{ fontSize: '9px', color: isCurrent ? '#03dac6' : '#888', fontWeight: 'bold' }}>#{p['Overall Pick']}</span>
                      <span style={{ fontSize: '11px', color: '#fff', fontWeight: 'bold', whiteSpace: 'nowrap' }}>{p.Owner}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
            {isUrgent && !isExpired && (
              <div className="urgent-warning" style={{ background: '#ff5722', color: '#fff', padding: '6px 14px', borderRadius: '8px', fontSize: '14px', fontWeight: 'bold' }}>
                ⚠️ HURRY!
              </div>
            )}
            {isExpired && (
              <div className="times-up-flash" style={{ background: '#f44336', color: '#fff', padding: '6px 14px', borderRadius: '8px', fontSize: '15px', fontWeight: 'bold' }}>
                ⏰ TIME'S UP!
              </div>
            )}
            <div className={timerClass} style={{
              fontSize: '34px',
              background: timerBg,
              padding: '4px 16px',
              borderRadius: '8px',
              border: `3px solid ${timerColor}`,
              color: timerTextColor,
              minWidth: '90px',
              textAlign: 'center',
              fontWeight: 900
            }}>
              {secondsLeft}s
            </div>
            <button
              onClick={handleResetMode}
              style={{ background: 'transparent', color: '#888', border: '1px solid #555', padding: '6px 10px', borderRadius: '6px', fontSize: '12px', cursor: 'pointer' }}
            >
              Exit Host
            </button>
            {onSwitchView && (
              <button
                onClick={() => onSwitchView('summary')}
                style={{ background: '#1e3a8a', color: '#fff', border: '1px solid #3b82f6', padding: '6px 12px', borderRadius: '6px', fontSize: '12px', cursor: 'pointer', fontWeight: 'bold' }}
              >
                📊 League Site
              </button>
            )}
          </div>
        </div>

        {/* 60% Left Stage */}
        <div style={{
          gridColumn: '1 / 2',
          gridRow: '2 / 3',
          display: 'flex',
          flexDirection: 'column',
          padding: '24px',
          borderRight: '1px solid #333',
          background: isExpired ? 'radial-gradient(ellipse at center, #2d1a1a 0%, #1a0000 100%)' : 'radial-gradient(ellipse at center, #1a1a2e 0%, #000 100%)',
          gap: '16px',
          overflowY: 'auto'
        }}>
          <div style={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            background: 'linear-gradient(135deg, #1e1e2f 0%, #252540 100%)',
            borderRadius: '16px',
            padding: '24px',
            border: '2px solid #333',
            position: 'relative',
            overflowY: 'auto'
          }}>
            <div style={{ fontSize: '16px', color: '#888', textTransform: 'uppercase', letterSpacing: '2px', textAlign: 'center', marginBottom: '16px' }}>
              {lastPick ? `Round ${lastPick.Round} • Pick ${lastPick['Overall Pick']}` : 'Waiting for first pick...'}
            </div>

            {lastPickPlayer ? (
              <div style={{ display: 'flex', width: '100%', alignItems: 'center', justifyContent: 'space-between', marginBottom: '20px' }}>
                <div style={{ width: '120px', height: '120px', borderRadius: '16px', border: '3px solid #bb86fc', overflow: 'hidden', background: '#111', flexShrink: 0, boxShadow: '0 4px 20px rgba(187, 134, 252, 0.4)' }}>
                  <img
                    src={getPlayerHeadshotUrl(lastPickPlayer)}
                    alt={lastPickPlayer.Player}
                    style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                    onError={(e) => handleHeadshotError(e, lastPickPlayer)}
                  />
                </div>
                <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '0 16px' }}>
                  <div style={{ fontSize: '38px', fontWeight: '900', lineHeight: '1.1', color: '#fff', textAlign: 'center', marginBottom: '10px' }}>
                    {lastPickPlayer.Player}
                  </div>
                  <div style={{ display: 'flex', gap: '10px', justifyContent: 'center', marginBottom: '14px' }}>
                    <span style={{ background: 'linear-gradient(135deg, #bb86fc 0%, #9c5dd9 100%)', padding: '4px 14px', borderRadius: '20px', fontSize: '14px', fontWeight: 'bold', color: '#fff' }}>
                      {lastPickPlayer.Position}
                    </span>
                    <span style={{ background: 'linear-gradient(135deg, #03dac6 0%, #00a896 100%)', padding: '4px 14px', borderRadius: '20px', fontSize: '14px', fontWeight: 'bold', color: '#000' }}>
                      {lastPickPlayer.Team}
                    </span>
                  </div>
                  <div style={{ display: 'flex', gap: '10px' }}>
                    {isPitcher ? (
                      <>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>ERA</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSERA || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>WHIP</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSWHIP || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>K</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSK || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>QS</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{getEffectiveQS(lastPickPlayer) || lastPickPlayer.ZIPSQS || '—'}</span>
                        </div>
                      </>
                    ) : (
                      <>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>HR</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSHR || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>RBI</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSRBI || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>SB</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSSB || '—'}</span>
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', background: 'rgba(0,0,0,0.4)', padding: '6px 14px', borderRadius: '6px', border: '1px solid #444', minWidth: '55px' }}>
                          <span style={{ fontSize: '10px', color: '#888' }}>OBP</span>
                          <span style={{ fontSize: '16px', color: '#fff', fontWeight: 'bold' }}>{lastPickPlayer.ZIPSOBP || '—'}</span>
                        </div>
                      </>
                    )}
                  </div>
                </div>
                <div style={{ width: '110px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px', flexShrink: 0 }}>
                  <div style={{ width: '90px', height: '90px', borderRadius: '50%', border: '3px solid #03dac6', overflow: 'hidden', background: '#111' }}>
                    <img
                      src={OWNER_AVATARS[lastPick?.Owner] || OWNER_AVATARS.default}
                      alt={lastPick?.Owner}
                      style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                      onError={e => { e.target.style.display = 'none'; }}
                    />
                  </div>
                  <div style={{ textAlign: 'center', background: 'rgba(0,0,0,0.5)', padding: '4px 10px', borderRadius: '8px', border: '1px solid #03dac644' }}>
                    <div style={{ fontSize: '9px', color: '#03dac6', fontWeight: 'bold', textTransform: 'uppercase' }}>Selected By</div>
                    <div style={{ fontSize: '14px', color: '#fff', fontWeight: 'bold' }}>{lastPick?.Owner}</div>
                  </div>
                </div>
              </div>
            ) : (
              <div style={{ fontSize: '32px', color: '#444', fontWeight: 'bold', textAlign: 'center', padding: '40px 0' }}>
                WAR ROOM READY
              </div>
            )}

            {/* AI Commentary in Host Mode */}
            <div style={{
              flex: 1,
              minHeight: '110px',
              background: 'rgba(0,0,0,0.3)',
              padding: '16px',
              borderRadius: '12px',
              borderLeft: '5px solid #bb86fc',
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden'
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                <span style={{ fontSize: '12px', color: '#bb86fc', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  🎙️ HeftyMatic 3000 Instant Reaction
                </span>
                {lastPickCommentary && lastPickCommentary !== 'Draft has not started.' && !generatingCommentary && (
                  <button
                    onClick={() => playPodcastTTS(lastPickCommentary)}
                    style={{
                      background: 'rgba(187, 134, 252, 0.15)',
                      border: '1px solid #bb86fc',
                      color: '#bb86fc',
                      borderRadius: '6px',
                      padding: '2px 8px',
                      fontSize: '11px',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '4px'
                    }}
                    title="Play podcast-style spoken analysis"
                  >
                    🔊 Listen
                  </button>
                )}
              </div>
              <div style={{ flex: 1, overflowY: 'auto', fontSize: '15px', color: '#ddd', lineHeight: '1.5' }}>
                {generatingCommentary ? (
                  <span style={{ color: '#888', fontStyle: 'italic' }}>🤔 HeftyMatic 3000 is analyzing this pick...</span>
                ) : (
                  <span dangerouslySetInnerHTML={{ __html: lastPickCommentary }} />
                )}
              </div>
            </div>

            {/* Skip & Chip 2-Person Podcast Debate Widget */}
            <PodcastBreakdownWidget
              script={lastPodcastScript}
              isGenerating={generatingPodcast}
              isPlaying={isPlayingPodcast}
              activeLine={activePodcastLine}
              onPlay={() => handlePlayPodcast()}
              onStop={handleStopPodcast}
              playerName={lastPickPlayer?.Player}
            />
          </div>
        </div>

        {/* 40% Right Stage */}
        <div style={{
          gridColumn: '2 / 3',
          gridRow: '2 / 3',
          background: '#111',
          padding: '20px',
          display: 'flex',
          flexDirection: 'column',
          gap: '15px',
          overflow: 'hidden'
        }}>
          <OnDeckSidebar upcomingPicks={upcomingPicks} currentUser={currentUser} />
          <div style={{ flex: 1, overflowY: 'auto' }}>
            <RecentActivityWidget
              recentPicks={recentPicks}
              players={displayPlayers}
              onPlayerClick={setSelectedPlayer}
              playerInfo={playerInfo}
            />
          </div>
        </div>

        {/* Bottom Ticker */}
        <div style={{ gridColumn: '1 / -1', gridRow: '3 / 4', borderTop: '2px solid #333' }}>
          <Ticker recentPicks={recentPicks} players={displayPlayers} />
        </div>

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
      </div>
    );
  }

  if (draftMode === 'mobile') {
    return (
      <div style={{
        ...styles.body,
        gridTemplateColumns: '1fr',
        gridTemplateRows: '50px 1fr 50px',
        height: '100vh'
      }}>
        {/* Mobile Header */}
        <div style={{
          gridColumn: '1 / -1',
          background: '#1f1f1f',
          borderBottom: '2px solid #bb86fc',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          padding: '0 15px',
          zIndex: 10
        }}>
          <div style={{ fontSize: '16px', fontWeight: 'bold', color: '#bb86fc' }}>
            HWR '{String(roomSeason).slice(-2)}
          </div>
          <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
            <div style={{
              fontSize: '12px',
              fontWeight: 'bold',
              color: isMyTurn ? '#03dac6' : '#888',
              animation: isMyTurn ? 'pulse 1s infinite' : 'none'
            }}>
              {currentPick ? `${currentPick.Owner} (#${currentPick['Overall Pick']})` : 'COMPLETE'}
            </div>
            <CountdownTimer pickStartTime={pickStartTime} />
            <button
              onClick={handleResetMode}
              style={{ background: 'transparent', color: '#888', border: '1px solid #444', padding: '3px 6px', borderRadius: '4px', fontSize: '10px', cursor: 'pointer' }}
            >
              Mode
            </button>
            {onSwitchView && (
              <button
                onClick={() => onSwitchView('summary')}
                style={{ background: '#1e3a8a', color: '#fff', border: '1px solid #3b82f6', padding: '3px 8px', borderRadius: '4px', fontSize: '10px', cursor: 'pointer', fontWeight: 'bold' }}
              >
                League
              </button>
            )}
          </div>
        </div>

        {/* Mobile Content Area */}
        <div style={{ gridColumn: '1 / -1', overflow: 'hidden', background: '#121212', padding: '10px' }}>
          {activePositionalRun && (
            <div style={{ marginBottom: '10px' }}>
              <PositionalRunRadar run={activePositionalRun} />
            </div>
          )}
          {activeTab === 'Pool' && (
            <PlayerPoolPanel
              players={displayPlayers}
              onDraft={handleDraft}
              isMyTurn={isMyTurn}
              queue={queue}
              onAddToQueue={addToQueue}
              onRemoveFromQueue={removeFromQueue}
              draftMode={draftMode}
              testModePicks={testModePicks}
              allPicks={displayPicks}
              onPlayerClick={setSelectedPlayer}
              playerInfo={playerInfo}
              isMobile={true}
              currentUser={currentUser}
              recommendations={smartRecommendations}
            />
          )}
          {activeTab === 'Queue' && (
            <div style={{ height: '100%', overflowY: 'auto' }}>
              <div style={styles.wrHeader}>My Queue ({queue.length})</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                {queue.length === 0 ? (
                  <div style={{ color: '#888', textAlign: 'center', padding: '20px' }}>No players in queue. Star players in the pool to add them here!</div>
                ) : (
                  queue.map(p => (
                    <div key={p['ESPN PlayerID']} style={{ ...styles.queueItem, background: '#222' }}>
                      <div>
                        <div style={{ color: '#fff', fontWeight: 'bold' }}>{p.Player}</div>
                        <div style={{ fontSize: '12px', color: '#888' }}>{p.Position} - {p.Team}</div>
                      </div>
                      <button onClick={() => removeFromQueue(p['ESPN PlayerID'])} style={styles.btnStarActive}>✕</button>
                    </div>
                  ))
                )}
              </div>
            </div>
          )}
          {activeTab === 'Roster' && (
            <RosterManagerPanel
              allPicks={displayPicks}
              players={displayPlayers}
              currentUser={currentUser}
            />
          )}
          {activeTab === 'Feed' && (
            <div style={{ height: '100%', overflowY: 'auto' }}>
              <div style={styles.wrHeader}>Draft Feed</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                {recentPicks.slice().reverse().map(p => {
                  const playerObj = displayPlayers.find(pl => String(pl['ESPN PlayerID']) === String(p['ESPN PlayerID']));
                  return (
                    <div key={p['Overall Pick']} style={{
                      padding: '10px',
                      background: '#222',
                      borderRadius: '4px',
                      borderLeft: `4px solid ${p.Owner === currentUser ? '#03dac6' : '#555'}`
                    }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', color: '#888', fontSize: '11px' }}>
                        <span>Pick #{p['Overall Pick']}</span>
                        <span>{p.Owner}</span>
                      </div>
                      <div style={{ color: '#fff', fontWeight: 'bold', fontSize: '14px' }}>
                        {playerObj?.Player || p.Selection || 'Pick Made'}
                      </div>
                      <div style={{ color: '#aaa', fontSize: '12px' }}>
                        {playerObj?.Position} - {playerObj?.Team}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {/* Mobile Bottom Nav */}
        <div style={{
          gridColumn: '1 / -1',
          background: '#2c2c2c',
          borderTop: '1px solid #444',
          display: 'flex',
          justifyContent: 'space-around',
          alignItems: 'center'
        }}>
          {[
            { id: 'Pool', label: 'Pool', icon: '📋' },
            { id: 'Queue', label: 'Queue', icon: '⭐' },
            { id: 'Roster', label: 'Roster', icon: '👥' },
            { id: 'Feed', label: 'Feed', icon: '📢' }
          ].map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              style={{
                background: 'none',
                border: 'none',
                color: activeTab === tab.id ? '#03dac6' : '#888',
                padding: '8px',
                fontSize: '11px',
                fontWeight: 'bold',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                cursor: 'pointer'
              }}
            >
              <span style={{ fontSize: '18px', marginBottom: '2px' }}>{tab.icon}</span>
              {tab.label}
            </button>
          ))}
        </div>

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
            isMobile={true}
          />
        )}
      </div>
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
        <div style={styles.header}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
            <div style={styles.leagueLogo}>
              ⚾ Hefty War Room {roomSeason}
            </div>

            {/* In-Room Season Switcher */}
            <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '2px', border: '1px solid #333' }}>
              <button
                onClick={() => handleSeasonChange(2027)}
                style={{
                  background: roomSeason === 2027 ? '#bb86fc' : 'transparent',
                  color: roomSeason === 2027 ? '#000' : '#888',
                  border: 'none',
                  borderRadius: '4px',
                  padding: '3px 9px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
              >
                🚀 2027 Draft
              </button>
              <button
                onClick={() => handleSeasonChange(2026)}
                style={{
                  background: roomSeason === 2026 ? '#03dac6' : 'transparent',
                  color: roomSeason === 2026 ? '#000' : '#888',
                  border: 'none',
                  borderRadius: '4px',
                  padding: '3px 9px',
                  fontSize: '11px',
                  fontWeight: 'bold',
                  cursor: 'pointer'
                }}
              >
                🏛️ 2026 Archive
              </button>
            </div>
            {draftMode === 'test' && (
              <span style={{ 
                fontSize: '12px', 
                background: '#ffc10722', 
                color: '#ffc107', 
                border: '1px solid #ffc107', 
                padding: '3px 8px', 
                borderRadius: '4px', 
                fontWeight: 'bold' 
              }}>
                [TEST]
              </span>
            )}
            {draftMode === 'multitest' && (
              <span style={{ 
                fontSize: '12px', 
                background: '#ff980022', 
                color: '#ff9800', 
                border: '1px solid #ff9800', 
                padding: '3px 8px', 
                borderRadius: '4px', 
                fontWeight: 'bold' 
              }}>
                [MULTIPLAYER TEST]
              </span>
            )}
            {draftMode === 'mockdraft' && (
              <span style={{ 
                fontSize: '12px', 
                background: '#bb86fc22', 
                color: '#bb86fc', 
                border: '1px solid #bb86fc', 
                padding: '3px 8px', 
                borderRadius: '4px', 
                fontWeight: 'bold' 
              }}>
                [MOCK DRAFT]
              </span>
            )}
            {draftMode === 'multitest' && (
              <button
                onClick={handleResetDraft}
                disabled={resetting}
                style={{
                  background: resetting ? '#666' : '#f44336',
                  color: '#fff',
                  border: 'none',
                  padding: '4px 10px',
                  borderRadius: '4px',
                  cursor: resetting ? 'wait' : 'pointer',
                  fontSize: '11px',
                  fontWeight: 'bold'
                }}
              >
                {resetting ? 'Resetting...' : '🔄 Reset Draft'}
              </button>
            )}
            <button
              onClick={handleResetMode}
              style={{
                background: 'transparent',
                color: '#888',
                border: '1px solid #444',
                padding: '4px 8px',
                borderRadius: '4px',
                fontSize: '11px',
                cursor: 'pointer'
              }}
              title="Switch between Live and Test Mode"
            >
              Switch Mode
            </button>
            <button
              onClick={handleResetUser}
              style={{
                background: 'transparent',
                color: '#888',
                border: '1px solid #444',
                padding: '4px 8px',
                borderRadius: '4px',
                fontSize: '11px',
                cursor: 'pointer'
              }}
              title="Change active team"
            >
              Switch Team ({currentUser})
            </button>
          </div>

          <div style={{ textAlign: 'center', color: '#fff' }}>
            <div style={{ fontSize: '11px', color: '#888', letterSpacing: '1px' }}>ON THE CLOCK</div>
            <div style={{ fontSize: '22px', color: 'var(--highlight)', fontWeight: 'bold' }}>
              {currentPick ? currentPick.Owner : 'DRAFT COMPLETE'}
            </div>
          </div>

          <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
            <button
              onClick={() => setAudioEnabled(!audioEnabled)}
              style={{
                background: audioEnabled ? '#4caf50' : '#444',
                color: '#fff',
                border: 'none',
                padding: '8px 14px',
                borderRadius: '6px',
                cursor: 'pointer',
                fontSize: '13px',
                fontWeight: 'bold',
                display: 'flex',
                alignItems: 'center',
                gap: '6px'
              }}
              title="Toggle draft audio announcements"
            >
              <span>{audioEnabled ? '🔊' : '🔇'}</span>
              <span>Audio {audioEnabled ? 'ON' : 'OFF'}</span>
            </button>

            {draftMode === 'test' && (
              <button
                onClick={() => {
                  if (!audioEnabled) {
                    alert('Please enable audio first by clicking the Audio ON button');
                    return;
                  }
                  playOwnerSound(currentUser);
                }}
                style={{
                  background: '#ffc107',
                  color: '#000',
                  border: 'none',
                  padding: '8px 12px',
                  borderRadius: '6px',
                  cursor: 'pointer',
                  fontSize: '13px',
                  fontWeight: 'bold'
                }}
              >
                🧪 Test Audio
              </button>
            )}

            <CountdownTimer pickStartTime={pickStartTime} />

            {onSwitchView && (
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <button
                  onClick={() => onSwitchView('capital')}
                  style={{
                    background: '#2e1065',
                    color: '#d8b4fe',
                    border: '1px solid #7e22ce',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    fontSize: '13px',
                    fontWeight: 'bold',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    cursor: 'pointer'
                  }}
                  title="View 2027 Draft Capital & Traded Picks"
                >
                  <span>🎟️</span>
                  <span>Draft Capital</span>
                </button>
                <button
                  onClick={() => onSwitchView('keepers')}
                  style={{
                    background: '#064e3b',
                    color: '#6ee7b7',
                    border: '1px solid #059669',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    fontSize: '13px',
                    fontWeight: 'bold',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    cursor: 'pointer'
                  }}
                  title="View Keepers & Budgets Panel"
                >
                  <span>💎</span>
                  <span>Keepers & Budget</span>
                </button>
                <button
                  onClick={() => onSwitchView('weekly')}
                  style={{
                    background: '#1e3a8a',
                    color: '#fff',
                    border: '1px solid #3b82f6',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    fontSize: '13px',
                    fontWeight: 'bold',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    cursor: 'pointer'
                  }}
                  title="Return to Main League Site"
                >
                  <span>⚾</span>
                  <span>League Site</span>
                </button>
              </div>
            )}
          </div>
        </div>

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
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                <span style={{ fontSize: '11px', color: '#bb86fc', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  🎙️ Instant Reaction
                </span>
                {lastPickCommentary && lastPickCommentary !== 'Draft has not started.' && !generatingCommentary && (
                  <button
                    onClick={() => playPodcastTTS(lastPickCommentary)}
                    style={{
                      background: 'rgba(187, 134, 252, 0.15)',
                      border: '1px solid #bb86fc',
                      color: '#bb86fc',
                      borderRadius: '4px',
                      padding: '2px 6px',
                      fontSize: '10px',
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
              {generatingCommentary ? (
                <span>🤔 Gemini is analyzing this selection...</span>
              ) : (
                <span dangerouslySetInnerHTML={{ __html: lastPickCommentary }} />
              )}
            </div>

            {/* Skip & Chip 2-Person Podcast Debate Widget */}
            <PodcastBreakdownWidget
              script={lastPodcastScript}
              isGenerating={generatingPodcast}
              isPlaying={isPlayingPodcast}
              activeLine={activePodcastLine}
              onPlay={() => handlePlayPodcast()}
              onStop={handleStopPodcast}
              compact={true}
              playerName={lastPickPlayer?.Player}
            />
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
        {showDashboard && (
          <div style={styles.warRoomPanel}>
            <div style={styles.panelNav}>
              {['Pool', 'Roster', 'MyPicks', 'DraftLog', 'Analysis', 'Standings'].map(tab => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  style={activeTab === tab ? styles.navBtnActive : styles.navBtn}
                >
                  {tab === 'Pool' ? 'Draft Pool' : 
                   tab === 'Roster' ? 'Roster Manager' : 
                   tab === 'MyPicks' ? 'My Picks' :
                   tab === 'DraftLog' ? 'Draft Log' :
                   tab === 'Analysis' ? 'Analysis History' :
                   '🏆 Projected Roto'}
                </button>
              ))}
              <div style={{ marginLeft: 'auto', color: '#888', fontSize: '13px', display: 'flex', alignItems: 'center' }}>
                Logged in as: <span style={{ color: 'var(--highlight)', fontWeight: 'bold', marginLeft: '5px' }}>{currentUser}</span>
              </div>
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Pool' ? 'grid' : 'none' }}>
              <PlayerPoolPanel
                players={displayPlayers}
                onDraft={handleDraft}
                isMyTurn={isMyTurn}
                queue={queue}
                onAddToQueue={addToQueue}
                onRemoveFromQueue={removeFromQueue}
                draftMode={draftMode}
                testModePicks={testModePicks}
                allPicks={displayPicks}
                onPlayerClick={setSelectedPlayer}
                playerInfo={playerInfo}
                currentUser={currentUser}
                recommendations={smartRecommendations}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Roster' ? 'grid' : 'none' }}>
              <RosterManagerPanel
                allPicks={displayPicks}
                players={displayPlayers}
                currentUser={currentUser}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'MyPicks' ? 'grid' : 'none' }}>
              <MyPicksPanel
                allPicks={displayPicks}
                players={displayPlayers}
                currentUser={currentUser}
                draftTrades={draftTrades}
                roomSeason={roomSeason}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'DraftLog' ? 'grid' : 'none' }}>
              <DraftLogPanel
                allPicks={displayPicks}
                players={displayPlayers}
                onPlayerClick={setSelectedPlayer}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Analysis' ? 'grid' : 'none' }}>
              <AnalysisHistoryPanel
                analysisHistory={analysisHistory}
                players={displayPlayers}
                roomSeason={roomSeason}
              />
            </div>

            <div style={{ ...styles.panelContent, display: activeTab === 'Standings' ? 'grid' : 'none' }}>
              <StandingsPanel 
                allPicks={displayPicks}
                keepers={_keepers}
                roomSeason={roomSeason}
              />
            </div>
          </div>
        )}

        {/* Mock Draft Floating Controller */}
        {draftMode === 'mockdraft' && (
          <div style={{
            position: 'fixed',
            bottom: '55px',
            left: '50%',
            transform: 'translateX(-50%)',
            display: 'flex',
            alignItems: 'center',
            gap: '10px',
            background: '#1a1a2e',
            border: '1px solid #bb86fc44',
            borderRadius: '12px',
            padding: '8px 18px',
            zIndex: 150,
            boxShadow: '0 4px 20px rgba(0,0,0,0.6)'
          }}>
            <span style={{ color: '#bb86fc', fontSize: '11px', fontWeight: 'bold', letterSpacing: '1px' }}>
              🤖 MOCK DRAFT
            </span>
            <div style={{ width: '1px', height: '20px', background: '#444' }} />
            <button
              onClick={() => setIsRunningMock(r => !r)}
              style={{
                background: isRunningMock ? '#f44336' : '#4caf50',
                color: '#fff',
                border: 'none',
                padding: '6px 16px',
                borderRadius: '6px',
                cursor: 'pointer',
                fontWeight: 'bold',
                fontSize: '12px'
              }}
            >
              {isRunningMock ? '⏸ PAUSE' : '▶ RUN'}
            </button>
            {!isRunningMock && (
              <button
                onClick={stepMockPick}
                disabled={currentPick?.Owner === currentUser}
                title="Advance one pick"
                style={{
                  background: currentPick?.Owner === currentUser ? '#444' : '#ffc107',
                  color: currentPick?.Owner === currentUser ? '#666' : '#000',
                  border: 'none',
                  padding: '6px 14px',
                  borderRadius: '6px',
                  cursor: currentPick?.Owner === currentUser ? 'not-allowed' : 'pointer',
                  fontWeight: 'bold',
                  fontSize: '12px'
                }}
              >
                ⏭ STEP
              </button>
            )}
            <div style={{ width: '1px', height: '20px', background: '#444' }} />
            <span style={{ color: '#888', fontSize: '11px' }}>Speed:</span>
            {[
              { label: '🐢', title: 'Slow (3s)', val: 3000 },
              { label: '⚡', title: 'Fast (1s)', val: 1000 },
              { label: '🚀', title: 'Turbo (300ms)', val: 300 }
            ].map(s => (
              <button
                key={s.val}
                onClick={() => setMockSpeed(s.val)}
                title={s.title}
                style={{
                  background: mockSpeed === s.val ? '#bb86fc' : '#333',
                  color: mockSpeed === s.val ? '#000' : '#ccc',
                  border: 'none',
                  padding: '5px 10px',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '14px'
                }}
              >
                {s.label}
              </button>
            ))}
            <div style={{ width: '1px', height: '20px', background: '#444' }} />
            {currentPick?.Owner === currentUser ? (
              <span style={{ color: '#ffc107', fontWeight: 'bold', fontSize: '12px', animation: 'pulse 1s infinite' }}>
                ⭐ YOUR PICK!
              </span>
            ) : (
              <span style={{ color: '#888', fontSize: '12px' }}>
                On clock: <span style={{ color: '#03dac6', fontWeight: 'bold' }}>{currentPick?.Owner || '—'}</span>
              </span>
            )}
          </div>
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
