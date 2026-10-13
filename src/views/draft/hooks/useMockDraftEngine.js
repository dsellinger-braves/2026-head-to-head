import { useState, useEffect, useCallback } from 'react';
import {
  OWNER_ARCHETYPES,
  normalizeManager,
  getPlayerProjectionStats
} from '../../../utils/draftRotoEngine';

const DEFAULT_OWNER_PROFILES = OWNER_ARCHETYPES;

/**
 * useMockDraftEngine
 * Manages the AI-driven mock draft simulation loop, archetype scoring, and stepping.
 */
export function useMockDraftEngine({
  draftMode,
  currentPick,
  currentUser,
  displayPicks,
  playersRef,
  roomSeason,
  setPickStartTime,
  setTestModePicks,
  handleNewPick
}) {
  const [isRunningMock, setIsRunningMock] = useState(false);
  const [mockSpeed, setMockSpeed] = useState(1000);

  const stepMockPick = useCallback(() => {
    const current = currentPick;
    if (!current || current.Owner === currentUser) {
      setIsRunningMock(false);
      return;
    }

    const takenIds = new Set(displayPicks.map(p => String(p['ESPN PlayerID'])).filter(Boolean));
    const available = (playersRef.current || []).filter(p => !takenIds.has(String(p['ESPN PlayerID'])));
    if (!available.length) return;

    const ownerRoster = displayPicks
      .filter(p => p.Owner === current.Owner && p['ESPN PlayerID'])
      .map(p => (playersRef.current || []).find(pl => String(pl['ESPN PlayerID']) === String(p['ESPN PlayerID'])))
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
      Round: current.Round,
      Selection: chosen.Player
    });
  }, [currentPick, displayPicks, currentUser, handleNewPick, roomSeason, playersRef, setPickStartTime, setTestModePicks]);

  useEffect(() => {
    if (draftMode !== 'mockdraft' || !isRunningMock) return;

    if (!currentPick || currentPick.Owner === currentUser) {
      return;
    }

    const timer = setTimeout(() => {
      stepMockPick();
    }, mockSpeed);

    return () => clearTimeout(timer);
  }, [draftMode, isRunningMock, mockSpeed, currentPick, currentUser, stepMockPick]);

  return {
    isRunningMock,
    setIsRunningMock,
    mockSpeed,
    setMockSpeed,
    stepMockPick
  };
}

export default useMockDraftEngine;
