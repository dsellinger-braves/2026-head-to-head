// src/context/AuthContext.jsx
import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import { LEAGUE_OWNERS } from '../utils/mlbTeams';
import { AuthContext } from './authContextDef';

// Utility to immediately clean sensitive or stale OAuth tokens from the browser URL hash
function cleanupOAuthHash() {
  if (typeof window === 'undefined') return;
  const rawHash = window.location.hash || '';
  if (rawHash.includes('access_token=') || rawHash.includes('refresh_token=') || rawHash.includes('error=')) {
    let target = '#/keepers';
    try {
      const saved = sessionStorage.getItem('oauth_pre_login_hash');
      if (saved) {
        sessionStorage.removeItem('oauth_pre_login_hash');
        target = saved.startsWith('#') ? saved : `#/${saved}`;
      }
    } catch {
      // ignore
    }
    try {
      const cleanPath = window.location.pathname + window.location.search + target;
      window.history.replaceState(null, '', cleanPath);
    } catch {
      window.location.hash = target;
    }
    window.dispatchEvent(new HashChangeEvent('hashchange'));
  }
}

const STATIC_LEAGUE_PROFILES = {
  dsellinger: { team_id: 5, owner_name: 'Daniel', role: 'commissioner' },
  dan: { team_id: 5, owner_name: 'Daniel', role: 'commissioner' },
  daniel: { team_id: 5, owner_name: 'Daniel', role: 'commissioner' },
  adriaxx: { team_id: 2, owner_name: 'Adrian', role: 'commissioner' },
  adrian: { team_id: 2, owner_name: 'Adrian', role: 'commissioner' },
  aznchuy: { team_id: 1, owner_name: 'Tim', role: 'owner' },
  tim: { team_id: 1, owner_name: 'Tim', role: 'owner' },
  ghutch: { team_id: 3, owner_name: 'Garrett', role: 'owner' },
  garrett: { team_id: 3, owner_name: 'Garrett', role: 'owner' },
  anilbhairo: { team_id: 6, owner_name: 'Anil', role: 'owner' },
  anil: { team_id: 6, owner_name: 'Anil', role: 'owner' },
  ay0h: { team_id: 8, owner_name: 'Alex', role: 'owner' },
  alex: { team_id: 8, owner_name: 'Alex', role: 'owner' },
  senorspice: { team_id: 12, owner_name: 'Will', role: 'owner' },
  will: { team_id: 12, owner_name: 'Will', role: 'owner' },
  mrussell38: { team_id: 13, owner_name: 'Mark', role: 'owner' },
  mark: { team_id: 13, owner_name: 'Mark', role: 'owner' },
  pston3: { team_id: 14, owner_name: 'Preston', role: 'owner' },
  preston: { team_id: 14, owner_name: 'Preston', role: 'owner' },
};

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [profile, setProfile] = useState(null);
  const [allProfiles, setAllProfiles] = useState([]);
  const [loading, setLoading] = useState(true);

  // Commissioner override: allows Dan (Team 5) and Adrian (Team 2) to manage any team
  const [overrideTeamId, setOverrideTeamId] = useState(null);

  // Fetch all profiles from Supabase with safe 4s timeout
  const fetchProfiles = useCallback(async () => {
    try {
      const queryPromise = supabase
        .from('league_profiles')
        .select('*')
        .order('team_id', { ascending: true });
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('fetchProfiles timeout')), 4000)
      );
      const { data, error } = await Promise.race([queryPromise, timeoutPromise]);
      if (!error && data) {
        setAllProfiles(data);
        return data;
      }
    } catch (err) {
      console.warn('Could not fetch league profiles:', err);
    }
    return [];
  }, []);

  // Sync Supabase user with league_profiles
  const syncUserProfile = useCallback(async (authUser, profilesList) => {
    if (!authUser) {
      setProfile(null);
      setOverrideTeamId(null);
      return;
    }

    const profiles = profilesList && profilesList.length > 0 ? profilesList : await fetchProfiles();
    const meta = authUser.user_metadata || {};
    
    // Normalized Discord username candidates
    const candidateUsernames = [
      meta.user_name,
      meta.preferred_username,
      authUser.identities?.[0]?.identity_data?.user_name,
      meta.custom_claims?.preferred_username,
      meta.name,
      meta.full_name,
      meta.custom_claims?.global_name,
    ]
      .filter(Boolean)
      .map(s => String(s).replace(/#\d+$/, '').toLowerCase().trim());

    const discordUsername = candidateUsernames[0] || '';
    const discordId = authUser.identities?.[0]?.id || meta.provider_id || authUser.identities?.[0]?.identity_data?.provider_id || null;
    const avatarUrl = meta.avatar_url || meta.picture || null;

    // Match profile: 1. by linked user_id, 2. by discord_id, 3. by normalized discord_username candidates, 4. static fallback
    let matched = profiles.find(p => p.user_id === authUser.id);
    if (!matched && discordId) {
      matched = profiles.find(p => p.discord_id && String(p.discord_id) === String(discordId));
    }
    if (!matched) {
      matched = profiles.find(p => {
        const pUser = (p.discord_username || '').toLowerCase().trim();
        const pOwner = (p.owner_name || '').toLowerCase().trim();
        return candidateUsernames.includes(pUser) || candidateUsernames.includes(pOwner);
      });
    }

    // Static fallback if DB row not found or delayed
    if (!matched) {
      for (const u of candidateUsernames) {
        if (STATIC_LEAGUE_PROFILES[u]) {
          const s = STATIC_LEAGUE_PROFILES[u];
          matched = {
            id: `static_${s.team_id}`,
            user_id: authUser.id,
            discord_id: discordId ? String(discordId) : null,
            discord_username: u,
            owner_name: s.owner_name,
            team_id: s.team_id,
            role: s.role,
            avatar_url: avatarUrl,
          };
          break;
        }
      }
    }

    if (matched) {
      const canonicalName = matched.owner_name === 'Dan' ? 'Daniel' : (matched.owner_name || 'Owner');
      const normalizedMatch = { ...matched, owner_name: canonicalName };

      // Link user_id, discord_id, and update avatar in Supabase if not yet linked
      if (matched.id && !String(matched.id).startsWith('static_')) {
        if (matched.user_id !== authUser.id || (discordId && matched.discord_id !== String(discordId)) || matched.avatar_url !== avatarUrl) {
          try {
            await supabase
              .from('league_profiles')
              .update({
                user_id: authUser.id,
                discord_id: discordId ? String(discordId) : matched.discord_id,
                avatar_url: avatarUrl || matched.avatar_url,
                updated_at: new Date().toISOString()
              })
              .eq('id', matched.id);
          } catch (e) {
            console.warn('Profile link update notice:', e);
          }
        }
      }
      setProfile(normalizedMatch);
    } else {
      // Fallback guest profile if Discord user isn't in league_profiles
      setProfile({
        discord_username: discordUsername || 'Guest',
        owner_name: meta.full_name || meta.name || 'Guest User',
        team_id: null,
        role: 'guest',
        avatar_url: avatarUrl
      });
    }
  }, [fetchProfiles]);

  // Listen to auth state changes
  useEffect(() => {
    let mounted = true;

    async function initAuth() {
      try {
        const sessionPromise = supabase.auth.getSession();
        const timeoutPromise = new Promise(resolve =>
          setTimeout(() => resolve({ data: { session: null } }), 3500)
        );
        const [profiles, { data: sessionData }] = await Promise.all([
          fetchProfiles(),
          Promise.race([sessionPromise, timeoutPromise])
        ]);
        const session = sessionData?.session;
        if (mounted) {
          setUser(session?.user || null);
          if (session?.user) {
            await syncUserProfile(session.user, profiles);
          }
          cleanupOAuthHash();
          setLoading(false);
        }
      } catch (err) {
        console.warn('initAuth notice:', err);
        cleanupOAuthHash();
        if (mounted) setLoading(false);
      }
    }

    initAuth();

    const { data: { subscription } } = supabase.auth.onAuthStateChange(async (event, session) => {
      if (!mounted) return;
      setUser(session?.user || null);
      if (session?.user) {
        await syncUserProfile(session.user);
        cleanupOAuthHash();
      } else {
        setProfile(null);
        setOverrideTeamId(null);
      }
      setLoading(false);
    });

    return () => {
      mounted = false;
      subscription?.unsubscribe();
    };
  }, [fetchProfiles, syncUserProfile]);

  const signInWithDiscord = useCallback(async () => {
    try {
      if (typeof window !== 'undefined') {
        const currentHash = window.location.hash || '#/keepers';
        if (!currentHash.includes('access_token=') && !currentHash.includes('refresh_token=')) {
          sessionStorage.setItem('oauth_pre_login_hash', currentHash);
        }
      }
    } catch (e) {
      console.warn('Could not save pre-login hash:', e);
    }

    const origin = window.location.origin;
    const pathname = window.location.pathname.endsWith('/') ? window.location.pathname : window.location.pathname + '/';
    const redirectTo = `${origin}${pathname}`;

    const { data, error } = await supabase.auth.signInWithOAuth({
      provider: 'discord',
      options: {
        redirectTo,
        scopes: 'identify email'
      }
    });
    if (error) {
      console.error('Discord sign in failed:', error);
      throw error;
    }
    return data;
  }, []);

  // Break-glass Commissioner Checkout State:
  // Dan (Team 5) and Adrian (Team 2) default to REGULAR owners until explicitly checking out powers!
  const [isCommishCheckedOut, setIsCommishCheckedOut] = useState(() => {
    if (typeof window === 'undefined') return false;
    try {
      return sessionStorage.getItem('commish_powers_checked_out') === 'true';
    } catch {
      return false;
    }
  });

  const [commishCheckoutReason, setCommishCheckoutReason] = useState(() => {
    if (typeof window === 'undefined') return '';
    try {
      return sessionStorage.getItem('commish_checkout_reason') || '';
    } catch {
      return '';
    }
  });

  const signOut = useCallback(async () => {
    await supabase.auth.signOut();
    setUser(null);
    setProfile(null);
    setOverrideTeamId(null);
    setIsCommishCheckedOut(false);
    setCommishCheckoutReason('');
    try {
      sessionStorage.removeItem('commish_powers_checked_out');
      sessionStorage.removeItem('commish_checkout_reason');
    } catch {
      // ignore
    }
  }, []);

  // Commissioner eligibility: strictly Dan (Team 5) and Adrian (Team 2)
  const isCommishEligible = useMemo(() => {
    if (!profile) return false;
    return profile.role === 'commissioner' || profile.team_id === 5 || profile.team_id === 2;
  }, [profile]);

  // Active commissioner status: requires BOTH eligibility AND explicit checkout!
  const isCommissioner = useMemo(() => {
    return Boolean(isCommishEligible && isCommishCheckedOut);
  }, [isCommishEligible, isCommishCheckedOut]);

  // Audit logging helper
  const logCommissionerAction = useCallback(async ({
    actionType,
    actionDescription,
    targetTeamId = null,
    targetOwner = null,
    details = {}
  }) => {
    if (!profile) return;
    try {
      await supabase.from('commissioner_audit_logs').insert({
        season_year: 2026,
        commissioner_name: profile.owner_name,
        commissioner_team_id: profile.team_id || 0,
        commissioner_discord_id: profile.discord_id || null,
        action_type: actionType,
        action_description: actionDescription,
        target_team_id: targetTeamId,
        target_owner: targetOwner,
        details: {
          ...details,
          checkout_reason: commishCheckoutReason,
          timestamp: new Date().toISOString()
        }
      });
    } catch (err) {
      console.warn('Failed to log commissioner action:', err);
    }
  }, [profile, commishCheckoutReason]);

  // Check out commissioner powers with reason and post to #league-news
  const checkoutCommissionerPowers = useCallback(async (reason = '') => {
    if (!isCommishEligible || !profile) return false;
    const finalReason = reason?.trim() || 'Administrative maintenance & trade management';
    setIsCommishCheckedOut(true);
    setCommishCheckoutReason(finalReason);
    try {
      sessionStorage.setItem('commish_powers_checked_out', 'true');
      sessionStorage.setItem('commish_checkout_reason', finalReason);
    } catch {
      // ignore
    }

    // 1. Log to commissioner_audit_logs table
    try {
      await supabase.from('commissioner_audit_logs').insert({
        season_year: 2026,
        commissioner_name: profile.owner_name,
        commissioner_team_id: profile.team_id || 0,
        commissioner_discord_id: profile.discord_id || null,
        action_type: 'checkout_powers',
        action_description: `${profile.owner_name} checked out commissioner powers`,
        details: {
          reason: finalReason,
          activated_at: new Date().toISOString()
        }
      });
    } catch (e) {
      console.warn('Audit log insert error:', e);
    }

    // 2. Dispatch announcement for Discord #league-news channel delivery
    try {
      await supabase.from('trade_notifications').insert({
        trade_proposal_id: null,
        event_type: 'commish_checkout',
        sender_team_id: profile.team_id || 0,
        sender_owner: profile.owner_name,
        recipient_team_id: 0,
        recipient_owner: 'League',
        details: {
          reason: finalReason,
          channel: 'league-news',
          activated_at: new Date().toISOString()
        },
        status: 'pending'
      });
    } catch (e) {
      console.warn('Announcement queue error:', e);
    }

    return true;
  }, [isCommishEligible, profile]);

  // Relinquish commissioner powers back to standard owner view
  const relinquishCommissionerPowers = useCallback(async () => {
    if (!profile) return;
    setIsCommishCheckedOut(false);
    setCommishCheckoutReason('');
    setOverrideTeamId(null);
    try {
      sessionStorage.removeItem('commish_powers_checked_out');
      sessionStorage.removeItem('commish_checkout_reason');
    } catch {
      // ignore
    }

    // Log to audit log
    try {
      await supabase.from('commissioner_audit_logs').insert({
        season_year: 2026,
        commissioner_name: profile.owner_name,
        commissioner_team_id: profile.team_id || 0,
        commissioner_discord_id: profile.discord_id || null,
        action_type: 'relinquish_powers',
        action_description: `${profile.owner_name} checked in / relinquished commissioner powers`,
        details: {
          relinquished_at: new Date().toISOString()
        }
      });
    } catch (e) {
      console.warn('Audit log insert error:', e);
    }
  }, [profile]);

  // The team currently being acted as (either own team or commissioner override)
  const effectiveTeamId = useMemo(() => {
    if (isCommissioner && overrideTeamId !== null) {
      return overrideTeamId;
    }
    return profile?.team_id || null;
  }, [isCommissioner, overrideTeamId, profile]);

  const effectiveOwner = useMemo(() => {
    if (effectiveTeamId) {
      const found = LEAGUE_OWNERS.find(o => o.id === effectiveTeamId);
      if (found) return found.name === 'Dan' ? 'Daniel' : found.name;
    }
    if (profile?.owner_name) {
      const norm = profile.owner_name === 'Dan' ? 'Daniel' : profile.owner_name;
      const match = LEAGUE_OWNERS.find(o => o.name.toLowerCase() === norm.toLowerCase());
      if (match) return match.name === 'Dan' ? 'Daniel' : match.name;
      const staticInfo = STATIC_LEAGUE_PROFILES[profile.owner_name.toLowerCase()];
      if (staticInfo) return staticInfo.owner_name;
      return norm;
    }
    return null;
  }, [effectiveTeamId, profile]);

  const setEffectiveTeamId = useCallback((teamId) => {
    if (!isCommissioner) return;
    setOverrideTeamId(teamId ? parseInt(teamId) : null);
  }, [isCommissioner]);

  const value = useMemo(() => ({
    user,
    profile,
    allProfiles,
    loading,
    isCommishEligible,
    isCommishCheckedOut,
    commishCheckoutReason,
    isCommissioner,
    checkoutCommissionerPowers,
    relinquishCommissionerPowers,
    logCommissionerAction,
    effectiveTeamId,
    effectiveOwner,
    isActingAsOther: isCommissioner && overrideTeamId !== null && overrideTeamId !== profile?.team_id,
    setEffectiveTeamId,
    signInWithDiscord,
    signOut,
    refreshProfiles: fetchProfiles
  }), [
    user,
    profile,
    allProfiles,
    loading,
    isCommishEligible,
    isCommishCheckedOut,
    commishCheckoutReason,
    isCommissioner,
    checkoutCommissionerPowers,
    relinquishCommissionerPowers,
    logCommissionerAction,
    effectiveTeamId,
    effectiveOwner,
    overrideTeamId,
    setEffectiveTeamId,
    signInWithDiscord,
    signOut,
    fetchProfiles
  ]);

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
}
