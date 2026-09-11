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
    
    // Normalized Discord username from custom_claims or profile data, stripping #discriminator (#0, #1234) if present
    const rawName = (
      meta.custom_claims?.global_name ||
      meta.full_name ||
      meta.user_name ||
      meta.preferred_username ||
      meta.name ||
      ''
    );
    const discordUsername = rawName.replace(/#\d+$/, '').toLowerCase().trim();
    const discordId = authUser.identities?.[0]?.id || meta.provider_id || authUser.identities?.[0]?.identity_data?.provider_id || null;
    const avatarUrl = meta.avatar_url || meta.picture || null;

    // Match profile: 1. by linked user_id, 2. by discord_id, 3. by normalized discord_username
    let matched = profiles.find(p => p.user_id === authUser.id);
    if (!matched && discordId) {
      matched = profiles.find(p => p.discord_id && String(p.discord_id) === String(discordId));
    }
    if (!matched && discordUsername) {
      matched = profiles.find(p => (p.discord_username || '').toLowerCase().trim() === discordUsername);
    }

    if (matched) {
      // Link user_id, discord_id, and update avatar in Supabase if not yet linked
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
          matched = { 
            ...matched, 
            user_id: authUser.id, 
            discord_id: discordId ? String(discordId) : matched.discord_id, 
            avatar_url: avatarUrl || matched.avatar_url 
          };
        } catch (e) {
          console.warn('Profile link update notice:', e);
        }
      }
      setProfile(matched);
    } else {
      // Fallback guest profile if Discord username isn't in league_profiles
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

  const signOut = useCallback(async () => {
    await supabase.auth.signOut();
    setUser(null);
    setProfile(null);
    setOverrideTeamId(null);
  }, []);

  // Commissioner validation: strictly Dan (Team 5) and Adrian (Team 2)
  const isCommissioner = useMemo(() => {
    if (!profile) return false;
    return profile.role === 'commissioner' || profile.team_id === 5 || profile.team_id === 2;
  }, [profile]);

  // The team currently being acted as (either own team or commissioner override)
  const effectiveTeamId = useMemo(() => {
    if (isCommissioner && overrideTeamId !== null) {
      return overrideTeamId;
    }
    return profile?.team_id || null;
  }, [isCommissioner, overrideTeamId, profile]);

  const effectiveOwner = useMemo(() => {
    if (!effectiveTeamId) return profile?.owner_name || null;
    const found = LEAGUE_OWNERS.find(o => o.id === effectiveTeamId);
    return found ? found.name : profile?.owner_name || null;
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
    isCommissioner,
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
    isCommissioner,
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
