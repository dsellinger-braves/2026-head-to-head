// src/context/AuthContext.jsx
import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { supabase } from '../supabaseClient';
import { LEAGUE_OWNERS } from '../utils/mlbTeams';
import { AuthContext } from './authContextDef';

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [profile, setProfile] = useState(null);
  const [allProfiles, setAllProfiles] = useState([]);
  const [loading, setLoading] = useState(true);

  // Commissioner override: allows Dan (Team 5) and Adrian (Team 2) to manage any team
  const [overrideTeamId, setOverrideTeamId] = useState(null);

  // Fetch all profiles from Supabase
  const fetchProfiles = useCallback(async () => {
    try {
      const { data, error } = await supabase
        .from('league_profiles')
        .select('*')
        .order('team_id', { ascending: true });
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
    const discordUsername = (meta.user_name || meta.preferred_username || meta.name || '').toLowerCase().trim();
    const discordId = authUser.identities?.[0]?.id || meta.provider_id || null;
    const avatarUrl = meta.avatar_url || null;

    // Match profile by discord_username or linked user_id
    let matched = profiles.find(p => p.user_id === authUser.id);
    if (!matched && discordUsername) {
      matched = profiles.find(p => (p.discord_username || '').toLowerCase() === discordUsername);
    }

    if (matched) {
      // Link user_id and update avatar in Supabase if not yet linked
      if (matched.user_id !== authUser.id || matched.avatar_url !== avatarUrl) {
        try {
          await supabase
            .from('league_profiles')
            .update({
              user_id: authUser.id,
              discord_id: discordId || matched.discord_id,
              avatar_url: avatarUrl || matched.avatar_url,
              updated_at: new Date().toISOString()
            })
            .eq('id', matched.id);
          matched = { ...matched, user_id: authUser.id, avatar_url: avatarUrl || matched.avatar_url };
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
      const profiles = await fetchProfiles();
      const { data: { session } } = await supabase.auth.getSession();
      if (mounted) {
        setUser(session?.user || null);
        if (session?.user) {
          await syncUserProfile(session.user, profiles);
        }
        setLoading(false);
      }
    }

    initAuth();

    const { data: { subscription } } = supabase.auth.onAuthStateChange(async (event, session) => {
      if (!mounted) return;
      setUser(session?.user || null);
      if (session?.user) {
        await syncUserProfile(session.user);
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
    const redirectTo = window.location.origin + window.location.pathname;
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
