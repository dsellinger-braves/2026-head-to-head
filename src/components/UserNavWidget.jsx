// src/components/UserNavWidget.jsx
import React, { useState, useRef, useEffect } from 'react';
import { useAuth } from '../context/useAuth';
import { LEAGUE_OWNERS } from '../utils/mlbTeams';
import CommishCheckoutModal from './CommishCheckoutModal';

export default function UserNavWidget() {
  const {
    user,
    profile,
    loading,
    isCommishEligible,
    isCommishCheckedOut,
    isCommissioner,
    relinquishCommissionerPowers,
    effectiveTeamId,
    effectiveOwner,
    isActingAsOther,
    setEffectiveTeamId,
    signInWithDiscord,
    signOut
  } = useAuth();

  const [menuOpen, setMenuOpen] = useState(false);
  const [checkoutModalOpen, setCheckoutModalOpen] = useState(false);
  const [loggingIn, setLoggingIn] = useState(false);
  const menuRef = useRef(null);

  // Close dropdown on click outside or ESC
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (menuRef.current && !menuRef.current.contains(e.target)) {
        setMenuOpen(false);
      }
    };
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') setMenuOpen(false);
    };
    document.addEventListener('mousedown', handleClickOutside);
    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, []);

  const handleLogin = async () => {
    try {
      setLoggingIn(true);
      await signInWithDiscord();
    } catch (err) {
      console.error('Discord login failed:', err);
      alert('Could not initiate Discord login. Please verify Supabase Discord provider is configured.');
    } finally {
      setLoggingIn(false);
    }
  };

  if (loading) {
    return (
      <div className="h-9 w-28 bg-slate-800/60 animate-pulse rounded-xl"></div>
    );
  }

  // LOGGED OUT STATE
  if (!user || !profile) {
    return (
      <button
        onClick={handleLogin}
        disabled={loggingIn}
        className="flex items-center gap-2 px-3.5 py-1.5 rounded-xl bg-[#5865F2]/90 hover:bg-[#5865F2] text-white text-xs font-bold transition-all shadow-md hover:shadow-indigo-500/20 active:scale-95 cursor-pointer disabled:opacity-50"
      >
        <svg className="w-4 h-4 fill-current" viewBox="0 0 24 24">
          <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028c.462-.63.874-1.295 1.226-1.994.021-.041.001-.09-.041-.106a13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.929 1.793 8.18 1.793 12.061 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.894.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.028zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.956-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.946 2.418-2.157 2.418z"/>
        </svg>
        <span>{loggingIn ? 'Connecting...' : 'Log In with Discord'}</span>
      </button>
    );
  }

  // LOGGED IN STATE
  return (
    <>
      <div className="relative" ref={menuRef}>
        <div className="flex items-center gap-2">
          {/* If Commissioner powers are dormant, show quick checkout button */}
          {isCommishEligible && !isCommishCheckedOut && (
            <button
              onClick={() => setCheckoutModalOpen(true)}
              className="hidden lg:flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-amber-500/15 hover:bg-amber-500/30 border border-amber-500/40 text-amber-300 text-xs font-bold transition-all cursor-pointer shadow-xs"
              title="Click to check out commissioner powers (action will be posted to #league-news)"
            >
              <span>🛡️</span>
              <span>Commish Checkout</span>
            </button>
          )}

          {/* If Commissioner is acting as another team */}
          {isActingAsOther && (
            <div className="hidden md:flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-amber-950/80 border border-amber-500/60 text-amber-200 text-xs font-bold animate-pulse shadow-sm">
              <span>⚠️ Acting As:</span>
              <span className="text-white underline">{effectiveOwner} (T{effectiveTeamId})</span>
              <button
                onClick={() => setEffectiveTeamId(profile.team_id)}
                className="ml-1 text-[10px] text-amber-400 hover:text-white cursor-pointer"
                title="Reset to My Team"
              >
                ✕ Reset
              </button>
            </div>
          )}

          <button
            onClick={() => setMenuOpen(!menuOpen)}
            className="flex items-center gap-2.5 p-1.5 pr-3 rounded-xl bg-slate-800/80 hover:bg-slate-700/80 border border-slate-700/60 text-white transition-all cursor-pointer shadow-sm"
          >
            {profile.avatar_url ? (
              <img
                src={profile.avatar_url}
                alt={profile.owner_name}
                className="w-7 h-7 rounded-lg object-cover ring-1 ring-indigo-500/50"
              />
            ) : (
              <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-indigo-600 to-purple-600 flex items-center justify-center text-xs font-black text-white shadow-xs">
                {(profile.owner_name || 'U')[0]}
              </div>
            )}

            <div className="text-left leading-tight hidden sm:block">
              <div className="text-xs font-bold text-slate-200 flex items-center gap-1.5">
                <span>{profile.owner_name}</span>
                {isCommissioner && (
                  <span className="text-[10px] font-black px-1.5 py-0.2 rounded bg-amber-400 text-slate-950 shadow-xs">
                    COMMISH ACTIVE
                  </span>
                )}
              </div>
              <div className="text-[10px] text-slate-400">
                {profile.team_id ? `Team ${profile.team_id}` : `@${profile.discord_username}`}
              </div>
            </div>

            <span className="text-slate-400 text-xs">▼</span>
          </button>
        </div>

        {/* DROPDOWN MENU */}
        {menuOpen && (
          <div className="absolute right-0 mt-2 w-72 bg-slate-900 border border-slate-700/80 rounded-2xl shadow-2xl z-50 p-3 space-y-3 animate-fade-in-up backdrop-blur-md">
            {/* User Header */}
            <div className="flex items-center gap-3 pb-3 border-b border-slate-800">
              {profile.avatar_url ? (
                <img
                  src={profile.avatar_url}
                  alt={profile.owner_name}
                  className="w-10 h-10 rounded-xl object-cover ring-2 ring-indigo-500/50"
                />
              ) : (
                <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-indigo-600 to-purple-600 flex items-center justify-center font-bold text-white text-base">
                  {(profile.owner_name || 'U')[0]}
                </div>
              )}
              <div className="overflow-hidden">
                <div className="text-sm font-bold text-white truncate flex items-center gap-1.5">
                  <span>{profile.owner_name}</span>
                  {isCommissioner ? (
                    <span className="text-[10px] font-black px-1.5 py-0.5 rounded bg-amber-400 text-slate-950">
                      👑 COMMISH ACTIVE
                    </span>
                  ) : isCommishEligible ? (
                    <span className="text-[10px] font-bold px-1.5 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
                      Commish Dormant
                    </span>
                  ) : null}
                </div>
                <div className="text-xs text-slate-400 truncate">
                  Discord: @{profile.discord_username}
                </div>
                {profile.team_id && (
                  <div className="text-[11px] font-semibold text-indigo-400">
                    Team {profile.team_id}
                  </div>
                )}
              </div>
            </div>

            {/* COMMISSIONER BREAK-GLASS CONTROLS (DAN & ADRIAN ONLY) */}
            {isCommishEligible && (
              <div className="bg-amber-950/30 border border-amber-500/40 rounded-xl p-3 space-y-2.5">
                <div className="flex items-center justify-between text-xs font-bold text-amber-300">
                  <span className="flex items-center gap-1.5">
                    <span>🛡️</span>
                    <span>Governance Powers</span>
                  </span>
                  {isCommissioner && (
                    <span className="text-[10px] bg-amber-500/20 text-amber-300 px-1.5 py-0.5 rounded font-black">
                      ACTIVE
                    </span>
                  )}
                </div>

                {!isCommishCheckedOut ? (
                  <div className="space-y-2">
                    <p className="text-[11px] text-slate-400 leading-relaxed">
                      Commish access is dormant by default. Check out powers to edit trades, manage rosters, or approve league actions.
                    </p>
                    <button
                      onClick={() => {
                        setMenuOpen(false);
                        setCheckoutModalOpen(true);
                      }}
                      className="w-full py-1.5 px-3 rounded-lg bg-amber-500 hover:bg-amber-400 text-slate-950 text-xs font-black transition-all cursor-pointer shadow-sm flex items-center justify-center gap-1.5"
                    >
                      <span>🛡️ Check Out Commish Powers</span>
                    </button>
                  </div>
                ) : (
                  <div className="space-y-2">
                    <div className="flex items-center justify-between text-[11px] text-amber-200">
                      <span>Acting as:</span>
                      <select
                        value={effectiveTeamId || ''}
                        onChange={(e) => setEffectiveTeamId(e.target.value ? parseInt(e.target.value) : null)}
                        className="bg-slate-950 border border-slate-700 rounded px-2 py-1 text-xs text-white font-semibold focus:outline-hidden focus:border-amber-400 cursor-pointer"
                      >
                        <option value={profile.team_id}>My Team ({profile.owner_name})</option>
                        {LEAGUE_OWNERS.filter(o => o.id !== profile.team_id).map((o) => (
                          <option key={o.id} value={o.id}>
                            Team {o.id} - {o.name}
                          </option>
                        ))}
                      </select>
                    </div>
                    <button
                      onClick={async () => {
                        await relinquishCommissionerPowers();
                        setMenuOpen(false);
                      }}
                      className="w-full py-1 px-3 rounded-lg bg-slate-950 hover:bg-slate-800 text-amber-300 hover:text-white text-xs font-bold transition-all border border-amber-500/40 cursor-pointer flex items-center justify-center gap-1.5"
                    >
                      <span>🔒 Relinquish Powers (Check In)</span>
                    </button>
                  </div>
                )}
              </div>
            )}

            {/* Action Links */}
            <div className="space-y-1 text-xs">
              <button
                onClick={() => {
                  setMenuOpen(false);
                  signOut();
                }}
                className="w-full flex items-center gap-2 px-3 py-2 rounded-lg text-rose-400 hover:bg-rose-950/40 hover:text-rose-300 transition-colors font-medium text-left cursor-pointer"
              >
                <span>🚪</span> Log Out
              </button>
            </div>
          </div>
        )}
      </div>

      <CommishCheckoutModal
        isOpen={checkoutModalOpen}
        onClose={() => setCheckoutModalOpen(false)}
      />
    </>
  );
}

