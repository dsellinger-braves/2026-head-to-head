import React from 'react';
import { MLB_TEAMS, LEAGUE_OWNERS, PROMINENT_AWARD_CANDIDATES } from '../../../utils/mlbTeams';

export default function PickemEntryTab({
  selectedSeason,
  entryOwner,
  setEntryOwner,
  entryPicks,
  handlePickChange,
  handleSavePicks,
  saving,
  isCommissioner,
  user,
  effectiveTeamId,
  governanceTitle = 'Commissioner',
  questions = []
}) {
  return (
    <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-6">
      <div className="border-b border-slate-800 pb-4 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <span>✍️</span> Submit & Edit Picks for {selectedSeason}
          </h2>
          <p className="text-xs text-slate-400 mt-1">
            Select your owner profile and make your official predictions for the season. You can revisit and update your picks anytime before the season lock!
          </p>
        </div>

        {/* Owner selector for entry */}
        <div className="flex items-center gap-2">
          <label className="text-xs font-bold text-slate-300">Owner:</label>
          {isCommissioner ? (
            <select
              value={entryOwner}
              onChange={(e) => setEntryOwner(e.target.value)}
              className="bg-slate-800 text-amber-300 font-bold text-sm px-3 py-1.5 rounded-lg border border-amber-500/60 focus:ring-2 focus:ring-amber-400 cursor-pointer shadow-sm"
            >
              {LEAGUE_OWNERS.map(o => (
                <option key={o.id} value={o.name}>
                  {o.name} (Team {o.id}) {o.id === 5 || o.id === 2 ? '👑' : ''}
                </option>
              ))}
            </select>
          ) : user ? (
            <div className="bg-slate-800 text-indigo-300 font-bold text-sm px-3 py-1.5 rounded-lg border border-indigo-700/60 flex items-center gap-2">
              <span>{entryOwner} (Team {effectiveTeamId})</span>
              <span className="text-[10px] text-emerald-400 font-semibold bg-emerald-950/60 px-1.5 py-0.5 rounded border border-emerald-500/30">Verified</span>
            </div>
          ) : (
            <select
              value={entryOwner}
              disabled
              className="bg-slate-800/50 text-slate-400 font-bold text-sm px-3 py-1.5 rounded-lg border border-slate-700 cursor-not-allowed"
            >
              <option value={entryOwner}>{entryOwner} (Log in to Submit)</option>
            </select>
          )}
        </div>
      </div>

      {/* Auth / Commissioner status alerts */}
      {!user && (
        <div className="bg-indigo-950/40 border border-indigo-500/40 rounded-xl p-3.5 flex items-center gap-3 text-indigo-200 text-xs shadow-sm">
          <span className="text-xl">🔒</span>
          <div>
            <span className="font-bold text-white">Discord Login Required:</span> Please log in with Discord via the top navigation bar to submit or save your official predictions for {selectedSeason}.
          </div>
        </div>
      )}

      {isCommissioner && (
        <div className="bg-amber-950/40 border border-amber-500/50 rounded-xl p-3 flex flex-col sm:flex-row sm:items-center justify-between gap-2 text-xs text-amber-200 shadow-sm">
          <div className="flex items-center gap-2">
            <span className="text-base">{governanceTitle === 'Commissioner' ? '👑' : '⚡'}</span>
            <span>
              <strong>{governanceTitle} Mode Active:</strong> You have {governanceTitle.toLowerCase()} privileges to submit or edit official picks on behalf of <strong>{entryOwner}</strong>.
            </span>
          </div>
          <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-bold border border-amber-500/40 uppercase tracking-wider text-[10px] w-fit">
            Admin Override Enabled
          </span>
        </div>
      )}

      <form onSubmit={handleSavePicks} className="space-y-8">
        {/* 1. DIVISION CHAMPIONS */}
        <div>
          <h3 className="text-base font-bold text-indigo-300 flex items-center gap-2 pb-2 border-b border-slate-800">
            <span>⚾</span> Division Champions (3 pts each)
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-3">
            {questions
              .filter(q => q.category === 'division')
              .map(q => {
                let divFilter = null;
                const key = q.question_key.toLowerCase();
                if (key.includes('al_east')) divFilter = 'AL East';
                if (key.includes('al_central')) divFilter = 'AL Central';
                if (key.includes('al_west')) divFilter = 'AL West';
                if (key.includes('nl_east')) divFilter = 'NL East';
                if (key.includes('nl_central')) divFilter = 'NL Central';
                if (key.includes('nl_west')) divFilter = 'NL West';

                const divTeams = divFilter
                  ? MLB_TEAMS.filter(t => t.division === divFilter)
                  : MLB_TEAMS;

                return (
                  <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-1.5">
                    <label className="text-xs font-bold text-slate-200 block">
                      {q.question_label}
                    </label>
                    <select
                      value={entryPicks[q.question_key] || ''}
                      onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                      className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-indigo-400 cursor-pointer"
                    >
                      <option value="">-- Select Team --</option>
                      {divTeams.map(t => (
                        <option key={t.code} value={t.name}>
                          {t.name}
                        </option>
                      ))}
                    </select>
                  </div>
                );
              })}
          </div>
        </div>

        {/* 2. WILD CARD TEAMS */}
        <div>
          <h3 className="text-base font-bold text-sky-300 flex items-center gap-2 pb-2 border-b border-slate-800">
            <span>🎫</span> Wild Card Qualifiers (2 pts each)
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-3">
            {questions
              .filter(q => q.category === 'wild_card')
              .map(q => {
                const isAL = q.question_key.toLowerCase().startsWith('al_');
                const leagueTeams = MLB_TEAMS.filter(t => (isAL ? t.league === 'AL' : t.league === 'NL'));

                return (
                  <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-1.5">
                    <label className="text-xs font-bold text-slate-200 block">
                      {q.question_label}
                    </label>
                    <select
                      value={entryPicks[q.question_key] || ''}
                      onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                      className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-sky-400 cursor-pointer"
                    >
                      <option value="">-- Select Wild Card --</option>
                      {leagueTeams.map(t => (
                        <option key={t.code} value={t.name}>
                          {t.name} ({t.division})
                        </option>
                      ))}
                    </select>
                  </div>
                );
              })}
          </div>
        </div>

        {/* 3. POSTSEASON CHAMPIONS */}
        <div>
          <h3 className="text-base font-bold text-amber-300 flex items-center gap-2 pb-2 border-b border-slate-800">
            <span>👑</span> Pennants & World Series (5 - 7 pts each)
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mt-3">
            {questions
              .filter(q => q.category === 'playoff_result')
              .map(q => {
                let pool = MLB_TEAMS;
                const key = q.question_key.toLowerCase();
                if (key.startsWith('al_')) pool = MLB_TEAMS.filter(t => t.league === 'AL');
                if (key.startsWith('nl_')) pool = MLB_TEAMS.filter(t => t.league === 'NL');

                return (
                  <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-1.5">
                    <label className="text-xs font-bold text-amber-200 block">
                      {q.question_label} ({q.points_exact} pts)
                    </label>
                    <select
                      value={entryPicks[q.question_key] || ''}
                      onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                      className="w-full bg-slate-900 border border-amber-800/60 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-amber-400 cursor-pointer"
                    >
                      <option value="">-- Select Champion --</option>
                      {pool.map(t => (
                        <option key={t.code} value={t.name}>
                          {t.name}
                        </option>
                      ))}
                    </select>
                  </div>
                );
              })}
          </div>
        </div>

        {/* 4. INDIVIDUAL AWARDS */}
        <div>
          <h3 className="text-base font-bold text-pink-300 flex items-center gap-2 pb-2 border-b border-slate-800">
            <span>🌟</span> Individual Awards (4 pts each)
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-3">
            {questions
              .filter(q => q.category === 'award')
              .map(q => {
                const suggestions = PROMINENT_AWARD_CANDIDATES[q.question_key] || [];

                return (
                  <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-2">
                    <label className="text-xs font-bold text-slate-200 block">
                      {q.question_label}
                    </label>
                    <input
                      type="text"
                      value={entryPicks[q.question_key] || ''}
                      onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                      placeholder="e.g. Player Name"
                      className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-pink-400"
                    />
                    {/* Suggestions pills */}
                    {suggestions.length > 0 && (
                      <div className="flex flex-wrap gap-1 mt-1">
                        {suggestions.slice(0, 4).map(name => (
                          <button
                            type="button"
                            key={name}
                            onClick={() => handlePickChange(q.question_key, name)}
                            className="text-[10px] px-1.5 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 transition-colors cursor-pointer"
                          >
                            + {name}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
          </div>
        </div>

        {/* 5. WIN / LOSS TOTALS */}
        <div>
          <h3 className="text-base font-bold text-emerald-300 flex items-center gap-2 pb-2 border-b border-slate-800">
            <span>📈</span> Extreme Team Win Totals (3 pts each)
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mt-3">
            {questions
              .filter(q => q.category === 'extremes')
              .map(q => (
                <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3.5 space-y-1.5">
                  <label className="text-xs font-bold text-slate-200 block">
                    {q.question_label}
                  </label>
                  <select
                    value={entryPicks[q.question_key] || ''}
                    onChange={(e) => handlePickChange(q.question_key, e.target.value)}
                    className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-emerald-400 cursor-pointer"
                  >
                    <option value="">-- Select Team --</option>
                    {MLB_TEAMS.map(t => (
                      <option key={t.code} value={t.name}>
                        {t.name}
                      </option>
                    ))}
                  </select>
                </div>
              ))}
          </div>
        </div>

        {/* SAVE BUTTON */}
        <div className="pt-4 border-t border-slate-800 flex items-center justify-between">
          <div className="text-xs text-slate-400">
            Saving as <span className="text-white font-bold">{entryOwner}</span> for season <span className="text-white font-bold">{selectedSeason}</span>.
          </div>
          <button
            type="submit"
            disabled={saving}
            className="px-6 py-3 rounded-xl bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white font-extrabold text-sm shadow-lg shadow-emerald-950/50 transition-all flex items-center gap-2 cursor-pointer disabled:opacity-50"
          >
            {saving ? (
              <>
                <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                <span>Saving Picks...</span>
              </>
            ) : (
              <>
                <span>💾</span>
                <span>Save & Lock In Picks</span>
              </>
            )}
          </button>
        </div>
      </form>
    </div>
  );
}
