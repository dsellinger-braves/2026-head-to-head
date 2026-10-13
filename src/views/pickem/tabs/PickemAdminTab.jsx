import React from 'react';

export default function PickemAdminTab({
  isCommissioner,
  user,
  profile,
  selectedSeason,
  newSeasonYear,
  setNewSeasonYear,
  handleBootstrapSeason,
  handleGradeSeason,
  saving,
  questions = [],
  gradingAnswers = {},
  setGradingAnswers
}) {
  if (!isCommissioner) {
    return (
      <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-10 text-center max-w-md mx-auto shadow-2xl space-y-4 my-8">
        <div className="w-16 h-16 rounded-2xl bg-amber-950/40 border border-amber-500/30 flex items-center justify-center text-3xl mx-auto shadow-inner">
          🔒
        </div>
        <div>
          <h3 className="text-lg font-bold text-white">Commissioner & Admin Access Only</h3>
          <p className="text-xs text-slate-400 mt-1 leading-relaxed">
            Official grading, answer resolution, and bootstrapping new Pick'em seasons are restricted to Commissioner (<strong>Adrian</strong>) and Admin (<strong>Dan</strong>).
          </p>
        </div>
        {user ? (
          <div className="pt-2 border-t border-slate-800 text-xs text-slate-400">
            Logged in as <span className="text-indigo-300 font-bold">{profile?.owner_name || user.email}</span> (Owner)
          </div>
        ) : (
          <div className="pt-2 border-t border-slate-800 text-xs text-slate-400">
            Please log in with Discord via the top navigation bar to verify commissioner permissions.
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* BOOTSTRAP FUTURE SEASON */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
        <h2 className="text-xl font-bold text-white flex items-center gap-2">
          <span>🚀</span> Bootstrap Upcoming Season
        </h2>
        <p className="text-xs text-slate-400 mt-1">
          Create a new Pick'em season with all 23 standard categories (Divisions, Wild Cards, Postseason, Awards, Win Totals) so league owners can start making picks.
        </p>

        <div className="mt-4 flex items-center gap-4">
          <div>
            <label className="text-xs font-bold text-slate-300 block mb-1">Season Year:</label>
            <input
              type="number"
              value={newSeasonYear}
              onChange={(e) => setNewSeasonYear(e.target.value)}
              className="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-white font-bold text-sm w-32 focus:ring-2 focus:ring-indigo-400"
            />
          </div>
          <div className="self-end">
            <button
              onClick={handleBootstrapSeason}
              disabled={saving}
              className="px-4 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold transition-all shadow cursor-pointer disabled:opacity-50"
            >
              Initialize Season {newSeasonYear} →
            </button>
          </div>
        </div>
      </div>

      {/* GRADE CURRENT SEASON */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-3 border-b border-slate-800 gap-2">
          <div>
            <h2 className="text-xl font-bold text-white flex items-center gap-2">
              <span>⚖️</span> Grade Season & Score Recalculator ({selectedSeason})
            </h2>
            <p className="text-xs text-slate-400 mt-1">
              Enter official MLB outcomes for each question. Saving will automatically score all owner picks, determine standings ranks, and award auction draft budget prizes.
            </p>
          </div>

          <button
            onClick={handleGradeSeason}
            disabled={saving}
            className="px-5 py-2.5 rounded-xl bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-500 hover:to-indigo-500 text-white font-bold text-xs shadow-lg transition-all flex items-center gap-2 cursor-pointer disabled:opacity-50"
          >
            {saving ? 'Grading...' : '💾 Calculate & Save All Scores'}
          </button>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-4">
          {questions.map(q => (
            <div key={q.id} className="bg-slate-950/60 border border-slate-800 rounded-xl p-3 space-y-1.5">
              <div className="flex items-center justify-between">
                <label className="text-xs font-bold text-slate-200">
                  {q.question_label}
                </label>
                <span className="text-[10px] text-slate-500 font-mono">{q.points_exact || 3} pts</span>
              </div>
              <input
                type="text"
                value={gradingAnswers[q.question_key] || ''}
                onChange={(e) => setGradingAnswers({ ...gradingAnswers, [q.question_key]: e.target.value })}
                placeholder="Actual MLB Outcome"
                className="w-full bg-slate-900 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-white placeholder-slate-600 focus:ring-2 focus:ring-purple-400"
              />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
