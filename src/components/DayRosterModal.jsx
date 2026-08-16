import { useMemo } from 'react';
import { SCORING_CATS, aggregateStats, LINEUP_SLOTS } from '../utils/scoring';
import TeamAvatar from './TeamAvatar';

const BAT_CATS   = ['PA', 'R', 'HR', 'RBI', 'SB', 'OBP'];
const PITCH_CATS = ['IP', 'K', 'QS', 'SV+HDs', 'ER', 'ERA', 'WHIP'];

const BATTER_SLOT_ORDER  = { C: 0, '1B': 1, '2B': 2, '3B': 3, SS: 4, '1B/3B': 5, '2B/SS': 6, IF: 7, OF: 8, UTIL: 9, DH: 10 };
const PITCHER_SLOT_ORDER = { SP: 0, RP: 1, P: 2 };

function formatVal(val, cat) {
  const n = parseFloat(val);
  if (isNaN(n) || val === undefined || val === null) return '-';
  if (cat === 'IP')  return `${Math.floor(n)}.${Math.round((n % 1) * 3)}`;
  if (cat === 'ERA') return n.toFixed(2);
  if (cat === 'OBP') return n.toFixed(4).replace(/^0/, '');
  if (SCORING_CATS[cat]?.isRate) return n.toFixed(3).replace(/^0/, '');
  return Math.round(n);
}

export default function DayRosterModal({ teamDayRecord, highlightStat, onClose }) {
  const { teamId, teamName, date, records } = teamDayRecord;

  const { batters, pitchers, batterTotals, pitcherTotals } = useMemo(() => {
    const players = records.map(r => {
      const stats = aggregateStats([r]);
      const slot  = LINEUP_SLOTS[r.lineup_slot_id] || 'BN';
      return { id: r.player_id, name: r.full_name, slot, stats, _r: r };
    });

    const batterPlayers  = players.filter(p => (p.stats.PA || 0) > 0);
    const pitcherPlayers = players.filter(p => (p.stats.IP || 0) > 0);

    return {
      batters: [...batterPlayers].sort((a, b) =>
        (BATTER_SLOT_ORDER[a.slot]  ?? 99) - (BATTER_SLOT_ORDER[b.slot]  ?? 99)
      ),
      pitchers: [...pitcherPlayers].sort((a, b) =>
        (PITCHER_SLOT_ORDER[a.slot] ?? 99) - (PITCHER_SLOT_ORDER[b.slot] ?? 99)
      ),
      batterTotals:  aggregateStats(batterPlayers.map(p => p._r)),
      pitcherTotals: aggregateStats(pitcherPlayers.map(p => p._r)),
    };
  }, [records]);

  const ColHeader = ({ cat }) => (
    <th className={`px-3 py-2 text-center text-xs font-bold uppercase tracking-wider
      ${cat === highlightStat ? 'text-blue-600 bg-blue-50' : 'text-gray-500'}`}>
      {SCORING_CATS[cat]?.label || cat}
    </th>
  );

  const StatCell = ({ val, cat, bold }) => (
    <td className={`px-3 py-2 text-center font-mono text-sm
      ${cat === highlightStat ? 'text-blue-700 font-bold bg-blue-50/50' : bold ? 'text-gray-900 font-bold' : 'text-gray-700'}`}>
      {formatVal(val, cat)}
    </td>
  );

  const TotalRow = ({ totals, cats }) => (
    <tr className="bg-gray-50 border-t-2 border-gray-300">
      <td className="px-4 py-2 text-xs font-black text-gray-500 uppercase tracking-wider">Total</td>
      <td />
      {cats.map(c => <StatCell key={c} val={totals[c]} cat={c} bold />)}
    </tr>
  );

  return (
    <div
      className="fixed inset-0 bg-black/70 flex items-center justify-center z-[80] p-4 backdrop-blur-sm"
      onClick={e => e.target === e.currentTarget && onClose()}
    >
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-4xl max-h-[90vh] flex flex-col overflow-hidden">

        {/* HEADER */}
        <div className="bg-slate-900 text-white p-4 flex items-center justify-between shrink-0">
          <div className="flex items-center gap-4">
            <TeamAvatar team={{ id: teamId, name: teamName }} size="md" />
            <div>
              <h2 className="text-lg font-black">{teamName}</h2>
              <p className="text-slate-400 text-xs font-semibold uppercase tracking-wider">{date}</p>
            </div>
          </div>
          <button onClick={onClose} className="text-slate-400 hover:text-white text-3xl leading-none">&times;</button>
        </div>

        <div className="overflow-y-auto flex-1 bg-gray-50">

          {/* BATTERS */}
          {batters.length > 0 && (
            <div>
              <div className="bg-gray-100 px-4 py-2 text-xs font-bold text-gray-500 uppercase tracking-wider border-y border-gray-200">
                Batting
              </div>
              <table className="w-full text-sm border-collapse">
                <thead className="bg-white sticky top-0 shadow-sm z-10">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-bold text-gray-500 uppercase tracking-wider border-b border-gray-100">Player</th>
                    <th className="px-3 py-2 text-center text-xs font-bold text-gray-500 uppercase tracking-wider border-b border-gray-100">Slot</th>
                    {BAT_CATS.map(c => <ColHeader key={c} cat={c} />)}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 bg-white">
                  {batters.map(p => (
                    <tr key={p.id} className="hover:bg-blue-50/30 transition-colors">
                      <td className="px-4 py-2 font-semibold text-gray-900">{p.name}</td>
                      <td className="px-3 py-2 text-center text-xs text-gray-400">{p.slot}</td>
                      {BAT_CATS.map(c => <StatCell key={c} val={p.stats[c]} cat={c} />)}
                    </tr>
                  ))}
                  <TotalRow totals={batterTotals} cats={BAT_CATS} />
                </tbody>
              </table>
            </div>
          )}

          {/* PITCHERS */}
          {pitchers.length > 0 && (
            <div>
              <div className="bg-gray-100 px-4 py-2 text-xs font-bold text-gray-500 uppercase tracking-wider border-y border-gray-200">
                Pitching
              </div>
              <table className="w-full text-sm border-collapse">
                <thead className="bg-white sticky top-0 shadow-sm z-10">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-bold text-gray-500 uppercase tracking-wider border-b border-gray-100">Player</th>
                    <th className="px-3 py-2 text-center text-xs font-bold text-gray-500 uppercase tracking-wider border-b border-gray-100">Slot</th>
                    {PITCH_CATS.map(c => <ColHeader key={c} cat={c} />)}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 bg-white">
                  {pitchers.map(p => (
                    <tr key={p.id} className="hover:bg-blue-50/30 transition-colors">
                      <td className="px-4 py-2 font-semibold text-gray-900">{p.name}</td>
                      <td className="px-3 py-2 text-center text-xs text-gray-400">{p.slot}</td>
                      {PITCH_CATS.map(c => <StatCell key={c} val={p.stats[c]} cat={c} />)}
                    </tr>
                  ))}
                  <TotalRow totals={pitcherTotals} cats={PITCH_CATS} />
                </tbody>
              </table>
            </div>
          )}

          {batters.length === 0 && pitchers.length === 0 && (
            <div className="p-12 text-center text-gray-400 italic">No active player data for this day.</div>
          )}
        </div>
      </div>
    </div>
  );
}
