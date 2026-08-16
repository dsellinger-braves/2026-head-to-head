// src/App.jsx
import { useEffect, useState, useMemo } from 'react';
import { supabase } from './supabaseClient';
import { LEAGUE_SCHEDULE, TEAMS, getPeriodRangeForWeek } from './schedule';
import { aggregateStats, calculateMatchupResult } from './utils/scoring';
import WeeklyView from './views/WeeklyView';
import SummaryView from './views/SummaryView';

function App() {
  const [loading, setLoading] = useState(true);
  const [currentView, setCurrentView] = useState('weekly'); // 'weekly' | 'summary'
  const [rawData, setRawData] = useState([]);

// 1. Fetch ALL data on load (with pagination)
  useEffect(() => {
    async function fetchAllData() {
      try {
        let allRecords = [];
        let from = 0;
        const step = 2000; // Fetch 2000 rows at a time
        let moreData = true;

        while (moreData) {
          const { data, error } = await supabase
            .from('player_daily_stats')
            .select('*')
            .range(from, from + step - 1); // Get the next chunk

          if (error) throw error;

          if (data.length > 0) {
            allRecords = [...allRecords, ...data];
            from += step;
          } else {
            // Stop when we get an empty array
            moreData = false;
          }
        }

        console.log(`Loaded ${allRecords.length} total records.`); // Check console to see the total count!
        setRawData(allRecords);
      } catch (error) {
        console.error("Error loading stats:", error);
      } finally {
        setLoading(false);
      }
    }
    fetchAllData();
  }, []);

  // 2. Process Data into "Weeks" structure
  // We memorize this so we don't recalculate on every view switch
  const processedWeeks = useMemo(() => {
    if (!rawData.length) return [];

    return LEAGUE_SCHEDULE.map(week => {
      // Get ID range for this week (e.g., 1 to 7)
      const { startId, endId } = getPeriodRangeForWeek(week);

      // Process matchups for this week
      const matchups = week.matchups.map(m => {
        // Filter raw data for Home and Away teams
        // MUST also filter by the week's ID range
        const homeRecords = rawData.filter(r => 
          r.team_id == m.homeTeamId && 
          r.scoring_period_id >= startId && 
          r.scoring_period_id <= endId
        );
        const awayRecords = rawData.filter(r => 
          r.team_id == m.awayTeamId && 
          r.scoring_period_id >= startId && 
          r.scoring_period_id <= endId
        );

        const homeStats = aggregateStats(homeRecords);
        const awayStats = aggregateStats(awayRecords);
        const result = calculateMatchupResult(homeStats, awayStats);

        return {
          matchupId: m.id,
          homeTeam: { ...TEAMS[m.homeTeamId], id: m.homeTeamId },
          awayTeam: { ...TEAMS[m.awayTeamId], id: m.awayTeamId },
          homeStats,
          awayStats,
          result,
          // Pass raw records down so we can do the "Roster View" later
          homeRecords, 
          awayRecords
        };
      });

      return { ...week, matchups };
    });
  }, [rawData]);

  if (loading) return <div className="min-h-screen flex items-center justify-center font-bold text-gray-500">Loading League Data...</div>;

  return (
    <div className="min-h-screen bg-gray-100 font-sans text-gray-800">
      {/* Navbar */}
      <nav className="bg-blue-900 text-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="font-black text-xl tracking-wider">FANTASY LEAGUE 2025</div>
            <div className="flex space-x-4">
              <button 
                onClick={() => setCurrentView('weekly')}
                className={`px-3 py-2 rounded-md text-sm font-medium ${currentView === 'weekly' ? 'bg-blue-700' : 'hover:bg-blue-800'}`}
              >
                Matchups
              </button>
              <button 
                onClick={() => setCurrentView('summary')}
                className={`px-3 py-2 rounded-md text-sm font-medium ${currentView === 'summary' ? 'bg-blue-700' : 'hover:bg-blue-800'}`}
              >
                Summary & Standings
              </button>
            </div>
          </div>
        </div>
      </nav>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
        {currentView === 'weekly' ? (
          <WeeklyView processedWeeks={processedWeeks} allStats={rawData} />
        ) : (
          <SummaryView processedWeeks={processedWeeks} allStats={rawData} />
        )}
      </main>
    </div>
  );
}

export default App;