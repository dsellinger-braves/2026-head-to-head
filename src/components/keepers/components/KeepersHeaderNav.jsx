/**
 * KeepersHeaderNav
 * Header bar for Keepers & Budgets panel including Season Toggle,
 * sub-tab buttons with active highlights, and official deadline banner.
 */
export default function KeepersHeaderNav({
  seasonYear,
  onSeasonYearChange,
  activeTab,
  onTabChange,
  keepersCount = 0,
  leagueSettings,
  isDeadlinePassed,
  isCommissioner,
  governanceTitle = 'Commissioner'
}) {
  const handleTabClick = (tabKey) => {
    if (onTabChange) {
      onTabChange(tabKey);
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '12px',
        background: '#161616',
        padding: '12px 16px',
        borderRadius: '8px',
        border: '1px solid #2a2a2a'
      }}>
        {/* Title & Season Switcher */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '14px', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '20px' }}>💎</span>
            <span style={{ fontSize: '18px', fontWeight: 'bold', color: '#fff' }}>
              Keepers & Budgets
            </span>
          </div>
          <div style={{ display: 'flex', background: '#202020', borderRadius: '8px', padding: '2px', border: '1px solid #333' }}>
            <button
              onClick={() => onSeasonYearChange && onSeasonYearChange(2027)}
              style={{
                background: seasonYear === 2027 ? '#03dac6' : 'rgba(255, 255, 255, 0.05)',
                color: seasonYear === 2027 ? '#000' : '#888',
                border: `1px solid ${seasonYear === 2027 ? '#03dac6' : '#333'}`,
                borderRadius: '6px',
                padding: '3px 8px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer',
                transition: 'all 0.15s ease'
              }}
            >
              🚀 2027 Prep
            </button>
            <button
              onClick={() => onSeasonYearChange && onSeasonYearChange(2026)}
              style={{
                background: seasonYear === 2026 ? '#bb86fc' : 'rgba(255, 255, 255, 0.05)',
                color: seasonYear === 2026 ? '#000' : '#888',
                border: `1px solid ${seasonYear === 2026 ? '#bb86fc' : '#333'}`,
                borderRadius: '6px',
                padding: '3px 8px',
                fontSize: '11px',
                fontWeight: 'bold',
                cursor: 'pointer',
                transition: 'all 0.15s ease'
              }}
            >
              🏛️ 2026 Archive
            </button>
          </div>
        </div>

        {/* Sub-Tab Buttons */}
        <div style={{ display: 'flex', background: '#111', borderRadius: '6px', padding: '3px', border: '1px solid #333', gap: '4px', flexWrap: 'wrap' }}>
          <button
            onClick={() => handleTabClick('matrix')}
            style={{
              background: activeTab === 'matrix' ? '#03dac6' : 'transparent',
              color: activeTab === 'matrix' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            📋 Keeper Summary
          </button>
          <button
            onClick={() => handleTabClick('rosters')}
            style={{
              background: activeTab === 'rosters' ? '#ffb74d' : 'transparent',
              color: activeTab === 'rosters' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            💎 Keeper Rosters ({keepersCount})
          </button>
          <button
            onClick={() => handleTabClick('simulator')}
            style={{
              background: activeTab === 'simulator' ? '#bb86fc' : 'transparent',
              color: activeTab === 'simulator' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            🎲 Comp Pick Submission
          </button>
          <button
            onClick={() => handleTabClick('planner')}
            style={{
              background: activeTab === 'planner' ? '#4caf50' : 'transparent',
              color: activeTab === 'planner' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            📋 Keeper Submission
          </button>
          <button
            onClick={() => handleTabClick('calculations')}
            style={{
              background: activeTab === 'calculations' ? '#00e676' : 'transparent',
              color: activeTab === 'calculations' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            🏷️ Keeper Prices
          </button>
          <button
            onClick={() => handleTabClick('settings')}
            style={{
              background: activeTab === 'settings' ? '#f59e0b' : 'transparent',
              color: activeTab === 'settings' ? '#000' : '#888',
              border: 'none',
              borderRadius: '4px',
              padding: '6px 12px',
              fontSize: '12px',
              fontWeight: 'bold',
              cursor: 'pointer',
              transition: 'all 0.15s ease'
            }}
          >
            👑 Commish Settings
          </button>
        </div>
      </div>

      {/* Official Keeper Deadline Notification Banner */}
      {leagueSettings?.keeper_lock_deadline && (
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '10px 16px',
          borderRadius: '8px',
          fontSize: '12px',
          background: isDeadlinePassed ? 'rgba(239, 68, 68, 0.12)' : 'rgba(59, 130, 246, 0.12)',
          border: isDeadlinePassed ? '1px solid rgba(239, 68, 68, 0.35)' : '1px solid rgba(59, 130, 246, 0.35)',
          color: isDeadlinePassed ? '#fca5a5' : '#93c5fd',
          flexWrap: 'wrap',
          gap: '8px'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ fontSize: '16px' }}>{isDeadlinePassed ? '🔒' : '⏰'}</span>
            <span>
              <strong>Official Keeper Deadline:</strong> {new Date(leagueSettings.keeper_lock_deadline).toLocaleString()}
              {isDeadlinePassed ? ' (Deadline has passed — keeper rosters are locked)' : ' (All keeper selections lock at this time)'}
            </span>
          </div>
          {isCommissioner && (
            <span style={{
              background: 'rgba(245, 158, 11, 0.2)',
              color: '#fbbf24',
              border: '1px solid rgba(245, 158, 11, 0.4)',
              padding: '2px 8px',
              borderRadius: '4px',
              fontSize: '10px',
              fontWeight: 'bold'
            }}>
              {governanceTitle === 'Commissioner' ? '👑' : '⚡'} {governanceTitle} Override Rights Active
            </span>
          )}
        </div>
      )}
    </div>
  );
}
