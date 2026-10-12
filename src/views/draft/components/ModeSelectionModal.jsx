export default function ModeSelectionModal({ onSelectMode, roomSeason = 2027, onSeasonChange }) {
  const modes = [
    {
      id: 'live',
      label: '🎯 Live Draft',
      desc: 'Sync with the real draft board. Your picks write to Supabase and update everyone in real time.',
      color: '#03dac6'
    },
    {
      id: 'multitest',
      label: '🤝 Multiplayer Test',
      desc: 'Shared board over Supabase. Multiple people can test together. Includes a Reset button to wipe all picks.',
      color: '#ff9800'
    },
    {
      id: 'mockdraft',
      label: '🤖 Mock Draft',
      desc: 'Fully local, no Supabase writes. The AI auto-picks for every other owner using their historical tendencies. Perfect for solo prep.',
      color: '#bb86fc'
    },
    {
      id: 'test',
      label: '🧪 Solo Test',
      desc: 'Local board, no Supabase writes. You pick for every slot yourself — useful for UI testing.',
      color: '#ffc107'
    },
    {
      id: 'mobile',
      label: '📱 Mobile',
      desc: 'Streamlined view for phones. Queue and draft from a small screen while away from your desk.',
      color: '#4caf50'
    },
    {
      id: 'host',
      label: '🎙️ Host / TV',
      desc: 'Big-screen broadcast view. Shows pick card, AI commentary, and countdown. Meant for a shared display.',
      color: '#cf6679'
    }
  ];

  return (
    <div style={{
      position: 'fixed',
      inset: 0,
      background: 'rgba(0,0,0,0.95)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 1000,
      padding: '20px'
    }}>
      <div style={{
        background: '#1e1e1e',
        borderRadius: '16px',
        padding: '36px 32px',
        textAlign: 'center',
        border: '2px solid #bb86fc',
        boxShadow: '0 20px 60px rgba(0,0,0,0.8)',
        width: '100%',
        maxWidth: '720px'
      }}>
        <div style={{ fontSize: '38px', marginBottom: '6px' }}>⚾</div>
        
        {/* Quick season toggle */}
        <div style={{ display: 'inline-flex', alignItems: 'center', gap: '8px', background: '#111', padding: '4px 10px', borderRadius: '20px', border: '1px solid #333', marginBottom: '14px' }}>
          <button
            onClick={() => onSeasonChange && onSeasonChange(2027)}
            style={{
              background: roomSeason === 2027 ? '#bb86fc' : 'transparent',
              color: roomSeason === 2027 ? '#000' : '#888',
              border: 'none',
              borderRadius: '14px',
              padding: '4px 12px',
              fontSize: '12px',
              fontWeight: 800,
              cursor: 'pointer'
            }}
          >
            🚀 2027 Draft (Active Prep)
          </button>
          <button
            onClick={() => onSeasonChange && onSeasonChange(2026)}
            style={{
              background: roomSeason === 2026 ? '#03dac6' : 'transparent',
              color: roomSeason === 2026 ? '#000' : '#888',
              border: 'none',
              borderRadius: '14px',
              padding: '4px 12px',
              fontSize: '12px',
              fontWeight: 800,
              cursor: 'pointer'
            }}
          >
            🏛️ 2026 Draft (Archive)
          </button>
        </div>

        <h1 style={{ fontSize: '28px', color: '#bb86fc', margin: '0 0 6px', fontWeight: 800, letterSpacing: '1px' }}>
          Hefty War Room {roomSeason}
        </h1>
        <p style={{ color: '#888', fontSize: '14px', margin: '0 0 28px' }}>
          {roomSeason === 2027 
            ? '2027 Draft Prep & War Room with 2026 trade continuity'
            : '2026 Official Completed Draft Archive & Historical Rosters'}
        </p>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '14px' }}>
          {modes.map(mode => (
            <button
              key={mode.id}
              onClick={() => onSelectMode(mode.id)}
              style={{
                background: `${mode.color}18`,
                border: `2px solid ${mode.color}55`,
                borderRadius: '12px',
                padding: '16px 18px',
                cursor: 'pointer',
                textAlign: 'left',
                transition: 'all 0.15s ease',
                color: '#e0e0e0'
              }}
              onMouseEnter={e => {
                e.currentTarget.style.background = `${mode.color}30`;
                e.currentTarget.style.borderColor = mode.color;
                e.currentTarget.style.transform = 'translateY(-2px)';
                e.currentTarget.style.boxShadow = `0 8px 24px ${mode.color}33`;
              }}
              onMouseLeave={e => {
                e.currentTarget.style.background = `${mode.color}18`;
                e.currentTarget.style.borderColor = `${mode.color}55`;
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = 'none';
              }}
            >
              <div style={{ fontSize: '15px', fontWeight: 700, color: mode.color, marginBottom: '6px' }}>
                {mode.label}
              </div>
              <div style={{ fontSize: '12px', color: '#999', lineHeight: '1.4' }}>
                {mode.desc}
              </div>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
