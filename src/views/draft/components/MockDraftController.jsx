/**
 * MockDraftController
 * Floating bottom toolbar for controlling mock draft simulation (Run/Pause, Step, Speed).
 */
export default function MockDraftController({
  isRunningMock,
  setIsRunningMock,
  stepMockPick,
  currentPick,
  currentUser,
  mockSpeed,
  setMockSpeed
}) {
  const isMyPick = currentPick?.Owner === currentUser;

  return (
    <div style={{
      position: 'fixed',
      bottom: '55px',
      left: '50%',
      transform: 'translateX(-50%)',
      display: 'flex',
      alignItems: 'center',
      gap: '10px',
      background: '#1a1a2e',
      border: '1px solid #bb86fc44',
      borderRadius: '12px',
      padding: '8px 18px',
      zIndex: 150,
      boxShadow: '0 4px 20px rgba(0,0,0,0.6)'
    }}>
      <span style={{ color: '#bb86fc', fontSize: '11px', fontWeight: 'bold', letterSpacing: '1px' }}>
        🤖 MOCK DRAFT
      </span>
      <div style={{ width: '1px', height: '20px', background: '#444' }} />
      <button
        onClick={() => setIsRunningMock(r => !r)}
        style={{
          background: isRunningMock ? '#f44336' : '#4caf50',
          color: '#fff',
          border: 'none',
          padding: '6px 16px',
          borderRadius: '6px',
          cursor: 'pointer',
          fontWeight: 'bold',
          fontSize: '12px'
        }}
      >
        {isRunningMock ? '⏸ PAUSE' : '▶ RUN'}
      </button>
      {!isRunningMock && (
        <button
          onClick={stepMockPick}
          disabled={isMyPick}
          title="Advance one pick"
          style={{
            background: isMyPick ? '#444' : '#ffc107',
            color: isMyPick ? '#666' : '#000',
            border: 'none',
            padding: '6px 14px',
            borderRadius: '6px',
            cursor: isMyPick ? 'not-allowed' : 'pointer',
            fontWeight: 'bold',
            fontSize: '12px'
          }}
        >
          ⏭ STEP
        </button>
      )}
      <div style={{ width: '1px', height: '20px', background: '#444' }} />
      <span style={{ color: '#888', fontSize: '11px' }}>Speed:</span>
      {[
        { label: '🐢', title: 'Slow (3s)', val: 3000 },
        { label: '⚡', title: 'Fast (1s)', val: 1000 },
        { label: '🚀', title: 'Turbo (300ms)', val: 300 }
      ].map(s => (
        <button
          key={s.val}
          onClick={() => setMockSpeed(s.val)}
          title={s.title}
          style={{
            background: mockSpeed === s.val ? '#bb86fc' : '#333',
            color: mockSpeed === s.val ? '#000' : '#ccc',
            border: 'none',
            padding: '5px 10px',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px'
          }}
        >
          {s.label}
        </button>
      ))}
      <div style={{ width: '1px', height: '20px', background: '#444' }} />
      {isMyPick ? (
        <span style={{ color: '#ffc107', fontWeight: 'bold', fontSize: '12px', animation: 'pulse 1s infinite' }}>
          ⭐ YOUR PICK!
        </span>
      ) : (
        <span style={{ color: '#888', fontSize: '12px' }}>
          On clock: <span style={{ color: '#03dac6', fontWeight: 'bold' }}>{currentPick?.Owner || '—'}</span>
        </span>
      )}
    </div>
  );
}
