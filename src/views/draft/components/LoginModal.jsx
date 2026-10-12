import { useState } from 'react';
import styles from '../styles/draftRoomStyles';

export default function LoginModal({ owners, onLogin, roomSeason = 2027 }) {
  const [selectedOwner, setSelectedOwner] = useState(owners[0] || "");

  return (
    <div style={styles.loginOverlay}>
      <div style={styles.loginBox}>
        <h1 style={{ fontSize: '36px', marginBottom: '10px', color: 'var(--accent)' }}>
          ⚾ Hefty War Room {roomSeason}
        </h1>
        <p style={{ marginBottom: '30px', color: '#888' }}>Select your team to enter the draft</p>
        <div>
          <select 
            value={selectedOwner} 
            onChange={(e) => setSelectedOwner(e.target.value)}
            style={styles.loginSelect}
          >
            {owners.map(owner => (
              <option key={owner} value={owner}>{owner}</option>
            ))}
          </select>
        </div>
        <button 
          onClick={() => onLogin(selectedOwner)}
          style={styles.loginButton}
        >
          Enter Draft Room
        </button>
      </div>
    </div>
  );
}
