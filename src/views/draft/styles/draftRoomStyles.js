// --- DRAFT ROOM STYLES ---
export const styles = {
  body: {
    backgroundColor: '#121212',
    color: '#e0e0e0',
    fontFamily: "'Roboto Condensed', sans-serif",
    margin: 0,
    height: '100vh',
    overflow: 'hidden',
    display: 'grid',
    gridTemplateColumns: '1fr 300px',
    gridTemplateRows: '70px 1fr minmax(140px, auto) 45px'
  },
  header: {
    gridColumn: '1 / -1',
    background: '#1f1f1f',
    borderBottom: '2px solid #bb86fc',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '0 25px',
    zIndex: 10
  },
  leagueLogo: {
    fontSize: '20px',
    fontWeight: 'bold',
    color: '#bb86fc',
    textTransform: 'uppercase',
    letterSpacing: '1px'
  },
  clock: {
    background: '#000',
    fontFamily: "'Orbitron', monospace",
    fontSize: '24px',
    padding: '4px 15px',
    border: '2px solid #333',
    borderRadius: '6px',
    userSelect: 'none',
    minWidth: '80px',
    textAlign: 'center'
  },
  warRoomToggle: {
    position: 'absolute',
    top: '80px',
    right: '320px',
    background: '#03dac6',
    color: '#000',
    border: 'none',
    padding: '8px 16px',
    borderRadius: '20px',
    cursor: 'pointer',
    fontWeight: 'bold',
    boxShadow: '0 4px 12px rgba(3, 218, 198, 0.4)',
    zIndex: 50,
    fontSize: '13px'
  },
  mainStage: {
    gridColumn: '1 / 2',
    gridRow: '2 / 3',
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    alignItems: 'center',
    position: 'relative',
    padding: '20px'
  },
  pickCard: {
    background: 'linear-gradient(145deg, #252525, #1a1a1a)',
    border: '1px solid #333',
    borderRadius: '12px',
    padding: '30px',
    textAlign: 'center',
    width: '88%',
    maxWidth: '850px',
    boxShadow: '0 20px 50px rgba(0,0,0,0.5)'
  },
  pickMeta: {
    fontSize: '16px',
    color: '#888',
    marginBottom: '8px',
    textTransform: 'uppercase',
    letterSpacing: '1px'
  },
  pickPlayer: {
    fontSize: '56px',
    fontWeight: '700',
    color: '#fff',
    margin: '8px 0',
    lineHeight: '1.1',
    textShadow: '0 4px 10px rgba(0,0,0,0.5)'
  },
  pickOwner: {
    fontSize: '26px',
    color: '#03dac6',
    fontWeight: 'bold'
  },
  aiBox: {
    marginTop: '20px',
    background: 'rgba(187, 134, 252, 0.08)',
    borderLeft: '4px solid #bb86fc',
    padding: '14px 20px',
    textAlign: 'left',
    fontSize: '16px',
    lineHeight: '1.5',
    fontStyle: 'italic',
    color: '#ccc',
    maxHeight: '150px',
    overflowY: 'auto'
  },
  sidebar: {
    gridColumn: '2 / 3',
    gridRow: '2 / 3',
    background: '#181818',
    borderLeft: '1px solid #333',
    overflowY: 'auto',
    padding: '15px'
  },
  deckItem: {
    background: '#222',
    marginBottom: '8px',
    padding: '10px',
    borderRadius: '4px',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center'
  },
  infoBar: {
    gridColumn: '1 / -1',
    gridRow: '3 / 4',
    display: 'grid',
    gridTemplateColumns: '1.2fr 1fr 1fr',
    gap: '12px',
    padding: '10px 20px',
    background: '#1a1a1a',
    borderTop: '1px solid #333'
  },
  infoWidget: {
    background: '#222',
    borderRadius: '8px',
    padding: '10px 14px',
    overflow: 'hidden'
  },
  infoWidgetTitle: {
    fontSize: '13px',
    fontWeight: 'bold',
    color: 'var(--highlight)',
    marginBottom: '8px',
    paddingBottom: '6px',
    borderBottom: '1px solid #333'
  },
  tickerContainer: {
    gridColumn: '1 / -1',
    gridRow: '4 / 5',
    background: '#bb86fc',
    color: '#000',
    overflow: 'hidden',
    display: 'flex',
    alignItems: 'center',
    whiteSpace: 'nowrap'
  },
  tickerContent: {
    display: 'inline-block',
    animation: 'scroll 40s linear infinite',
    paddingLeft: '100%',
    fontWeight: 'bold',
    fontSize: '16px'
  },
  tickerItem: {
    marginRight: '40px'
  },
  warRoomPanel: {
    display: 'grid',
    position: 'fixed',
    bottom: 0,
    left: 0,
    width: '100%',
    height: '82%',
    background: '#242424',
    borderTop: '4px solid #03dac6',
    zIndex: 100,
    padding: '16px 20px',
    boxSizing: 'border-box',
    gridTemplateRows: '40px minmax(0, 1fr)',
    gap: '15px',
    boxShadow: '0 -10px 30px rgba(0,0,0,0.8)'
  },
  panelNav: {
    display: 'flex',
    gap: '10px',
    borderBottom: '1px solid #444',
    paddingBottom: '10px'
  },
  navBtn: {
    background: '#333',
    color: '#ccc',
    border: 'none',
    padding: '6px 14px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontWeight: 'bold',
    fontSize: '13px'
  },
  navBtnActive: {
    background: '#03dac6',
    color: '#000',
    border: 'none',
    padding: '6px 14px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontWeight: 'bold',
    fontSize: '13px'
  },
  panelContent: {
    height: '100%',
    minHeight: 0,
    overflow: 'hidden',
    gridTemplateColumns: '3fr 1fr',
    gap: '15px',
    paddingRight: '5px'
  },
  wrColumn: {
    display: 'flex',
    flexDirection: 'column',
    background: '#1a1a1a',
    padding: '12px 16px',
    borderRadius: '8px',
    height: '100%',
    minHeight: 0,
    overflow: 'hidden'
  },
  wrHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: '10px',
    color: '#03dac6',
    fontSize: '16px',
    fontWeight: 'bold',
    borderBottom: '1px solid #444',
    paddingBottom: '6px',
    alignItems: 'center',
    flexWrap: 'wrap',
    gap: '10px'
  },
  poolTableContainer: {
    flexGrow: 1,
    minHeight: 0,
    overflowY: 'auto',
    overflowX: 'auto',
    marginTop: '6px'
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: '12px',
    minWidth: '100%'
  },
  tableHead: {
    position: 'sticky',
    top: 0,
    background: '#2c2c2c',
    color: '#03dac6',
    zIndex: 10
  },
  th: {
    padding: '8px 6px',
    textAlign: 'left',
    cursor: 'pointer',
    borderBottom: '2px solid #555',
    userSelect: 'none',
    whiteSpace: 'nowrap'
  },
  td: {
    padding: '6px 6px',
    borderBottom: '1px solid #333',
    color: '#ccc',
    whiteSpace: 'nowrap'
  },
  tableRow: {
    transition: 'background 0.2s'
  },
  btnDraft: {
    background: '#03dac6',
    border: 'none',
    color: '#000',
    fontWeight: 'bold',
    padding: '3px 7px',
    cursor: 'pointer',
    borderRadius: '4px',
    fontSize: '11px',
    marginRight: '5px'
  },
  btnStar: {
    background: 'none',
    border: 'none',
    color: '#666',
    cursor: 'pointer',
    fontSize: '14px'
  },
  btnStarActive: {
    background: 'none',
    border: 'none',
    color: 'gold',
    cursor: 'pointer',
    fontSize: '14px'
  },
  filterControl: {
    background: '#333',
    color: '#fff',
    border: '1px solid #555',
    padding: '4px 8px',
    borderRadius: '4px',
    fontSize: '12px'
  },
  rosterSelect: {
    background: '#333',
    color: '#fff',
    border: '1px solid #555',
    padding: '4px',
    width: '100%',
    borderRadius: '3px',
    cursor: 'pointer',
    fontSize: '12px'
  },
  queueItem: {
    padding: '8px',
    borderBottom: '1px solid #333',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    borderLeft: '3px solid #03dac6'
  },
  loginOverlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(0,0,0,0.92)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 1000
  },
  loginBox: {
    background: '#1e1e1e',
    padding: '50px 40px',
    borderRadius: '12px',
    textAlign: 'center',
    border: '2px solid #bb86fc',
    boxShadow: '0 20px 60px rgba(0,0,0,0.8)',
    maxWidth: '480px',
    width: '90%'
  },
  loginSelect: {
    fontSize: '16px',
    padding: '10px 14px',
    margin: '20px 0',
    width: '100%',
    borderRadius: '6px',
    border: '2px solid #444',
    background: '#333',
    color: '#fff'
  },
  loginButton: {
    fontSize: '16px',
    padding: '12px 24px',
    background: '#03dac6',
    color: '#000',
    border: 'none',
    borderRadius: '8px',
    cursor: 'pointer',
    fontWeight: 'bold',
    width: '100%'
  },
  modalOverlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(0, 0, 0, 0.85)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 2000,
    backdropFilter: 'blur(4px)'
  },
  modalContent: {
    background: '#1e1e1e',
    borderRadius: '12px',
    width: '95%',
    maxWidth: '1280px',
    maxHeight: '90vh',
    overflow: 'hidden',
    boxShadow: '0 20px 60px rgba(0,0,0,0.8)',
    border: '2px solid #bb86fc',
    display: 'flex',
    flexDirection: 'column'
  },
  modalHeader: {
    padding: '20px 25px',
    borderBottom: '2px solid #333',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    background: '#252525'
  },
  modalCloseBtn: {
    background: 'none',
    border: 'none',
    color: '#888',
    fontSize: '24px',
    cursor: 'pointer',
    padding: '0',
    width: '28px',
    height: '28px',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: '4px'
  },
  modalBody: {
    padding: '20px 25px',
    overflowY: 'auto',
    flexGrow: 1
  },
  modalSection: {
    marginBottom: '20px',
    padding: '14px',
    background: '#252525',
    borderRadius: '8px',
    borderLeft: '4px solid var(--accent)'
  },
  modalSectionTitle: {
    margin: '0 0 10px 0',
    color: 'var(--highlight)',
    fontSize: '15px',
    fontWeight: 'bold',
    textTransform: 'uppercase',
    letterSpacing: '1px'
  },
  newsItem: {
    padding: '10px 12px',
    marginBottom: '8px',
    background: '#1a1a1a',
    borderRadius: '6px',
    borderLeft: '3px solid #03dac6'
  },
  externalLink: {
    display: 'inline-block',
    padding: '8px 16px',
    background: '#03dac6',
    color: '#000',
    textDecoration: 'none',
    borderRadius: '6px',
    fontWeight: 'bold',
    fontSize: '13px',
    transition: 'all 0.2s'
  }
};

export default styles;
