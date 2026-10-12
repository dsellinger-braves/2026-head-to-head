export default function PlayerNameButton({ player, onClick, style = {} }) {
  return (
    <span
      onClick={(e) => {
        e.stopPropagation();
        onClick(player);
      }}
      style={{
        cursor: 'pointer',
        textDecoration: 'none',
        transition: 'opacity 0.2s',
        ...style
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.opacity = '0.8';
        e.currentTarget.style.textDecoration = 'underline';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.opacity = '1';
        e.currentTarget.style.textDecoration = 'none';
      }}
    >
      {player.Player}
    </span>
  );
}
