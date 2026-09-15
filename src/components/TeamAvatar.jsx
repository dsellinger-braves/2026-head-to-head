// src/components/TeamAvatar.jsx
import { TEAMS } from '../schedule';

export default function TeamAvatar({ team, teamId, size = 'md', className = '' }) {
  const resolvedTeam = team || (teamId !== undefined ? TEAMS[teamId] : null) || { id: 0, name: 'Team' };
  const effectiveId = typeof resolvedTeam.id === 'number' ? resolvedTeam.id : (teamId || 0);
  const effectiveName = resolvedTeam.name || resolvedTeam.owner || 'Team';

  // Generate a consistent color based on the team ID
  const colors = [
    'bg-red-600', 'bg-blue-600', 'bg-green-600', 'bg-yellow-500', 
    'bg-purple-600', 'bg-pink-600', 'bg-indigo-600', 'bg-teal-600'
  ];
  const colorClass = colors[Math.abs(effectiveId) % colors.length] || 'bg-gray-600';

  // Get initials (e.g., "Team One" -> "TO")
  const initials = String(effectiveName)
    .split(' ')
    .filter(Boolean)
    .map(n => n[0])
    .join('')
    .substring(0, 2)
    .toUpperCase() || 'T';

  const sizeClasses = {
    xs: 'w-6 h-6 text-[10px]',
    sm: 'w-8 h-8 text-xs',
    md: 'w-12 h-12 text-lg',
    lg: 'w-16 h-16 text-2xl'
  };

  return (
    <div className={`${sizeClasses[size] || sizeClasses.md} ${colorClass} rounded-full flex items-center justify-center text-white font-bold shadow-md border-2 border-white ${className}`}>
      {initials}
    </div>
  );
}