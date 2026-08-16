// src/components/TeamAvatar.jsx
export default function TeamAvatar({ team, size = 'md' }) {
  // Generate a consistent color based on the team ID
  const colors = [
    'bg-red-600', 'bg-blue-600', 'bg-green-600', 'bg-yellow-500', 
    'bg-purple-600', 'bg-pink-600', 'bg-indigo-600', 'bg-teal-600'
  ];
  const colorClass = colors[team.id % colors.length] || 'bg-gray-600';

  // Get initials (e.g., "Team One" -> "TO")
  const initials = team.name
    .split(' ')
    .map(n => n[0])
    .join('')
    .substring(0, 2)
    .toUpperCase();

  const sizeClasses = {
    sm: 'w-8 h-8 text-xs',
    md: 'w-12 h-12 text-lg',
    lg: 'w-16 h-16 text-2xl'
  };

  return (
    <div className={`${sizeClasses[size]} ${colorClass} rounded-full flex items-center justify-center text-white font-bold shadow-md border-2 border-white`}>
      {initials}
    </div>
  );
}