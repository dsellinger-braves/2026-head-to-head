// src/supabaseClient.js
import { createClient } from '@supabase/supabase-js';

// Active project Supabase endpoint and public publishable key
const DEFAULT_SUPABASE_URL = 'https://wczdkcdqgtzlsbssogoz.supabase.co';
const DEFAULT_SUPABASE_ANON_KEY = 'sb_publishable_4V9eQ0UiBYL4MYQ9v7v_DA_G083Zxhz';

const rawUrl = import.meta.env.VITE_SUPABASE_URL || DEFAULT_SUPABASE_URL;
const rawKey = import.meta.env.VITE_SUPABASE_ANON_KEY || DEFAULT_SUPABASE_ANON_KEY;

// Ensure service_role secret keys are never accidentally used as the public anon key
const supabaseKey = (rawKey && !rawKey.startsWith('sb_secret_')) ? rawKey : DEFAULT_SUPABASE_ANON_KEY;
const supabaseUrl = rawUrl || DEFAULT_SUPABASE_URL;

// Clear any stale supabase auth tokens from localStorage that trigger 401 JWT validation failures
if (typeof window !== 'undefined' && window.localStorage) {
  try {
    for (let i = window.localStorage.length - 1; i >= 0; i--) {
      const key = window.localStorage.key(i);
      if (key && (key.startsWith('sb-') || key.includes('supabase.auth.token'))) {
        window.localStorage.removeItem(key);
      }
    }
  } catch {
    // Ignore storage access errors
  }
}

export const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: {
    persistSession: false,
    autoRefreshToken: false,
    detectSessionInUrl: false,
  },
});