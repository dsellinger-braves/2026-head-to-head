// src/supabaseClient.js
import { createClient } from '@supabase/supabase-js';

// Active project Supabase endpoint and public anon key (HS256 JWT supported by both PostgREST and Edge Functions)
export const DEFAULT_SUPABASE_URL = 'https://wczdkcdqgtzlsbssogoz.supabase.co';
export const DEFAULT_SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndjemRrY2RxZ3R6bHNic3NvZ296Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0NzQxMjQsImV4cCI6MjA4NTA1MDEyNH0.wOwQg2oRj5Z_XWtpjvprr0moAiA-ZvCXfVfu_0rrw44';

const rawUrl = import.meta.env.VITE_SUPABASE_URL || DEFAULT_SUPABASE_URL;
const rawKey = import.meta.env.VITE_SUPABASE_ANON_KEY || DEFAULT_SUPABASE_ANON_KEY;

// Ensure service_role secret keys are never accidentally used as the public anon key
const supabaseKey = (rawKey && !rawKey.startsWith('sb_secret_')) ? rawKey : DEFAULT_SUPABASE_ANON_KEY;
export const supabaseUrl = rawUrl || DEFAULT_SUPABASE_URL;


export const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true,
  },
});