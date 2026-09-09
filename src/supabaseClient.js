// src/supabaseClient.js
import { createClient } from '@supabase/supabase-js'

// Active project Supabase endpoint and public publishable key
const DEFAULT_SUPABASE_URL = 'https://wczdkcdqgtzlsbssogoz.supabase.co'
const DEFAULT_SUPABASE_ANON_KEY = 'sb_publishable_4V9eQ0UiBYL4MYQ9v7v_DA_G083Zxhz'

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL || DEFAULT_SUPABASE_URL
const supabaseKey = import.meta.env.VITE_SUPABASE_ANON_KEY || DEFAULT_SUPABASE_ANON_KEY

export const supabase = createClient(supabaseUrl, supabaseKey)