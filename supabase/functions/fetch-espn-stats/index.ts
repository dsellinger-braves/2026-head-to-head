import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const LEAGUE_ID = 130215;
const TEAM_IDS = [1, 2, 3, 5, 6, 8, 9, 11, 12, 13,14];
const SEASON_START = new Date("2026-03-25T00:00:00Z");
const CONCURRENCY = 10;
const UPSERT_BATCH_SIZE = 100;

function getCurrentScoringPeriod(): number {
  const diffMs = Date.now() - SEASON_START.getTime();
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
  return Math.max(1, diffDays + 1);
}

interface PlayerRecord {
  team_id: number;
  scoring_period_id: number;
  player_id: number | null;
  on_team_id: number | null;
  full_name: string;
  lineup_slot_id: number | null;
  stats: Record<string, number>;
  fetched_at: string;
}

async function fetchTeamPeriod(
  teamId: number,
  period: number
): Promise<PlayerRecord[]> {
  const url =
    `https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/2026/segments/0/leagues/${LEAGUE_ID}` +
    `?forTeamId=${teamId}&scoringPeriodId=${period}&view=mRoster`;

  const res = await fetch(url);
  if (!res.ok) return [];

  const data = await res.json();
  if (!data.teams?.length) return [];

  const teamData = data.teams[0];
  if (!teamData.roster?.entries) return [];

  const records: PlayerRecord[] = [];
  const fetchedAt = new Date().toISOString();

  for (const player of teamData.roster.entries) {
    const poolEntry = player.playerPoolEntry ?? {};
    const details = poolEntry.player ?? {};
    const statsList: Array<Record<string, unknown>> = details.stats ?? [];

    let statsDict: Record<string, number> = {};
    for (const splitType of [3, 5]) {
      const match = statsList.find(
        (s) =>
          s.statSplitTypeId === splitType && s.scoringPeriodId === period
      );
      if (match?.stats) {
        statsDict = match.stats as Record<string, number>;
        break;
      }
    }

    records.push({
      team_id: teamId,
      scoring_period_id: period,
      player_id: details.id ?? null,
      on_team_id: details.onTeamId ?? null,
      full_name: details.fullName ?? "Unknown",
      lineup_slot_id: player.lineupSlotId ?? null,
      stats: statsDict,
      fetched_at: fetchedAt,
    });
  }

  return records;
}

async function runInBatches<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>
): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const batchResults = await Promise.all(batch.map(fn));
    results.push(...batchResults);
  }
  return results;
}

Deno.serve(async (_req) => {
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  const todayPeriod = getCurrentScoringPeriod();

  // Build all (teamId, period) combos
  const combos: [number, number][] = [];
  for (const teamId of TEAM_IDS) {
    for (let period = 1; period <= todayPeriod; period++) {
      combos.push([teamId, period]);
    }
  }

  // Fetch all combos in parallel batches
  const nestedRecords = await runInBatches(
    combos,
    CONCURRENCY,
    ([teamId, period]) => fetchTeamPeriod(teamId, period)
  );
  const allRecords = nestedRecords.flat();

  // Upsert in batches
  let upsertErrors = 0;
  for (let i = 0; i < allRecords.length; i += UPSERT_BATCH_SIZE) {
    const batch = allRecords.slice(i, i + UPSERT_BATCH_SIZE);
    const { error } = await supabase
      .from("player_daily_stats")
      .upsert(batch, { onConflict: "team_id,scoring_period_id,player_id" });
    if (error) {
      console.error("Upsert error:", error.message);
      upsertErrors++;
    }
  }

  return new Response(
    JSON.stringify({
      success: upsertErrors === 0,
      periods_fetched: todayPeriod,
      records_upserted: allRecords.length,
      upsert_errors: upsertErrors,
    }),
    { headers: { "Content-Type": "application/json" } }
  );
});
