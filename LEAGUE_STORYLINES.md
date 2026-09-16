# 📖 HEFTYSTRONG League Narrative & AI Recap Context Guide

This document establishes the **in-season trends, category battlegrounds, manager tendencies, and narrative patterns** that feed directly into the **Google Gemini AI daily and weekly recaps** posted to Discord.

---

## 1. How Context Injection Works

When daily or weekly recaps run:
1. **Real MLB News Ingestion**: The recap engine fetches breaking MLB news from ESPN and MLB.com RSS feeds and matches mentioned players against active fantasy rosters in league `130215`.
2. **Live Category Battleground Detection**: The pipeline dynamically inspects live standings and daily/weekly stat deltas to detect:
   - **Active Roto Point Dogfights**: Managers separated by razor-thin margins (e.g. within .003 OBP, 3 SB, 3 SV+HD, 0.15 ERA) where 1 roto point is actively on the line or flipping.
   - **Active Point Flips**: Direct category point trades between rival managers that happened today or this week.
   - **Standings Logjams**: Clusters of teams separated by ≤ 1.5 total roto points fighting for podium spots.
3. **Persistent League Context Synthesis**: It blends live battles with the season trends, category battlegrounds, and manager tendencies in `data/league_context.json` to generate spicy, contextual commissioner trash talk.

---

## 2. In-Season Manager Tendencies & Banter Profiles

| Manager | Team Name | In-Season Persona / Archetype | Signature In-Season Habits & Banter Tropes |
|:---|:---|:---|:---|
| **Dan** | Daniel's Squad | *The High-Volume Wire Hawk* | Leading the league in moves; 2:00 AM reliever claims; weekend pitcher streaming; hunting fractional counting stat edges. |
| **Adrian** | Adrian's Aces | *The Commissioner & Ratio Purist* | Defending pristine ERA/WHIP ratios; pacing starter innings; sweating bullpen meltdowns; enforcing trade deadlines strictly. |
| **Garrett** | Garrett's Sluggers | *The Pure Power Maximizer* | Stacking sluggers to dominate HR and RBI; riding sub-.315 OBP cold streaks without panic; Gunnar Henderson multi-HR days. |
| **Mark** | Mark's Maulers | *The Second-Half Surge Specialist* | Patient early-season pacing followed by explosive mid-summer charges; climbing 8-10 roto points in two weeks when the weather heats up. |
| **Anil** | Anil's Avengers | *The Speed Pacesetter & Dynast* | Untouchable double-digit lead in Stolen Bases behind Bobby Witt Jr.; rock-solid balanced floor across all 5 hitting categories. |
| **Tim** | Tim's Veterans | *The Steady Veteran Accumulator* | Dependable veteran rotation workhorses throwing 100+ pitches; quietly creeping into 1st place while others churn the wire. |
| **Will** | Will's Warriors | *The Contact & OBP Fundamentalist* | Premier OBP hawk; obsessing over walk rates and .001 OBP decimal margins; locking down saves with Josh Hader. |
| **Preston** | Preston's Bullpen | *The Relief Pitching Baron* | Hoarding 6+ setup/closer relievers to sweep SV+HD every week; powered by nuclear Aaron Judge HR explosions. |
| **Alex** | Alex's All-Stars | *The High-Variance Wildcard* | Aggressive prospect call-ups; massive scoring volatility (putting up 80 points one week and 25 the next); lightning in a bottle. |
| **Joe** | Joe's Juggernauts | *The Middle-Tier Disrupter* | Fierce middle-tier competitor; premium catcher production; disrupting playoff contenders with Sunday night stat upsets. |

---

## 3. Persistent Category Battlegrounds

1. **The Fractional OBP Decimal War (Will vs Garrett vs Adrian)**
   - *Theme*: Will's disciplined high-contact approach (.3306) vs Garrett's raw power (.3287) vs Adrian's balanced lineup (.3312).
   - *Dynamic*: Chronically separated by less than .002 in team OBP. A single 0-for-4 night or 3-walk Sunday night trades 1-2 roto points back and forth.
2. **The High-Leverage Bullpen Arms Race (Preston vs Will vs Tim)**
   - *Theme*: Preston's dedicated bullpen monopoly vs Will (Hader) vs Tim's veteran relievers.
   - *Dynamic*: Preston and Tim frequently deadlocked on saves and holds. Every blown save or 8th-inning hold swings a direct roto point at the top of the standings.
3. **The Stolen Base Turf War (Anil vs Dan vs Will)**
   - *Theme*: Anil's speed juggernaut vs Dan's wire speed streaming vs Will's opportunistic baserunners.
   - *Dynamic*: Single steals frequently flip 7th vs 8th place points in the final hours of a week.
4. **Pitching Volume vs Ratio Tightrope (Adrian vs Dan vs Garrett)**
   - *Theme*: Adrian's ace ratio preservation vs Dan's high-volume starter streaming.
   - *Dynamic*: Dan streams to capture K and QS volume at the risk of ERA/WHIP spikes; Adrian protects fragile ratios with premium starters.
5. **The Heavyweight Slugging Clash (Garrett vs Preston vs Mark)**
   - *Theme*: Garrett's power depth vs Preston's Aaron Judge engine vs Mark's summer hot streaks.

---

## 4. Season-Long Trends & Patterns

- **The Mid-Table Standings Logjam**: Teams ranked 3rd through 7th in the roto standings are chronically clustered within 2 to 4 total points of each other. A single category win on Sunday night regularly rearranges 3 or 4 standings positions simultaneously.
- **Sunday Night Decimal Volatility**: Rate categories (OBP, ERA, WHIP) and razor-thin counting stats (SB, SV+HD) experience dramatic swings during the Sunday night slate.
- **Summer Rotation Fatigue & Ratio Spikes**: As MLB starters fatigue in July and August, team ERAs climb across the league, rewarding teams with deep bullpens and innings management.
- **Streaming Volume vs Rate Erosion**: High-transaction managers routinely gain counting stat points in Strikeouts and Quality Starts through heavy streaming, while battling ratio erosion.
- **Second-Half Surges vs Early Fast Starters**: Fast spring starters must defend their cushion against second-half hot streaks from surging second-half managers.

---

## 5. How to Update & Edit Storylines

Feel free to edit `data/league_context.json` or this file directly whenever new category battles, manager habits, or seasonal patterns emerge! The pipeline reads from `data/league_context.json` automatically on every scheduled recap run.
