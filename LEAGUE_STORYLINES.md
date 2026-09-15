# 📖 HEFTYSTRONG League Narrative & AI Recap Context Guide

This document establishes the **master league storylines, manager personas, historical rivalries, and lore** that feed directly into the **Google Gemini AI daily and weekly recaps** posted to Discord.

---

## 1. How Context Injection Works

When daily or weekly recaps run:
1. **Real MLB News Ingestion**: The recap engine fetches real-time breaking MLB news from ESPN and MLB.com RSS feeds.
2. **Roster Overlap Matching**: It cross-references players mentioned in MLB headlines against who rosters them in our league (`130215`).
3. **League Lore & Persona Synthesis**: It blends real-world headlines with the manager profiles and rivalries below to create custom, trash-talking banter.

---

## 2. Manager Personas & Banter Profiles

| Manager | Team Name | Persona / Archetype | Signature Banter & Tropes |
|:---|:---|:---|:---|
| **Dan** | Daniel's Squad | *The Waiver Wire Hawk* | Leading the league in moves; 2:00 AM reliever claims; traded MacKenzie Gore at pick 243 to Tim. |
| **Adrian** | Adrian's Aces | *The Commissioner & Ace Architect* | Pitching purist; enforces trade deadlines strictly; rotation perpetually flirting with the IL. |
| **Garrett** | Garrett's Sluggers | *The Pure Power Maximizer* | Leading in home runs and RBI; sub-.310 OBP; fiercely protective of Gunnar Henderson. |
| **Mark** | Mark's Maulers | *The Late-Season Specialist* | Historic August/September surges; executed the Bobby Witt Jr. blockbuster with Anil. |
| **Anil** | Anil's Avengers | *The Dynasty Builder* | Fleeced the 2025 draft by acquiring Bobby Witt Jr. and Nathan Eovaldi; boasts about stolen base leads. |
| **Tim** | Tim's Veterans | *The Draft Capital Capitalizer* | Elite draft pick trader; extracted MacKenzie Gore and Trea Turner; workhorse veteran pitching. |
| **Will** | Will's Warriors | *The Contact & OBP Fundamentalist* | Wins matchups on fractional OBP decimals; acquired Josh Hader to lock down saves. |
| **Preston** | Preston's Bullpen | *The Relief Pitching Baron* | Hoards 6+ setup/closer relievers to sweep SV+HD every week; powered by nuclear Aaron Judge HRs. |
| **Alex** | Alex's All-Stars | *The Upside Speculator* | Aggressive prospect drafter; capable of putting up 80 points one week and 25 the next. |
| **Joe** | Joe's Juggernauts | *The Draft Board Strategist* | High-volume pick trader; staunch defender of catcher premium value with Adley Rutschman. |

---

## 3. Active League Rivalries

1. **The Veteran Showdown: Dan vs Tim**
   - *Theme*: High-frequency waiver churning vs stoic draft patience.
   - *Lore*: Tim never lets Dan forget acquiring MacKenzie Gore at Pick #243 in 2024.
2. **The Commissioner Clashes: Adrian vs Preston**
   - *Theme*: Classic rotation workhorse aces vs radical relief pitching monopoly.
   - *Lore*: A referendum on whether starting pitching or bullpen streaming wins championships.
3. **The Witt-Carroll Blockbuster Rivalry: Mark vs Anil**
   - *Theme*: The aftermath of the 2025 franchise trade.
   - *Lore*: Mark sent Bobby Witt Jr. to Anil for Corbin Carroll and picks. Every head-to-head matchup is an emotional audit of who won the deal.
4. **The Category Grinders: Will vs Garrett**
   - *Theme*: Will's contact discipline and high OBP vs Garrett's raw home runs and power slugging.

---

## 4. Master Trade Lore

- **2024 Deal #1**: Dan traded Trea Turner and Pick #243 to Tim for Mike Trout, Pick #80, and $11 cash. Tim drafted MacKenzie Gore (181 K, 110.7 pts) at Pick 243 for an A+ grade. Dan drafted Yandy Diaz (81.2 pts) at Pick 80.
- **2024 Deal #3 & #5**: Adrian and Dan swapped Pick #64, which turned into Tyler Glasnow (135.1 pts, 168 K, 14 QS).
- **2025 Deal #1**: Mark traded Bobby Witt Jr. (186.0 pts) and Pick #235 to Anil for Corbin Carroll (222.8 pts) and Pick #52. Anil drafted Nathan Eovaldi at Pick 235 (182.5 pts).

---

## 5. How to Update & Edit Storylines

Feel free to edit `data/league_context.json` or this file directly whenever new league trades, banter, or nicknames emerge! The pipeline reads from `data/league_context.json` automatically on every scheduled recap run.
