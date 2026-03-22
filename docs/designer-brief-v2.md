# Designer Brief v2: Cleveland Helicopter Watch — Redesign

## Context

We built a first version based on your initial designs. It works and the data is real, but the visual direction ended up too dramatic — dark theme, sharp edges, intelligence-briefing aesthetic. It reads as intense and potentially conspiratorial rather than credible and civic.

We need a redesign that a city council member, a local journalist, and a neighbor on Nextdoor would all take seriously. The data is compelling on its own — the design should get out of its way.

## The Problem With v1

- Dark theme reads as aggressive/edgy rather than trustworthy
- "Investigative thriller" tone oversells it — this is a neighbor with data, not WikiLeaks
- Dramatic typography and eyebrow labels like "NOCTURNAL BREACH" or tiny uppercase tracking feel pretentious
- The design is doing too much work trying to make you feel something — the numbers should do that

## New Direction: ProPublica / Marshall Project / City Observatory

Think local investigative journalism or civic data reporting. Reference sites:
- [ProPublica](https://www.propublica.org/) — clean, light, editorial, data-supported storytelling
- [The Marshall Project](https://www.themarshallproject.org/) — serious but human, long-form meets data
- [City Observatory](https://cityobservatory.org/) — civic data, accessible, not flashy
- [USAFacts](https://usafacts.org/) — government data made readable

### Key Characteristics
- **Light background** (white or warm off-white) — immediately reads as trustworthy, open, transparent
- **Serif headlines, sans-serif body** — editorial warmth without being stuffy
- **Generous white space** — let the content breathe. Don't crowd the page.
- **Data visualizations as the centerpiece** — charts and maps should be the hero, not giant text
- **Muted, purposeful color** — one accent color used sparingly for data highlights and interactive elements. No neon, no gradients.
- **Readable at every level** — a city council staffer skimming for 30 seconds and a journalist reading every word should both get what they need

## Site Name

**Cleveland Helicopter Watch**
- Domain: clehelicopterwatch.com
- Email: hello@clehelicopterwatch.com

## Tone of Voice

A neighbor who did the homework. Not angry, not academic, not political. Just: "I tracked this, here's what I found, and here's why it matters."

- Write at an 8th grade reading level
- Short sentences. No jargon.
- Let stats speak: "250 flights in 219 days" not "a pervasive pattern of nocturnal aerial deployment"
- Be direct but not combative

## Pages (4 total)

### 1. Dashboard (Landing Page)

The first thing anyone sees. Needs to communicate the story in under 10 seconds.

**Layout:**
- Clean header with site name and navigation
- Short intro sentence: "Cleveland police helicopters fly over your neighborhood almost every night. Here's the data."
- 4-5 key stat callouts (not giant hero cards — more like inline highlighted numbers):
  - $3.5M taxpayer money spent on refurbishment
  - 250 flights tracked in 219 days
  - 63% of flights happen after 7 PM
  - ~82 dB noise at ground level (WHO safe limit is 40 dB)
- Calendar heatmap showing daily flight activity (amber/orange scale on light background)
- Time-of-day bar chart showing the 7-10 PM concentration
- Flight path heat map on a clean map base (not dark satellite imagery — try a light/neutral map style)
- A "why this matters" section linking to the research

### 2. Flight Log ("Was that the police helicopter?")

- Simple, clean table of all tracked flights
- "Last night" section at the top showing the most recent flights
- Date, aircraft, start time, end time, duration
- All times in Eastern
- Sortable columns

### 3. The Research

Three sections, each with a summary paragraph and 4-6 citation cards in a grid:
1. **Health & Sleep** — WHO noise limits, cardiovascular studies, children's reading impacts
2. **Who Bears the Burden** — neighborhood disparities, children, low-income communities, surveillance
3. **Your Money, Their Noise** — $3.5M refurbishment, no evidence of crime reduction, cost comparisons

Citation cards should feel like references, not dramatic reveals. Title, one-sentence summary, source link.

### 4. Take Action

- Call 311 (featured prominently with a script of what to say)
- Contact your council member (link to ward finder)
- Share the data
- Get involved (email contact)

## Data We Have (Real Numbers)

These are the actual stats from our database — use these in mockups:

| Stat | Value |
|------|-------|
| Total flights | 250 |
| Tracking period | 219 days (Aug 2025 – Mar 2026) |
| Flights after 7 PM | 63% |
| Noise at ground level | ~82 dB |
| WHO nighttime safe limit | 40 dB |
| Median patrol altitude | 725 ft AGL |
| $3.5M | Refurbishment cost (two helicopters) |
| $167K–$334K | Estimated annual operating cost |
| Days with flights | 56% of all days |

## Design Constraints

- **Mobile-first** — most visitors will come from social media links on their phones
- **Static site** (Astro on Vercel) — no backend
- **Data updates daily** — design should accommodate growing numbers gracefully
- **Accessible** — WCAG AA contrast ratios, screen reader friendly
- **Shareable** — OG social card already exists, but individual charts should be easy to screenshot

## What's Different From v1

| v1 | v2 |
|----|-----|
| Dark background | Light/white background |
| Dramatic, sharp, angular | Clean, warm, editorial |
| Intelligence briefing aesthetic | Local journalism aesthetic |
| Design tries to make you feel something | Data makes you feel something |
| Tiny uppercase tracking labels | Normal readable labels |
| Aggressive accent gradients | Muted, purposeful accent color |

## What I Need

1. **4 page designs** (dashboard, flight log, research, take action)
2. **Light theme color palette** with one accent color
3. **Typography system** — serif headlines, sans-serif body
4. **Component designs** for: stat callouts, calendar heatmap, time chart, map container, citation cards, flight table, action cards
5. **Mobile layouts** for all pages
6. **Footer** with: contact email, data attribution, GitHub link
