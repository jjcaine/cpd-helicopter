# Designer Brief: CPD Helicopter Tracker Public Dashboard

## What We're Building

A public-facing website that makes a data-driven case that Cleveland Police Department helicopter flights are a community nuisance. This is an advocacy tool — when someone visits the URL (a neighbor, a journalist, a city council member), they should immediately understand the scale of the problem and feel compelled to care.

The site is backed by real flight data collected automatically every day from public transponder tracking. We have detailed GPS telemetry, timestamps, durations, and flight paths for every CPD helicopter flight going back to August 2025 — and growing daily.

## Audience

1. **Cleveland residents** who hear the helicopter and want to understand the pattern ("Was that a helicopter last night? How often does this happen?")
2. **Journalists** looking for a story with data behind it
3. **City council members and officials** who need to see the fiscal and quality-of-life case
4. **Advocacy allies** (neighborhood associations, community orgs) who want shareable evidence

## Tone

Serious, credible, data-forward. Not tinfoil-hat conspiracy. Think investigative journalism meets civic data project. The data speaks for itself — the design should let it. Restrained use of color. Clean typography. Should feel trustworthy and authoritative, not angry (even though the people behind it are angry).

## Key Pages / Views

### 1. Landing Page / Dashboard

The first thing you see. Should communicate the core story in under 10 seconds.

**Hero section:**
- 3-4 large stat callouts, e.g.:
  - "374 hours of helicopter noise over Cleveland in 7 months"
  - "82 nights with helicopters circling past 9 PM"
  - "Estimated $XXX,XXX cost to taxpayers"
  - "0 evidence helicopter patrols reduce crime"
- Brief 1-2 sentence explanation of what this project is

**Calendar heatmap:**
- Every day since tracking began, colored by total hours of helicopter activity
- Should make the relentlessness immediately visceral — "they fly almost every day"
- Hovering/clicking a day could show flight count and times

**Time-of-day chart:**
- Hourly distribution of when flights happen
- Needs to visually emphasize the 7-10 PM concentration
- Overlay a "sleep hours" or "quiet hours" zone (maybe 9 PM - 7 AM shaded)
- The message: this is a nighttime operation

**Flight path heat map:**
- Map of Cleveland with GPS telemetry overlaid
- Heat map showing which neighborhoods get the most helicopter activity
- This is probably the most visually striking element — hundreds of thousands of GPS points showing circular patrol patterns over specific neighborhoods

### 2. Recent Activity / "Was That a Helicopter?"

- List of recent flights with start time, end time, duration
- Simple search/filter by date so residents can look up specific nights
- Possibly a "last night" quick view

### 3. The Cost

- Running cost estimate with transparent assumptions
- Comparison to what else that money could fund (potholes fixed, teachers hired, etc.)
- The $3.5M refurbishment context

### 4. The Research

- Plain-language summaries of the health, learning, and environmental justice research
- Organized by the three messaging pillars:
  - Community disruption / health / sleep
  - Fiscal waste / ineffectiveness
  - Environmental justice / disproportionate impact
- Links to actual studies for credibility
- Not an academic paper — short, scannable, persuasive

### 5. Take Action (future)

- How to file noise complaints
- Who to contact (council members)
- How to get involved
- This page may come later once the coalition strategy is clearer

## Data Available

The site can read from Parquet files hosted on GitHub (updated daily by automation):

- **flights.parquet**: flight ID, aircraft ICAO code, start time, end time, duration, telemetry point count
- **telemetry.parquet**: flight ID, latitude, longitude, altitude, ground speed, timestamp, aircraft ICAO code (~800K+ data points and growing)

No database access or API needed — the site can be fully static, reading Parquet client-side (e.g., with DuckDB-WASM or Apache Arrow JS) or at build time.

## Design Considerations

- **Mobile-first** — residents will share this link on social media and open it on their phones
- **Fast loading** — the telemetry dataset is ~8MB+. The map visualization may need to load on interaction rather than page load. Consider progressive loading.
- **Shareable** — each visualization should be easy to screenshot or share. Consider OG image / social card generation for link previews.
- **Accessible** — high contrast, screen reader friendly, color choices that work for colorblind users
- **Updateable** — data refreshes daily. Design should accommodate growing dataset gracefully.

## Branding

TBD — needs a name and identity. Some directions to explore:
- Something that evokes the noise/disruption without being too aggressive
- Could reference Cleveland directly
- Should work as a domain name
- Needs to feel civic/credible, not like a personal grudge

## Technical Constraints

- Static hosting preferred (GitHub Pages, Cloudflare Pages, Vercel, Netlify)
- Data comes from Parquet files on GitHub — no backend/API to build
- Framework is flexible — designer/developer can recommend
- Map visualization will need a mapping library (Mapbox, deck.gl, Leaflet)

## What I Need From You

1. **Visual direction / mood board** — what does this feel like?
2. **Layout and information hierarchy** — how do we sequence the story?
3. **Branding exploration** — name, color palette, typography, logo/mark
4. **Component designs** for the key visualizations (calendar, time chart, map, stat cards)
5. **Mobile responsive approach**
6. **Recommendations** on anything I'm missing or getting wrong
