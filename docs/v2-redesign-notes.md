# v2 Redesign Notes

## Direction

Shift from dark "investigative thriller" to light "civic data report." ProPublica / Marshall Project editorial feel. The data is compelling enough — the design should get out of the way.

## Key Changes

### Theme
- Light background (white/off-white) as default
- Support both light and dark mode (CSS `prefers-color-scheme` + manual toggle)
- Dark mode should be the current dark theme, toned down slightly

### Accent Color
- Current bright orange (#f59e0b) is too dramatic, especially on light backgrounds
- Options to try:
  - Muted amber/dark gold: #a67c00 or #b8860b
  - Slate blue: #4a6fa5
  - Deep teal: #0d7377
- Pick one that reads as "highlighted fact" not "emergency"

### Language / Tone
- Soften from advocacy to informative. We have an opinion but we're letting the data lead.
- Current: "They fly over your neighborhood almost every night." — a bit aggressive
- Better: "How often do Cleveland police helicopters fly? We've been tracking them since August 2025."
- Drop combative framing ("Make it stop.") in favor of empowering framing ("Here's what you can do.")
- Keep the facts punchy but let readers draw their own conclusions
- The research page stays factual — cite studies, don't editorialize them
- Take Action stays concrete and practical, not angry

### Specific Copy to Revisit
- Landing headline — less accusatory, more inviting curiosity
- Take Action headline ("Make it stop.") — too aggressive
- "Why this matters" section — good content, check tone
- Hero stats — the numbers speak for themselves, labels should be neutral
- Footer — needs to look better, maybe 3-column layout

### Technical Changes
- Light/dark CSS variables with `prefers-color-scheme` media query
- Manual toggle in nav (sun/moon icon)
- Map basemap: light style for light mode, dark style for dark mode
- Calendar heatmap: adjust color scale for both backgrounds
- Histogram: adjust bar color and sleep-hours shading for both themes
- Social card: may need a light version or a version that works on both

### What Stays the Same
- Astro framework, Vercel hosting, same build pipeline
- All data, charts, map, and page structure
- The 4-page structure (dashboard, flight log, research, take action)
- Citation content and links
- 311 instructions and council contact info

## Stitch v2 Project
- Project ID: 16847667462324667654
- Title: "Cleveland Helicopter Watch v2 — Light Editorial"
- Has generated a light theme design system with Newsreader + Inter, warm amber accent
- Screens may still be generating — check back
