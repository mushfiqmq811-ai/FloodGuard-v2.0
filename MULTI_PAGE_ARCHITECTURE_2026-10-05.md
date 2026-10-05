# FloodGuard BD — Multi-Page Architecture

FloodGuard BD now exposes clean page URLs while preserving the existing visual design and shared Flask/GloFAS architecture.

## Public pages
- `/` — Home / national overview
- `/monitor` — Live monitored zones
- `/map` — Bangladesh risk map
- `/forecast` — 72h / 7d / 15d forecast
- `/analytics` — cross-zone analytics
- `/how-it-works` — methodology and system explanation
- `/hazards` — hazard centre
- `/simulation` — scenario / simulation lab
- `/alerts` — account and alert centre

## Research page
- `/research` — Research Mode, ensemble analysis, historical replay and verification

## Architecture rule
These pages share the same frontend shell and the same Flask API. They are URL-addressable pages, not separate applications. GloFAS retrieval remains centralized and cached by the backend so navigating between pages does not create one EWDS request per page.

## Render Free design rule
Do not introduce a persistent background worker or a second web service just to support the pages. The existing bounded asynchronous GloFAS fetch/cache approach remains the source of truth.

## UI rule
No visual redesign was introduced in this build. Existing typography, spacing, cards, colors, responsive behavior and component styling remain the source of truth.
