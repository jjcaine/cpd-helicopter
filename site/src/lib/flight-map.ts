import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { MapboxOverlay } from '@deck.gl/mapbox';
import { HeatmapLayer } from '@deck.gl/aggregation-layers';

interface HexEntry {
  h3_index: string;
  count: number;
  lat: number;
  lng: number;
}

interface HexData {
  hexagons: HexEntry[];
}

export function renderFlightMap(
  container: HTMLElement,
  hexData: HexData
): void {
  // Clear placeholder content
  container.innerHTML = '';

  // Style the container for the map
  container.style.position = 'relative';
  container.style.background = '#121416';
  container.style.height = '500px';

  // Responsive: smaller height on mobile
  if (window.innerWidth < 768) {
    container.style.height = '350px';
  }

  const map = new maplibregl.Map({
    container,
    style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
    center: [-81.69, 41.48],
    zoom: 11,
    interactive: true,
    attributionControl: false,
  });

  // Add compact attribution in bottom-right
  map.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');

  const heatmapLayer = new HeatmapLayer<HexEntry>({
    id: 'flight-heatmap',
    data: hexData.hexagons,
    getPosition: (d) => [d.lng, d.lat],
    getWeight: (d) => d.count,
    radiusPixels: 40,
    intensity: 1,
    threshold: 0.05,
    colorRange: [
      [80, 40, 5],      // dark amber (transparent edge)
      [140, 70, 8],     // deeper amber
      [200, 120, 10],   // mid amber
      [245, 158, 11],   // #f59e0b primary
      [251, 191, 36],   // #fbbf24 lighter
      [255, 255, 255],  // white (hottest)
    ],
  });

  const overlay = new MapboxOverlay({
    layers: [heatmapLayer],
  });

  map.addControl(overlay as unknown as maplibregl.IControl);
}
