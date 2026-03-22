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

const BASEMAPS = {
  light: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
  dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
};

function getCurrentTheme(): 'light' | 'dark' {
  return (document.documentElement.getAttribute('data-theme') as 'light' | 'dark') || 'light';
}

function getHeatmapColors(theme: 'light' | 'dark'): [number, number, number][] {
  if (theme === 'dark') {
    return [
      [80, 40, 5],
      [140, 70, 8],
      [200, 120, 10],
      [212, 160, 23],
      [224, 176, 48],
      [255, 255, 255],
    ];
  }
  // Light: amber tones that read well on a white/light map
  return [
    [212, 160, 23, 40],
    [180, 120, 10],
    [160, 100, 8],
    [139, 105, 20],
    [120, 80, 10],
    [80, 40, 5],
  ] as [number, number, number][];
}

export function renderFlightMap(
  container: HTMLElement,
  hexData: HexData
): void {
  container.innerHTML = '';
  container.style.position = 'relative';
  container.style.height = '500px';

  if (window.innerWidth < 768) {
    container.style.height = '350px';
  }

  const theme = getCurrentTheme();

  const map = new maplibregl.Map({
    container,
    style: BASEMAPS[theme],
    center: [-81.69, 41.48],
    zoom: 11,
    interactive: true,
    attributionControl: false,
  });

  map.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');

  function createOverlay(t: 'light' | 'dark') {
    const heatmapLayer = new HeatmapLayer<HexEntry>({
      id: 'flight-heatmap',
      data: hexData.hexagons,
      getPosition: (d) => [d.lng, d.lat],
      getWeight: (d) => d.count,
      radiusPixels: 40,
      intensity: t === 'light' ? 1.2 : 1,
      threshold: 0.05,
      colorRange: getHeatmapColors(t),
    });

    return new MapboxOverlay({ layers: [heatmapLayer] });
  }

  let overlay = createOverlay(theme);
  map.addControl(overlay as unknown as maplibregl.IControl);

  // React to theme changes
  window.addEventListener('themechange', ((e: CustomEvent) => {
    const newTheme = e.detail.theme as 'light' | 'dark';

    // Swap basemap
    map.setStyle(BASEMAPS[newTheme]);

    // Re-add overlay after style loads
    map.once('styledata', () => {
      map.removeControl(overlay as unknown as maplibregl.IControl);
      overlay = createOverlay(newTheme);
      map.addControl(overlay as unknown as maplibregl.IControl);
    });
  }) as EventListener);
}
