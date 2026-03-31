// @ts-nocheck
import { useEffect, useRef } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

interface MapPoint {
  lat: number;
  lng: number;
  label?: string;
}

export default function LeafletMap({ points }: { points: MapPoint[] }) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstance = useRef<L.Map | null>(null);

  useEffect(() => {
    if (!mapRef.current || mapInstance.current) return;

    // Calculate center from points
    const validPoints = points.filter(p => p.lat && p.lng && !isNaN(p.lat) && !isNaN(p.lng));
    const centerLat = validPoints.length > 0 ? validPoints.reduce((s, p) => s + p.lat, 0) / validPoints.length : 20;
    const centerLng = validPoints.length > 0 ? validPoints.reduce((s, p) => s + p.lng, 0) / validPoints.length : 0;

    const map = L.map(mapRef.current, {
      center: [centerLat, centerLng],
      zoom: validPoints.length > 50 ? 3 : validPoints.length > 10 ? 4 : 5,
      zoomControl: true,
      attributionControl: false,
    });

    // Use CartoDB light tiles (free, no API key needed)
    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
    }).addTo(map);

    // Add markers
    validPoints.forEach(p => {
      const marker = L.circleMarker([p.lat, p.lng], {
        radius: 6,
        fillColor: "#E8453C",
        color: "#fff",
        weight: 2,
        opacity: 1,
        fillOpacity: 0.8,
      }).addTo(map);

      if (p.label) {
        marker.bindPopup(`<div style="font-size:12px"><strong>${p.label}</strong><br/>${p.lat.toFixed(4)}, ${p.lng.toFixed(4)}</div>`);
      } else {
        marker.bindPopup(`<div style="font-size:12px">${p.lat.toFixed(4)}, ${p.lng.toFixed(4)}</div>`);
      }
    });

    // Fit bounds if we have points
    if (validPoints.length > 1) {
      const bounds = L.latLngBounds(validPoints.map(p => [p.lat, p.lng] as [number, number]));
      map.fitBounds(bounds, { padding: [30, 30] });
    }

    // Attribution
    L.control.attribution({ position: "bottomright", prefix: false })
      .addAttribution(`${validPoints.length} points`)
      .addTo(map);

    mapInstance.current = map;

    return () => {
      map.remove();
      mapInstance.current = null;
    };
  }, [points]);

  return <div ref={mapRef} className="h-full w-full rounded-lg" style={{ minHeight: 300 }} />;
}
