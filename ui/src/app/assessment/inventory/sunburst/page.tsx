// @ts-nocheck
"use client";
import IframeView from "../IframeView";

export default function SunburstViewPage() {
  return (
    <IframeView
      viewKey="sunburst"
      title="Sunburst View"
      description="Zoomable concentric ring chart — click any segment to drill in, click the center ring to zoom out."
    />
  );
}
