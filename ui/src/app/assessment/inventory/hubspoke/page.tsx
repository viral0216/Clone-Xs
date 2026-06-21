// @ts-nocheck
"use client";
import IframeView from "../IframeView";

export default function HubSpokeViewPage() {
  return (
    <IframeView
      viewKey="hubspoke"
      title="Hub & Spoke View"
      description="Radial drill-down with breadcrumb navigation and a jump-to picker for large catalogs."
    />
  );
}
