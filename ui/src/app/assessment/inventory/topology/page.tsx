// @ts-nocheck
"use client";
import IframeView from "../IframeView";

export default function TopologyViewPage() {
  return (
    <IframeView
      viewKey="topology"
      title="Infrastructure Topology"
      description="Storage accounts, external locations, credentials, and connections — visualised as a topology graph."
    />
  );
}
