// @ts-nocheck
"use client";
import IframeView from "../IframeView";

export default function TreeViewPage() {
  return (
    <IframeView
      viewKey="tree"
      title="Tree View"
      description="Collapsible catalog tree with search, type filters, owner filters, and column-level detail panels."
    />
  );
}
