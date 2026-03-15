// @ts-nocheck
"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useDiff, useValidate } from "@/hooks/useApi";
import { GitCompare, CheckCircle, XCircle } from "lucide-react";

export default function DiffPage() {
  const [source, setSource] = useState("");
  const [dest, setDest] = useState("");

  const diff = useDiff();
  const validate = useValidate();

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Diff & Compare</h1>
        <p className="text-gray-500 mt-1">Compare catalogs and validate clones</p>
      </div>

      {/* Input */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Source Catalog</label>
              <Input value={source} onChange={(e) => setSource(e.target.value)} placeholder="production" />
            </div>
            <div className="flex-1">
              <label className="text-sm font-medium">Destination Catalog</label>
              <Input value={dest} onChange={(e) => setDest(e.target.value)} placeholder="staging" />
            </div>
            <Button
              onClick={() => diff.mutate({ source_catalog: source, destination_catalog: dest })}
              disabled={!source || !dest || diff.isPending}
            >
              <GitCompare className="h-4 w-4 mr-2" />
              Diff
            </Button>
            <Button
              variant="outline"
              onClick={() => validate.mutate({ source_catalog: source, destination_catalog: dest })}
              disabled={!source || !dest || validate.isPending}
            >
              <CheckCircle className="h-4 w-4 mr-2" />
              Validate
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Diff Results */}
      {diff.data && (
        <Card>
          <CardHeader>
            <CardTitle>Diff Results</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="bg-gray-100 p-4 rounded text-sm overflow-auto max-h-[500px]">
              {JSON.stringify(diff.data, null, 2)}
            </pre>
          </CardContent>
        </Card>
      )}

      {/* Validate Results */}
      {validate.data && (
        <Card>
          <CardHeader>
            <CardTitle>Validation Results</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="bg-gray-100 p-4 rounded text-sm overflow-auto max-h-[500px]">
              {JSON.stringify(validate.data, null, 2)}
            </pre>
          </CardContent>
        </Card>
      )}

      {diff.isError && (
        <Card className="border-red-200">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />
            {(diff.error as Error).message}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
