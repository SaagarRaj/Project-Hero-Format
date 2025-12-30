"use client";

import { useState } from "react";
import { Card } from "../components/ui/card";
import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";

export default function Page() {
  const [mappingFile, setMappingFile] = useState(null);
  const [templateFile, setTemplateFile] = useState(null);
  const [dataFiles, setDataFiles] = useState([]);
  const [status, setStatus] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [ownerName, setOwnerName] = useState("");
  const [migrationDate, setMigrationDate] = useState("");

  const backendUrl =
    process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";

  const handleSubmit = async (e) => {
    e.preventDefault();
    setStatus("");

    if (!mappingFile) {
      setStatus("Please upload a mapping file.");
      return;
    }
    if (!dataFiles || dataFiles.length === 0) {
      setStatus("Please upload at least one input file.");
      return;
    }
    if (!ownerName) {
      setStatus("Please enter an owner name.");
      return;
    }
    if (!migrationDate) {
      setStatus("Please enter a migration date.");
      return;
    }

    const formData = new FormData();
    formData.append("mapping", mappingFile);
    if (templateFile) {
      formData.append("template", templateFile);
    }
    Array.from(dataFiles).forEach((file) => {
      formData.append("files", file);
    });
    formData.append("owner_name", ownerName);
    formData.append("migration_date", migrationDate);

    setIsSubmitting(true);
    try {
      const response = await fetch(`${backendUrl}/process`, {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || "Failed to process files");
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "final_output.xlsx";
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      setStatus("Download started.");
    } catch (err) {
      setStatus(err.message);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center px-4 py-12">
      <Card>
        <div className="mb-6">
          <div className="inline-flex items-center gap-2 rounded-full border border-emerald-100 bg-emerald-50 px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-emerald-700">
            Hero-Format Output
          </div>
          <h1 className="mt-4 text-3xl font-semibold tracking-tight text-slate-900 md:text-4xl">
            Excel Mapping Tool
          </h1>
          <p className="mt-3 text-sm text-slate-600 md:text-base">
            Upload mapping, optional template, and input files to generate a
            standardized Hero-ready output.
          </p>
        </div>
        <form onSubmit={handleSubmit}>
          <div className="mb-5">
            <Label htmlFor="mapping">
              Mapping file <span className="text-rose-600">*</span>
            </Label>
            <Input
              id="mapping"
              type="file"
              accept=".xlsx,.xls"
              onChange={(e) => setMappingFile(e.target.files?.[0] ?? null)}
            />
          </div>

          <div className="grid gap-5 md:grid-cols-2">
            <div>
              <Label htmlFor="data">
                Input files (CSV or Excel, one or more){" "}
                <span className="text-rose-600">*</span>
              </Label>
              <Input
                id="data"
                type="file"
                multiple
                accept=".csv,.xlsx,.xls"
                onChange={(e) => setDataFiles(e.target.files ?? [])}
              />
              <div className="mt-2 text-xs text-slate-500">
                We will merge and normalize all provided files.
              </div>
            </div>
            <div>
              <Label htmlFor="template">Template file (optional)</Label>
              <Input
                id="template"
                type="file"
                accept=".xlsx,.xls"
                onChange={(e) => setTemplateFile(e.target.files?.[0] ?? null)}
              />
            </div>
          </div>

          <div className="mt-5 grid gap-5 md:grid-cols-2">
            <div>
              <Label htmlFor="data">
                Owner name <span className="text-rose-600">*</span>
              </Label>
              <Input
                id="data"
                type="text"
                onChange={(e) => setOwnerName(e.target.value)}
              />
              <div className="mt-2 text-xs text-slate-500">
                We will merge and normalize Owner name in the final output file.
              </div>
            </div>

            <div>
              <Label htmlFor="migration-date">
                Date of Migration <span className="text-rose-600">*</span>
              </Label>
              <Input
                id="migration-date"
                type="date"
                onChange={(e) => setMigrationDate(e.target.value)}
              />
            </div>
          </div>

          <div className="mt-6 flex flex-wrap items-center gap-3">
            <Button type="submit" disabled={isSubmitting}>
              {isSubmitting ? "Processing..." : "Process Files"}
            </Button>
            <span className="text-xs text-slate-500">
              Output will download automatically.
            </span>
          </div>
        </form>
        {status && (
          <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
            {status}
          </div>
        )}
      </Card>
    </div>
  );
}
