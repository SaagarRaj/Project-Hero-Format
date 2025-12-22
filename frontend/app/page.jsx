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

    const formData = new FormData();
    formData.append("mapping", mappingFile);
    if (templateFile) {
      formData.append("template", templateFile);
    }
    Array.from(dataFiles).forEach((file) => {
      formData.append("files", file);
    });
    formData.append("owner_name", ownerName);

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
    <div className="page-shell">
      <Card>
        <h1>Excel Normalizer</h1>
        <p>
          Upload your mapping, optional template, and data files to produce a
          standardized Excel output.
        </p>
        <form onSubmit={handleSubmit}>
          <div className="section">
            <Label htmlFor="mapping">Mapping file (required)</Label>
            <Input
              id="mapping"
              type="file"
              accept=".xlsx,.xls"
              onChange={(e) => setMappingFile(e.target.files?.[0] ?? null)}
            />
          </div>

          <div className="section">
            <Label htmlFor="template">Template file (optional)</Label>
            <Input
              id="template"
              type="file"
              accept=".xlsx,.xls"
              onChange={(e) => setTemplateFile(e.target.files?.[0] ?? null)}
            />
          </div>

          <div className="section">
            <Label htmlFor="data">
              Input files (CSV or Excel, one or more)
            </Label>
            <Input
              id="data"
              type="file"
              multiple
              accept=".csv,.xlsx,.xls"
              onChange={(e) => setDataFiles(e.target.files ?? [])}
            />
            <div className="files-hint">
              We will merge and normalize all provided files.
            </div>
          </div>

          <div className="section">
            <Label htmlFor="data">Owner name (required)</Label>
            <Input
              id="data"
              type="text"
              onChange={(e) => setOwnerName(e.target.value)}
            />
            <div className="files-hint">
              We will merge and normalize Owner name in the final output file.
            </div>
          </div>

          <Button type="submit" disabled={isSubmitting}>
            {isSubmitting ? "Processing..." : "Process Files"}
          </Button>
        </form>
        {status && <div className="status">{status}</div>}
      </Card>
    </div>
  );
}
