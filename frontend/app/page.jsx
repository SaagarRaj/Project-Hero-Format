"use client";

import { useEffect, useRef, useState } from "react";
import { Card } from "../components/ui/card";
import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";

export default function Page() {
  const todayDate = new Date().toISOString().slice(0, 10);
  const [selectedMappingType, setSelectedMappingType] = useState("");
  const [mappingFile, setMappingFile] = useState(null);
  const [mappingFileSource, setMappingFileSource] = useState("");
  const [templateFile, setTemplateFile] = useState(null);
  const [dataFiles, setDataFiles] = useState([]);
  const [status, setStatus] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [ownerName, setOwnerName] = useState("");
  const [migrationDate, setMigrationDate] = useState(todayDate);
  const [useLocalUpload, setUseLocalUpload] = useState(true);
  const [useDriveUpload, setUseDriveUpload] = useState(false);
  const [driveAccessToken, setDriveAccessToken] = useState("");
  const [isDriveReady, setIsDriveReady] = useState(false);
  const [isDriveConnecting, setIsDriveConnecting] = useState(false);
  const [driveError, setDriveError] = useState("");
  const [driveMappingFile, setDriveMappingFile] = useState(null);
  const [driveDataFiles, setDriveDataFiles] = useState([]);
  const tokenClientRef = useRef(null);

  const backendUrl =
    process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";
  const googleClientId = process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID;
  const googleApiKey = process.env.NEXT_PUBLIC_GOOGLE_API_KEY;

  const templateDownloadMap = {
    ESS: "/MappingTemplate/ESS/ess_mapping.xlsx",
    SSM: "/MappingTemplate/SSM/ssm_mapping.xlsx",
    StorEdge: "/MappingTemplate/StorEdge/storEdge_mapping.xlsx",
    SiteLink: "/MappingTemplate/SiteLink/siteLink_mapping.xlsx",
    Storage_Commander: "/MappingTemplate/storageCommander_mapping.xlsx",
    CUSTOM: "/MappingTemplate/General/mapping_template.xlsx",
  };

  const loadScript = (src) =>
    new Promise((resolve, reject) => {
      if (document.querySelector(`script[src="${src}"]`)) {
        resolve();
        return;
      }
      const script = document.createElement("script");
      script.src = src;
      script.async = true;
      script.onload = () => resolve();
      script.onerror = () => reject(new Error(`Failed to load ${src}`));
      document.body.appendChild(script);
    });

  useEffect(() => {
    if (!useDriveUpload || !googleClientId || !googleApiKey) {
      return;
    }
    let canceled = false;
    const initDrive = async () => {
      try {
        await loadScript("https://accounts.google.com/gsi/client");
        await loadScript("https://apis.google.com/js/api.js");
        if (window.gapi?.load) {
          await new Promise((resolve) => window.gapi.load("picker", resolve));
        }
        if (!canceled) {
          tokenClientRef.current =
            window.google?.accounts?.oauth2?.initTokenClient?.({
              client_id: googleClientId,
              scope: "https://www.googleapis.com/auth/drive.readonly",
              callback: (tokenResponse) => {
                setDriveAccessToken(tokenResponse.access_token || "");
                setIsDriveConnecting(false);
                setDriveError("");
              },
            }) || null;
          setIsDriveReady(true);
        }
      } catch (error) {
        if (!canceled) {
          setDriveError(error.message);
          setIsDriveConnecting(false);
        }
      }
    };
    initDrive();
    return () => {
      canceled = true;
    };
  }, [useDriveUpload, googleClientId, googleApiKey]);

  const handlePmsChange = (value) => {
    if (!value) {
      setSelectedMappingType("");
      setMappingFileSource("");
      setMappingFile(null);
      return;
    }
    setSelectedMappingType(value);
    setMappingFileSource("PREDEFINED");
    setMappingFile(null);
  };

  const handleCustomToggle = (checked) => {
    if (checked) {
      setSelectedMappingType("CUSTOM");
      setMappingFileSource("UPLOADED");
      setMappingFile(null);
      setDriveMappingFile(null);
    } else {
      setSelectedMappingType("");
      setMappingFileSource("");
      setMappingFile(null);
      setDriveMappingFile(null);
    }
  };

  const resolvePredefinedMapping = async (type) => {
    const mappingPath = templateDownloadMap[type];
    const response = await fetch(mappingPath);
    if (!response.ok) {
      throw new Error("Failed to load predefined mapping template.");
    }
    const blob = await response.blob();
    return new File([blob], mappingPath.split("/").pop() || "mapping.xlsx", {
      type:
        blob.type ||
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });
  };

  const ensureXlsxName = (name) => {
    if (!name) {
      return "drive_file.xlsx";
    }
    return name.toLowerCase().endsWith(".xlsx") ? name : `${name}.xlsx`;
  };

  const downloadDriveFile = async (fileMeta) => {
    const isSheet =
      fileMeta.mimeType === "application/vnd.google-apps.spreadsheet";
    const url = isSheet
      ? `https://www.googleapis.com/drive/v3/files/${fileMeta.id}/export?mimeType=application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
      : `https://www.googleapis.com/drive/v3/files/${fileMeta.id}?alt=media`;
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${driveAccessToken}`,
      },
    });
    if (!response.ok) {
      throw new Error(`Failed to download ${fileMeta.name}`);
    }
    const blob = await response.blob();
    const name = isSheet ? ensureXlsxName(fileMeta.name) : fileMeta.name;
    return new File([blob], name, {
      type:
        blob.type ||
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });
  };

  const requestDriveToken = () => {
    if (!tokenClientRef.current) {
      setDriveError("Drive is not ready yet. Please try again.");
      return;
    }
    setIsDriveConnecting(true);
    tokenClientRef.current.requestAccessToken({ prompt: "" });
  };

  const openDrivePicker = ({ allowMultiSelect, onPicked, title }) => {
    if (!driveAccessToken) {
      setStatus("Please connect Google Drive first.");
      return;
    }
    if (!window.google?.picker) {
      setStatus("Google Picker is not ready yet.");
      return;
    }
    const view = new window.google.picker.DocsView(
      window.google.picker.ViewId.DOCS
    )
      .setIncludeFolders(true)
      .setSelectFolderEnabled(false)
      .setMimeTypes(
        [
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          "application/vnd.ms-excel",
          "text/csv",
          "application/vnd.google-apps.spreadsheet",
        ].join(",")
      );
    const picker = new window.google.picker.PickerBuilder()
      .setTitle(title || "Select files")
      .setDeveloperKey(googleApiKey)
      .setOAuthToken(driveAccessToken)
      .addView(view)
      .setCallback((data) => {
        if (
          data.action === window.google.picker.Action.PICKED &&
          data.docs?.length
        ) {
          onPicked(
            data.docs.map((doc) => ({
              id: doc.id,
              name: doc.name,
              mimeType: doc.mimeType,
            }))
          );
        }
      });
    if (allowMultiSelect) {
      picker.enableFeature(window.google.picker.Feature.MULTISELECT_ENABLED);
    }
    picker.build().setVisible(true);
  };

  const handleDriveMappingPick = () => {
    openDrivePicker({
      allowMultiSelect: false,
      title: "Select Mapping file",
      onPicked: (docs) => {
        const [doc] = docs;
        if (doc) {
          setDriveMappingFile(doc);
          setMappingFile(null);
          setMappingFileSource("DRIVE");
          setSelectedMappingType("CUSTOM");
        }
      },
    });
  };

  const handleDriveDataPick = () => {
    openDrivePicker({
      allowMultiSelect: true,
      title: "Select Input files",
      onPicked: (docs) => {
        setDriveDataFiles(docs);
      },
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setStatus("");

    if (!selectedMappingType) {
      setStatus("Please select a mapping option.");
      return;
    }
    if (selectedMappingType === "CUSTOM" && !mappingFile && !driveMappingFile) {
      setStatus("Please upload a custom mapping file.");
      return;
    }
    if (
      (!dataFiles || dataFiles.length === 0) &&
      (!driveDataFiles || driveDataFiles.length === 0)
    ) {
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
    let mappingToSend = mappingFile;
    if (selectedMappingType !== "CUSTOM") {
      mappingToSend = await resolvePredefinedMapping(selectedMappingType);
    } else if (driveMappingFile) {
      mappingToSend = await downloadDriveFile(driveMappingFile);
    }
    formData.append("mapping", mappingToSend);
    formData.append("mappingType", selectedMappingType);
    formData.append("mappingFileSource", mappingFileSource);
    if (templateFile) {
      formData.append("template", templateFile);
    }
    Array.from(dataFiles).forEach((file) => {
      formData.append("files", file);
    });
    for (const fileMeta of driveDataFiles) {
      const driveFile = await downloadDriveFile(fileMeta);
      formData.append("files", driveFile);
    }
    formData.append("owner_name", ownerName);
    const formattedMigrationDate = migrationDate;
    formData.append("migration_date", formattedMigrationDate);

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
          <p className="mt-3 text-xs text-slate-600 md:text-sm">
            Upload mapping, optional template, and input files to generate a
            standardized Hero-Format ready output.
          </p>
        </div>
        <form onSubmit={handleSubmit}>
          <div className="mb-6 rounded-2xl border border-slate-200 bg-white/80 p-5">
            <h2 className="text-base font-semibold text-slate-900">
              Upload Sources
            </h2>
            <p className="mt-1 text-xs text-slate-600">
              Use Local Upload, Google Drive, or both to provide input files.
            </p>
            <div className="mt-4 flex flex-wrap gap-4">
              <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
                <input
                  type="checkbox"
                  checked={useLocalUpload}
                  onChange={(e) => setUseLocalUpload(e.target.checked)}
                  className="h-4 w-4 text-emerald-500"
                />
                Local Upload
              </label>
              <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
                <input
                  type="checkbox"
                  checked={useDriveUpload}
                  onChange={(e) => setUseDriveUpload(e.target.checked)}
                  className="h-4 w-4 text-emerald-500"
                />
                Google Drive
              </label>
            </div>
            {useDriveUpload && (!googleClientId || !googleApiKey) && (
              <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
                Missing Google Drive keys. Set NEXT_PUBLIC_GOOGLE_CLIENT_ID and
                NEXT_PUBLIC_GOOGLE_API_KEY.
              </div>
            )}
          </div>
          <div className="mb-6 rounded-2xl border border-amber-100 bg-amber-50/50 p-5">
            <h2 className="text-base font-semibold text-slate-900">
              Select Mapping File <span className="text-rose-600">*</span>
            </h2>
            <p className="mt-1 text-xs text-slate-600">
              Choose a predefined PMS mapping or upload a custom mapping file.
            </p>

            <div className="mt-4 grid gap-3 md:grid-cols-[1fr_auto] md:items-end">
              <div className="space-y-2">
                <Label className="text-xs uppercase tracking-[0.2em] text-slate-500">
                  Predefined PMS Mapping
                </Label>
                <select
                  className="w-full rounded-xl border border-slate-300/80 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm transition focus:border-emerald-500 focus:outline-none focus:ring-2 focus:ring-emerald-200"
                  value={
                    selectedMappingType !== "CUSTOM" ? selectedMappingType : ""
                  }
                  onChange={(e) => handlePmsChange(e.target.value)}
                  disabled={selectedMappingType === "CUSTOM"}
                >
                  <option value="">Select Property Management System</option>
                  <option value="ESS">ESS</option>
                  <option value="SSM">SSM</option>
                  <option value="StorEdge">StorEdge</option>
                  <option value="SiteLink">SiteLink</option>
                  <option value="Storage_Commander">Storage Commander</option>
                </select>
              </div>
              {selectedMappingType !== "CUSTOM" && selectedMappingType && (
                <a
                  className="inline-flex items-center justify-center rounded-lg border border-emerald-200 bg-white px-3 py-2 text-xs font-semibold text-emerald-700 transition hover:border-emerald-400"
                  href={templateDownloadMap[selectedMappingType]}
                  download
                >
                  Download Mapping Template
                </a>
              )}
            </div>

            <div className="mt-4 rounded-xl border border-slate-200 bg-white/70 px-4 py-3">
              <label className="flex items-center gap-3 text-sm font-medium text-slate-800">
                <input
                  type="checkbox"
                  checked={selectedMappingType === "CUSTOM"}
                  onChange={(e) => handleCustomToggle(e.target.checked)}
                  className="h-4 w-4 text-emerald-500"
                />
                Custom Mapping File
              </label>

              {selectedMappingType === "CUSTOM" && (
                <div className="mt-3 flex flex-wrap items-center gap-3">
                  <a
                    className="inline-flex items-center justify-center rounded-lg border border-emerald-200 bg-white px-3 py-1.5 text-xs font-semibold text-emerald-700 transition hover:border-emerald-400"
                    href={templateDownloadMap.CUSTOM}
                    download
                  >
                    Download Mapping Template
                  </a>
                  <div className="min-w-[220px] flex-1">
                    <Input
                      id="mapping"
                      type="file"
                      accept=".xlsx,.xls"
                      onChange={(e) =>
                        setMappingFile(e.target.files?.[0] ?? null)
                      }
                      className="bg-white"
                    />
                  </div>
                </div>
              )}
            </div>
          </div>

          {useLocalUpload && (
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
          )}

          {useDriveUpload && (
            <div className="mt-6 rounded-2xl border border-slate-200 bg-slate-50/60 p-5">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h2 className="text-base font-semibold text-slate-900">
                    Google Drive Files
                  </h2>
                  <p className="mt-1 text-xs text-slate-600">
                    Pick files directly from Drive (Sheets export to .xlsx).
                  </p>
                </div>
                <Button
                  type="button"
                  onClick={requestDriveToken}
                  disabled={isDriveConnecting || !isDriveReady}
                >
                  <span>{!driveAccessToken ? "Connect" : "Reconnect"}</span>
                  <img
                    src="/drive-favicon.png"
                    alt=""
                    className="ml-2 h-5 w-5 rounded-md bg-white p-0.5"
                  />
                </Button>
              </div>
              {driveError && (
                <div className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">
                  {driveError}
                </div>
              )}
              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <div className="rounded-xl border border-slate-200 bg-white px-4 py-3">
                  <Label className="text-xs uppercase tracking-[0.2em] text-slate-500">
                    Mapping File (Custom)
                  </Label>
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <Button
                      type="button"
                      onClick={handleDriveMappingPick}
                      disabled={!driveAccessToken}
                    >
                      Choose Mapping from Drive
                    </Button>
                    {driveMappingFile && (
                      <span className="text-xs text-slate-600">
                        {driveMappingFile.name}
                      </span>
                    )}
                  </div>
                </div>
                <div className="rounded-xl border border-slate-200 bg-white px-4 py-3">
                  <Label className="text-xs uppercase tracking-[0.2em] text-slate-500">
                    Input Files
                  </Label>
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <Button
                      type="button"
                      onClick={handleDriveDataPick}
                      disabled={!driveAccessToken}
                    >
                      Choose Input Files
                    </Button>
                    {driveDataFiles.length > 0 && (
                      <span className="text-xs text-slate-600">
                        {driveDataFiles.length} selected
                      </span>
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}

          <div className="mt-5 grid gap-5 md:grid-cols-2">
            <div>
              <Label htmlFor="data">
                Property name <span className="text-rose-600">*</span>
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
                value={migrationDate}
                onChange={(e) => setMigrationDate(e.target.value)}
              />
            </div>
          </div>

          <div className="mt-6 flex flex-wrap items-center gap-3">
            <Button
              type="submit"
              disabled={
                isSubmitting ||
                !selectedMappingType ||
                (selectedMappingType === "CUSTOM" &&
                  !mappingFile &&
                  !driveMappingFile)
              }
            >
              {isSubmitting ? "Processing..." : "Process Files"}
            </Button>
            <span className="text-xs text-slate-500">
              Output will download automatically.
            </span>
          </div>
          {(mappingFile ||
            driveMappingFile ||
            dataFiles.length > 0 ||
            driveDataFiles.length > 0) && (
            <div className="mt-6 rounded-2xl border border-slate-200 bg-white/80 px-5 py-4 text-xs text-slate-700">
              <div className="font-semibold text-slate-900">Selected Files</div>
              {selectedMappingType === "CUSTOM" && (
                <div className="mt-2">
                  <div className="text-[11px] uppercase tracking-[0.2em] text-slate-400">
                    Mapping
                  </div>
                  <div className="mt-1">
                    {driveMappingFile
                      ? `${driveMappingFile.name} (Drive)`
                      : mappingFile
                      ? `${mappingFile.name} (Local)`
                      : "None"}
                  </div>
                </div>
              )}
              <div className="mt-3">
                <div className="text-[11px] uppercase tracking-[0.2em] text-slate-400">
                  Inputs
                </div>
                {dataFiles.length === 0 && driveDataFiles.length === 0 ? (
                  <div className="mt-1">None</div>
                ) : (
                  <ul className="mt-2 space-y-1">
                    {Array.from(dataFiles || []).map((file) => (
                      <li key={`local-${file.name}`}>{file.name} (Local)</li>
                    ))}
                    {driveDataFiles.map((file) => (
                      <li key={`drive-${file.id}`}>{file.name} (Drive)</li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          )}
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
