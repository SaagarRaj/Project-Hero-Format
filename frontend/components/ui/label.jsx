"use client";

export function Label({ children, className = "", ...props }) {
  return (
    <label
      className={`mb-2 block text-sm font-semibold text-slate-800 ${className}`}
      {...props}
    >
      {children}
    </label>
  );
}
