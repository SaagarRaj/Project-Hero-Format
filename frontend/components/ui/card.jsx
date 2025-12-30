"use client";

export function Card({ children, className = "" }) {
  return (
    <div
      className={`relative w-full max-w-3xl rounded-3xl border-2 border-amber-200/80 bg-white/95 p-8 shadow-[0_18px_50px_rgba(15,23,42,0.12)] ring-1 ring-amber-100/60 transition hover:-translate-y-1 hover:border-amber-300 hover:shadow-[0_24px_70px_rgba(15,23,42,0.18)] md:p-10 ${className}`}
    >
      <div className="pointer-events-none absolute inset-0 -z-10 rounded-[28px] bg-[radial-gradient(circle_at_10%_10%,rgba(16,185,129,0.12),transparent_55%)]" />
      {children}
    </div>
  );
}
