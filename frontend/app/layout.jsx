"use client";

import "./globals.css";

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body className='min-h-screen bg-[radial-gradient(circle_at_20%_20%,#fff4e6_0%,#fef7ed_45%,#f8fafc_100%)] font-["Space Grotesk"] text-slate-900'>
        {children}
      </body>
    </html>
  );
}
