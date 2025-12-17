import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "월손익 예상 대시보드",
  description: "브랜드별 월말 손익 예상 시스템",
  openGraph: {
    title: "월손익 예상 대시보드",
    description: "브랜드별 월말 손익 예상 시스템",
    url: "https://forecast-dashboard-six.vercel.app",
    siteName: "월손익 예상 대시보드",
    type: "website",
    locale: "ko_KR",
  },
  twitter: {
    card: "summary",
    title: "월손익 예상 대시보드",
    description: "브랜드별 월말 손익 예상 시스템",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        {children}
      </body>
    </html>
  );
}
