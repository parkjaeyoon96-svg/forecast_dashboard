import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "월손익 예상 대시보드",
  description: "브랜드별 월말 손익 예상 시스템",
  openGraph: {
    title: "월손익 예상 대시보드",
    description: "브랜드별 월말 손익 예상 시스템",
    url: "https://forecast-dashboard-six.vercel.app/dashboard",
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

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return children;
}

