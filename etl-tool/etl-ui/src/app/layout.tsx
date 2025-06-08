import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css' // Tailwind global styles

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'ETL Management UI',
  description: 'UI for managing ETL jobs',
}

import Navbar from '@/components/layout/Navbar'; // Import the Navbar

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={`${inter.className} flex flex-col min-h-screen`}>
        <Navbar /> {/* Add Navbar here */}
        <main className="flex-grow container mx-auto p-4"> {/* Adjust padding as needed */}
          {children}
        </main>
        <footer className="bg-gray-100 dark:bg-gray-800 text-center p-4 text-sm text-gray-600 dark:text-gray-400">
          ETL Management Tool © {new Date().getFullYear()}
        </footer>
      </body>
    </html>
  )
}
