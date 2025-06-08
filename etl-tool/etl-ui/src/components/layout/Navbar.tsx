'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation'; // To highlight active link

export default function Navbar() {
  const pathname = usePathname();

  const navItems = [
    { href: '/', label: 'Home' },
    { href: '/jobs', label: 'Job Definitions' },
    // Add more links here as new pages/sections are created
    // { href: '/executions', label: 'Job Executions' },
    // { href: '/monitoring', label: 'Monitoring' },
  ];

  return (
    <nav className="bg-gray-800 text-white shadow-md">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          <div className="flex items-center">
            <Link href="/" className="text-xl font-bold hover:text-gray-300">
              ETL Tool
            </Link>
          </div>
          <div className="flex space-x-4">
            {navItems.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className={`px-3 py-2 rounded-md text-sm font-medium hover:bg-gray-700
                  ${pathname === item.href ? 'bg-gray-900' : ''}`}
              >
                {item.label}
              </Link>
            ))}
          </div>
        </div>
      </div>
    </nav>
  );
}
