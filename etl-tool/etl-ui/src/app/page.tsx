import Link from 'next/link';

export default function HomePage() {
  return (
    <div className="container mx-auto p-4">
      <h1 className="text-3xl font-bold mb-6">ETL Management Dashboard</h1>
      <p className="mb-4">
        Welcome to the ETL Management UI. Use the navigation to manage your ETL jobs.
      </p>
      <div className="space-x-4">
        <Link href="/jobs" className="text-blue-500 hover:text-blue-700 underline">
          View Job Definitions
        </Link>
        {/* Add more links as other pages are created */}
      </div>
    </div>
  );
}
