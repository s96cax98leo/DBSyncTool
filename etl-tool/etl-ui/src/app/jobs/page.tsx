'use client'; // This directive makes it a Client Component

import { useEffect, useState } from 'react';
import Link from 'next/link';
import apiClient from '@/lib/apiClient';
import { FiEdit, FiPlayCircle, FiTrash2, FiPlusCircle } from 'react-icons/fi'; // Example icons

// Simplified interface for displaying job configs. Add more fields as needed.
// Matches structure of common.model.EtlJobConfig and orchestration DTOs for now
interface DatabaseConnectionUIDisplay {
  connectionName?: string;
  jdbcUrl?: string;
  username?: string;
  driverClassName?: string;
  // additionalProperties might be too complex for a summary table
}

interface UiEtlJobConfig {
  jobId: string;
  jobName: string;
  sourceDbConfig?: DatabaseConnectionUIDisplay;
  targetDbConfig?: DatabaseConnectionUIDisplay;
  tablesToProcess?: string[];
  // tableTransformationConfigs: Map<String, JobTransformationConfig>; // Too complex for summary
}

export default function JobsPage() {
  const [jobs, setJobs] = useState<UiEtlJobConfig[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchJobs = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await apiClient.get<UiEtlJobConfig[]>('/jobs'); // Uses JobConfigResponse from backend DTO
        setJobs(response.data);
      } catch (err: any) {
        setError(err.response?.data?.message || err.message || 'Failed to fetch jobs.');
        console.error("Fetch jobs error:", err);
      } finally {
        setLoading(false);
      }
    };

    fetchJobs();
  }, []);

  const handleStartJob = async (jobId: string) => {
    try {
      const response = await apiClient.post(`/jobs/${jobId}/start`);
      alert(`Job ${jobId} started successfully. Execution ID: ${response.data.executionId}`);
      // Optionally, refresh job list or update status locally
    } catch (err: any) {
      alert(`Failed to start job ${jobId}: ${err.response?.data?.message || err.message}`);
    }
  };

  const handleDeleteJob = async (jobId: string) => {
    if(confirm(`Are you sure you want to delete job ${jobId}? This action cannot be undone.`)){
        try {
            // await apiClient.delete(`/jobs/${jobId}`); // Assuming a DELETE endpoint exists
            alert(`Job ${jobId} would be deleted. (DELETE endpoint not yet implemented).`);
            // setJobs(jobs.filter(job => job.jobId !== jobId)); // Update UI
        } catch (err:any) {
            alert(`Failed to delete job ${jobId}: ${err.response?.data?.message || err.message}`);
        }
    }
  };


  if (loading) {
    return <div className="text-center py-10">Loading job definitions...</div>;
  }

  if (error) {
    return <div className="text-center py-10 text-red-500">Error: {error}</div>;
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold">Job Definitions</h1>
        <Link href="/jobs/new" className="bg-blue-500 hover:bg-blue-600 text-white font-semibold py-2 px-4 rounded inline-flex items-center">
          <FiPlusCircle className="mr-2" /> Create New Job
        </Link>
      </div>

      {jobs.length === 0 ? (
        <p className="text-gray-600">No job definitions found. Get started by creating a new job.</p>
      ) : (
        <div className="shadow overflow-hidden border-b border-gray-200 sm:rounded-lg">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Job Name</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Job ID</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Source</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Target</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Tables</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {jobs.map((job) => (
                <tr key={job.jobId}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{job.jobName}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 font-mono">{job.jobId.substring(0,8)}...</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{job.sourceDbConfig?.connectionName || 'N/A'}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{job.targetDbConfig?.connectionName || 'N/A'}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{job.tablesToProcess?.join(', ') || 'N/A'}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium space-x-2">
                    <button onClick={() => alert(`View/Edit job ${job.jobId} (Not implemented)`)} className="text-indigo-600 hover:text-indigo-900" title="View/Edit">
                        <FiEdit />
                    </button>
                    <button onClick={() => handleStartJob(job.jobId)} className="text-green-600 hover:text-green-900" title="Start Job">
                        <FiPlayCircle />
                    </button>
                     <button onClick={() => handleDeleteJob(job.jobId)} className="text-red-600 hover:text-red-900" title="Delete Job">
                        <FiTrash2 />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
