'use client'; // This directive makes it a Client Component

import { useState, FormEvent } from 'react';
import { useRouter } from 'next/navigation'; // For redirecting after form submission
import apiClient from '@/lib/apiClient';
import { DatabaseConnectionConfig, JobTransformationConfig } from '@/lib/models'; // Define these models based on common

// Define models locally for UI or import from a shared types location if available
// For now, mirroring the structure of common.model.DatabaseConnectionConfig
// and common.model.EtlJobConfig (specifically CreateJobRequest from orchestration)

interface CreateJobPayload {
  jobName: string;
  sourceDbConfig: DatabaseConnectionConfig;
  targetDbConfig: DatabaseConnectionConfig;
  tablesToProcess: string[];
  tableTransformationConfigs?: { [key: string]: JobTransformationConfig }; // Key is table name
}

export default function NewJobPage() {
  const router = useRouter();
  const [jobName, setJobName] = useState('');

  const [sourceConnName, setSourceConnName] = useState('');
  const [sourceJdbcUrl, setSourceJdbcUrl] = useState('');
  const [sourceDriver, setSourceDriver] = useState('');
  const [sourceUsername, setSourceUsername] = useState('');
  const [sourcePassword, setSourcePassword] = useState('');

  const [targetConnName, setTargetConnName] = useState('');
  const [targetJdbcUrl, setTargetJdbcUrl] = useState('');
  const [targetDriver, setTargetDriver] = useState('');
  const [targetUsername, setTargetUsername] = useState('');
  const [targetPassword, setTargetPassword] = useState('');

  const [tablesToProcessStr, setTablesToProcessStr] = useState('');

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSubmitting(true);
    setError(null);
    setSuccessMessage(null);

    const tables = tablesToProcessStr.split(',').map(t => t.trim()).filter(t => t.length > 0);
    if (tables.length === 0) {
        setError("Please provide at least one table to process.");
        setIsSubmitting(false);
        return;
    }

    const payload: CreateJobPayload = {
      jobName,
      sourceDbConfig: {
        connectionName: sourceConnName,
        jdbcUrl: sourceJdbcUrl,
        driverClassName: sourceDriver,
        username: sourceUsername,
        password: sourcePassword, // Handle password securely in real apps
      },
      targetDbConfig: {
        connectionName: targetConnName,
        jdbcUrl: targetJdbcUrl,
        driverClassName: targetDriver,
        username: targetUsername,
        password: targetPassword,
      },
      tablesToProcess: tables,
      tableTransformationConfigs: {}, // UI for this is complex, skipping for now
    };

    try {
      await apiClient.post('/jobs', payload); // Endpoint from Orchestration service
      setSuccessMessage(`Job "${jobName}" created successfully! Redirecting to jobs list...`);
      // Clear form or redirect
      setTimeout(() => {
        router.push('/jobs');
      }, 2000);
    } catch (err: any) {
      setError(err.response?.data?.message || err.message || 'Failed to create job.');
      console.error("Create job error:", err);
    } finally {
      setIsSubmitting(false);
    }
  };

  const renderDbConfigFields = (type: 'source' | 'target') => {
    const setters = type === 'source' ?
        { connName: setSourceConnName, jdbcUrl: setSourceJdbcUrl, driver: setSourceDriver, username: setSourceUsername, password: setSourcePassword } :
        { connName: setTargetConnName, jdbcUrl: setTargetJdbcUrl, driver: setTargetDriver, username: setTargetUsername, password: setTargetPassword };
    const values = type === 'source' ?
        { connName: sourceConnName, jdbcUrl: sourceJdbcUrl, driver: sourceDriver, username: sourceUsername, password: sourcePassword } :
        { connName: targetConnName, jdbcUrl: targetJdbcUrl, driver: targetDriver, username: targetUsername, password: targetPassword };

    return (
      <div className="space-y-4 p-4 border border-gray-300 rounded-md">
        <h3 className="text-lg font-semibold">{type === 'source' ? 'Source' : 'Target'} Database Configuration</h3>
        <div>
          <label htmlFor={`${type}ConnName`} className="block text-sm font-medium text-gray-700">Connection Name</label>
          <input type="text" name={`${type}ConnName`} id={`${type}ConnName`} required value={values.connName} onChange={e => setters.connName(e.target.value)}
                 className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm" />
        </div>
        <div>
          <label htmlFor={`${type}JdbcUrl`} className="block text-sm font-medium text-gray-700">JDBC URL</label>
          <input type="text" name={`${type}JdbcUrl`} id={`${type}JdbcUrl`} required value={values.jdbcUrl} onChange={e => setters.jdbcUrl(e.target.value)}
                 className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm" />
        </div>
         <div>
          <label htmlFor={`${type}Driver`} className="block text-sm font-medium text-gray-700">Driver Class Name (Optional)</label>
          <input type="text" name={`${type}Driver`} id={`${type}Driver`} value={values.driver} onChange={e => setters.driver(e.target.value)}
                 className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm" />
        </div>
        <div>
          <label htmlFor={`${type}Username`} className="block text-sm font-medium text-gray-700">Username</label>
          <input type="text" name={`${type}Username`} id={`${type}Username`} required value={values.username} onChange={e => setters.username(e.target.value)}
                 className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm" />
        </div>
        <div>
          <label htmlFor={`${type}Password`} className="block text-sm font-medium text-gray-700">Password</label>
          <input type="password" name={`${type}Password`} id={`${type}Password`} required value={values.password} onChange={e => setters.password(e.target.value)}
                 className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm" />
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8 max-w-3xl">
      <h1 className="text-3xl font-bold mb-6">Create New ETL Job</h1>

      {error && <div className="mb-4 p-3 bg-red-100 text-red-700 border border-red-300 rounded">{error}</div>}
      {successMessage && <div className="mb-4 p-3 bg-green-100 text-green-700 border border-green-300 rounded">{successMessage}</div>}

      <form onSubmit={handleSubmit} className="space-y-6 bg-white p-6 shadow sm:rounded-lg">
        <div>
          <label htmlFor="jobName" className="block text-sm font-medium text-gray-700">Job Name</label>
          <input type="text" name="jobName" id="jobName" required value={jobName} onChange={e => setJobName(e.target.value)}
                 className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm" />
        </div>

        {renderDbConfigFields('source')}
        {renderDbConfigFields('target')}

        <div>
          <label htmlFor="tablesToProcess" className="block text-sm font-medium text-gray-700">Tables to Process (comma-separated)</label>
          <textarea name="tablesToProcess" id="tablesToProcess" rows={3} required value={tablesToProcessStr} onChange={e => setTablesToProcessStr(e.target.value)}
                    className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
                    placeholder="e.g., ORDERS,CUSTOMERS,PRODUCTS"></textarea>
        </div>

        {/* Placeholder for Transformation Rules UI */}
        <div className="p-4 border border-dashed border-gray-300 rounded-md">
            <p className="text-sm text-gray-500">Configuration for transformation rules per table will be added here in a future update.</p>
        </div>

        <div>
          <button type="submit" disabled={isSubmitting}
                  className="w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:bg-indigo-300">
            {isSubmitting ? 'Creating Job...' : 'Create Job'}
          </button>
        </div>
      </form>
    </div>
  );
}

// Helper models - should ideally be in a shared types directory e.g. src/types/
// For now, defining inline or importing from a placeholder @/lib/models
// These should match the structure expected by the backend CreateJobRequest DTO
// which in turn matches the common EtlJobConfig (partially)
declare module '@/lib/models' {
    export interface DatabaseConnectionConfig {
        connectionName: string;
        jdbcUrl: string;
        username: string;
        password?: string; // Password is sent on create/update, might be omitted on read
        driverClassName?: string;
        additionalProperties?: { [key: string]: string };
    }

    export interface TransformationRule {
        sourceField?: string;
        sourceFields?: string[];
        targetField: string;
        transformationType: string; // Ideally an enum: MAP, CONVERT_*, CONSTANT etc.
        constantValue?: string;
        parameters?: { [key: string]: string };
    }

    export interface JobTransformationConfig {
        rules: TransformationRule[];
    }
}
